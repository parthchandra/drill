#include <queue>
#include <vector>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/log/trivial.hpp>

#include "drillClient.hpp"
#include "drillClientImpl.hpp"
#include "rpcEncoder.hpp"
#include "rpcDecoder.hpp"
#include "rpcMessage.hpp"
#include "recordBatch.hpp"
#include "UserBitShared.pb.h"

#ifdef DEBUG
#define BOOST_ASIO_ENABLE_HANDLER_TRACKING
#endif

using namespace std;
using namespace boost;
using namespace Drill;
using namespace exec::user;
using boost::asio::ip::tcp;

RpcEncoder DrillClientImpl::s_encoder;
RpcDecoder DrillClientImpl::s_decoder;

void DrillClientImpl::Connect(const UserServerEndPoint& userver){
    // connect the endpoint
    //TODO: Handle connection failure
    if(!this->m_bIsConnected){
        /*  
             // boost::asio::io_service io_service;
         
             tcp::resolver resolver(m_io_service);
             tcp::resolver::query query(tcp::v4(), userver.m_addr, userver.m_port);
             tcp::resolver::iterator iterator = resolver.resolve(query);
             // tcp::socket s(io_service);
             boost::asio::connect(m_socket, iterator);
        */

        boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::address::from_string(userver.m_addr), userver.m_port);
        m_socket.connect(endpoint);
    }
}

void DrillClientImpl::sendSync(OutBoundRpcMessage& msg){
    boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
    DrillClientImpl::s_encoder.Encode(m_wbuf, msg);
    m_socket.write_some(boost::asio::buffer(m_wbuf));
}

void DrillClientImpl::recvSync(InBoundRpcMessage& msg){
    boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
    m_socket.read_some(boost::asio::buffer(m_rbuf));
    uint32_t length = 0;
    int bytes_read = DrillClientImpl::s_decoder.LengthDecode(m_rbuf.data(), &length);
    DrillClientImpl::s_decoder.Decode(m_rbuf.data() + bytes_read, length, msg);
}

bool DrillClientImpl::ValidateHandShake(){
    UserToBitHandshake u2b;
    u2b.set_channel(exec::shared::USER);
    u2b.set_rpc_version(1);
    u2b.set_support_listening(true);

    int coordId = this->getNextCoordinationId();

    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, HANDSHAKE, coordId, &u2b);
    sendSync(out_msg);

    InBoundRpcMessage in_msg;
    recvSync(in_msg);

    BitToUserHandshake b2u;
    b2u.ParseFromArray(in_msg.m_pbody.data(), in_msg.m_pbody.size());

    // validate handshake
    if (b2u.rpc_version() != u2b.rpc_version()) {
        BOOST_LOG_TRIVIAL(trace) << "Invalid rpc version.  Expected << " <<u2b.rpc_version() << ", actual "<< b2u.rpc_version() << "." ;
        return false;
    }
    return true;
}


std::vector<Drill::FieldMetadata*> DrillClientQueryResult::s_emptyColDefs;

DrillClientQueryResult* DrillClientImpl::SubmitQuery(QueryType t, const string& plan, pfnQueryResultsListener l, void* lCtx){
    RunQuery query;
    query.set_results_mode(STREAM_FULL);
    query.set_type(t);
    query.set_plan(plan);

    int coordId;
    {
        boost::lock_guard<boost::mutex> bufferLock(m_coordMutex);
        coordId = this->getNextCoordinationId();
    }
    OutBoundRpcMessage out_msg(exec::rpc::REQUEST, RUN_QUERY, coordId, &query);
    sendSync(out_msg);

    DrillClientQueryResult* pQuery=NULL;
    pQuery = new DrillClientQueryResult(this, coordId);
    pQuery->registerListener(l, lCtx);
    this->m_queryIds[coordId]=pQuery;

    BOOST_LOG_TRIVIAL(trace)  << "Submit Query Request. Coordination id = " << coordId;

    //TODO: Handle errors in query results.
    bool sendRequest=false;
    {
        boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
        if(m_pendingRequests++==0){
            sendRequest=true;
        }else{
            BOOST_LOG_TRIVIAL(trace) << "Queueing read request to server" << endl;
        }
    }
    if(sendRequest){
        getNextResult(); // async wait for results
    }

    //run this in a new thread
    {
        if(this->m_pListenerThread==NULL){
            this->m_pListenerThread= new boost::thread(boost::bind(&boost::asio::io_service::run, &this->m_io_service));
        }
    }
    return pQuery;
}

void DrillClientImpl::getNextResult() {
    boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
    //TODO: free this (use free, not delete) 
    ByteBuf_t readBuf = allocateBuffer(LEN_PREFIX_BUFLEN);
    memset(readBuf, 0, LEN_PREFIX_BUFLEN);
    async_read( 
            this->m_socket,
            boost::asio::buffer(readBuf, LEN_PREFIX_BUFLEN),
            boost::bind(
                &DrillClientImpl::handleRead,
                this,
                readBuf,
                boost::asio::placeholders::error, 
                boost::asio::placeholders::bytes_transferred)
            );
    BOOST_LOG_TRIVIAL(trace) << "Sending read request to server" << endl;
}

void DrillClientImpl::waitForResults(){
    this->m_pListenerThread->join();
    delete this->m_pListenerThread; this->m_pListenerThread=NULL;
}

status_t DrillClientImpl::readMsg(ByteBuf_t _buf, InBoundRpcMessage& msg, boost::system::error_code& error){
    size_t leftover=0;
    uint32_t rmsgLen;
    ByteBuf_t currentBuffer;
        {
            // We need to protect the readLength and read buffer, and the pending requests counter, 
            // but we don't have to keep the lock while we decode the rest of the buffer.
            boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
            int bytes_read = DrillClientImpl::s_decoder.LengthDecode(_buf, &rmsgLen);
            BOOST_LOG_TRIVIAL(trace) << "len bytes = " << bytes_read << endl;
            BOOST_LOG_TRIVIAL(trace) << "rmsgLen = " << rmsgLen << endl;

            if(rmsgLen>0){
                leftover = LEN_PREFIX_BUFLEN - bytes_read;
                // Allocate a buffer
                BOOST_LOG_TRIVIAL(trace) << "Allocated and locked buffer." << endl;
                currentBuffer=allocateBuffer(rmsgLen);
                if(currentBuffer==NULL){
                    return QRY_CLIENT_OUTOFMEM;
                }
                if(leftover){
                    memcpy(currentBuffer, _buf + bytes_read, leftover);
                }
                freeBuffer(_buf);
                BOOST_LOG_TRIVIAL(trace) << "reading data (rmsgLen - leftover) : " << (rmsgLen - leftover) << endl;
                ByteBuf_t b=currentBuffer + leftover;
                size_t bytesToRead=rmsgLen - leftover;
                while(1){
                    size_t dataBytesRead=this->m_socket.read_some(
                            boost::asio::buffer(b, bytesToRead), 
                            error);
                    if(error) break;
                    BOOST_LOG_TRIVIAL(trace) << "Data Message: actual bytes read = " << dataBytesRead << endl;
                    if(dataBytesRead==bytesToRead) break;
                    bytesToRead-=dataBytesRead;
                    b+=dataBytesRead;
                }
                if(!error){
                    // read data successfully
                    DrillClientImpl::s_decoder.Decode(currentBuffer, rmsgLen, msg);
                    BOOST_LOG_TRIVIAL(trace) << "Done decoding chunk. Coordination id: " <<msg.m_coord_id<< endl;
                }else{
                    return QRY_FAILURE;
                }
            }else{
                return QRY_NO_MORE_DATA;
            }
        }
        return QRY_SUCCESS;
}

status_t DrillClientImpl::processQueryResult(InBoundRpcMessage& msg ){
        DrillClientQueryResult* pDrillClientQueryResult=NULL;
            status_t ret=QRY_SUCCESS;
            {
            boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
            QueryResult* qr = new QueryResult; //Record Batch will own this object and free it up.
            qr->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
            BOOST_LOG_TRIVIAL(trace) << qr->DebugString();
            //TODO: Check QueryResult.queryState. QueryResult could have an error.
            
            //Build Record Batch here 
            qr->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());

            BOOST_LOG_TRIVIAL(trace) << qr->DebugString();
            
            //TODO: schema changed??
            //status_t r=QRY_SUCCESS;
            //r=pDrillClientQueryResult->setupColumnDefs(qr);

            RecordBatch* pRecordBatch= new RecordBatch(qr, msg.m_dbody);
            pRecordBatch->build();

            //TODO: schema changed??
            //if(r==QRY_SUCCESS_WITH_INFO){
            //    pRecordBatch->schemaChanged(true);
            //}
            exec::shared::QueryId qid;
            qid.CopyFrom(qr->query_id());
            std::map<QueryId*, DrillClientQueryResult*, compareQueryId>::iterator it;
            it=this->m_queryResults.find(&qid);
            if(it!=this->m_queryResults.end()){
                pDrillClientQueryResult=(*it).second;
            }else{
                //TODO: return QRY_FAILURE;
            }
            BOOST_LOG_TRIVIAL(trace) << "Drill Client Query Result - " << pDrillClientQueryResult << endl;

            pDrillClientQueryResult->m_bIsQueryPending=true;
            pDrillClientQueryResult->m_bIsLastChunk=qr->is_last_chunk();
            pfnQueryResultsListener pResultsListener=pDrillClientQueryResult->m_pResultsListener;
            if(pResultsListener!=NULL){
                ret = pResultsListener(pDrillClientQueryResult, pRecordBatch, NULL);
            }else{
                //Use a default callback that is called when a record batch is received
                ret = pDrillClientQueryResult->defaultQueryResultsListener(pDrillClientQueryResult, pRecordBatch, NULL);
            }
            } // release lock
            if(ret==QRY_FAILURE){
                pDrillClientQueryResult->sendCancel(msg);
                {
                    boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
                    m_pendingRequests--;
                }
                BOOST_LOG_TRIVIAL(trace) << "Client app canceled query.";
                //if(m_pendingRequests!=0){
                //    getNextResult();
                //}
                return ret;
            }
            if(pDrillClientQueryResult->m_bIsLastChunk){
                {
                    boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
                    m_pendingRequests--;
                }
                ret=QRY_NO_MORE_DATA;
                BOOST_LOG_TRIVIAL(trace) << "Received last batch.";
                //if(m_pendingRequests!=0){
                //    getNextResult();
                //}
                pDrillClientQueryResult->sendAck(msg);
                return ret;
            }
            pDrillClientQueryResult->sendAck(msg);
            return ret;
}

status_t DrillClientImpl::processQueryId(InBoundRpcMessage& msg ){
        DrillClientQueryResult* pDrillClientQueryResult=NULL;
            status_t ret=QRY_SUCCESS;
            boost::lock_guard<boost::mutex> bufferLock(this->m_bufferMutex);
            std::map<int,DrillClientQueryResult*>::iterator it;
            it=this->m_queryIds.find(msg.m_coord_id);
            if(it!=this->m_queryIds.end()){
                pDrillClientQueryResult=(*it).second;
                exec::shared::QueryId *qid = new exec::shared::QueryId;
                BOOST_LOG_TRIVIAL(trace)  << "Received Query Handle" << msg.m_pbody.size();
                qid->ParseFromArray(msg.m_pbody.data(), msg.m_pbody.size());
                BOOST_LOG_TRIVIAL(trace) << qid->DebugString();
                m_queryResults[qid]=pDrillClientQueryResult;
                //save queryId allocated here so we can free it later
                pDrillClientQueryResult->setQueryId(qid);
            }else{
                ret=QRY_FAILURE;
            }
            return ret;
}

void DrillClientImpl::handleRead(ByteBuf_t _buf, const boost::system::error_code& err, size_t bytes_transferred) {
    //TODO: LOCK the queries container
    //TODO: LOCK the buffers container
    //size_t leftover=0;
    boost::system::error_code error=err;
    if (!error){
        //uint32_t rmsgLen;
        //ByteBuf_t currentBuffer;
        //DrillClientQueryResult* pDrillClientQueryResult=NULL;
        InBoundRpcMessage msg;

        BOOST_LOG_TRIVIAL(trace) << "Getting new message" << endl;

        if(readMsg(_buf, msg, error)!=QRY_SUCCESS){
          //TODO:
          return;  
        } 
          
        if(!error && msg.m_rpc_type==QUERY_RESULT){
            status_t ret;
            ret=processQueryResult(msg);
            //TODO: check the return value and do not continue if the 
            //client app returns FAILURE
            if(ret==QRY_FAILURE||ret==QRY_NO_MORE_DATA){
                if(m_pendingRequests!=0){
                    getNextResult();
                }
                return;
            }
        }else if(!error && msg.m_rpc_type==QUERY_HANDLE){ 
        //DrillClientQueryResult* pDrillClientQueryResult=NULL;
        status_t ret=processQueryId(msg);
        //TODO: handle QRY_ERROR 
        }else{
            if(error){
                //this->m_bIsQueryPending=false;
                //TODO: handle socket read error
                // We have a socket read error, but we do not know which query this is for. 
                // Signal ALL pending queries that they should stop waiting.
                BOOST_LOG_TRIVIAL(trace) << "read error: " << error << "\n";
                return;
            }else{
                //TODO:If not QUERY_RESULT, then do appropriate error handling
                //pDrillClientQueryResult->m_bIsQueryPending=false; // any blocked listener threads will move on
                BOOST_LOG_TRIVIAL(trace) << "QueryResult returned " << msg.m_rpc_type;
                return;
            }
        }
        getNextResult();
    }
    return;
}

void DrillClientQueryResult::sendAck(InBoundRpcMessage& msg){
    exec::rpc::Ack ack;
    ack.set_ok(true);
    OutBoundRpcMessage ack_msg(exec::rpc::RESPONSE, ACK, msg.m_coord_id, &ack);
    m_pClient->sendSync(ack_msg);
    BOOST_LOG_TRIVIAL(trace) << "ACK sent" << endl;
}

void DrillClientQueryResult::sendCancel(InBoundRpcMessage& msg){
    exec::rpc::Ack ack;
    ack.set_ok(true);
    OutBoundRpcMessage ack_msg(exec::rpc::RESPONSE, CANCEL_QUERY, msg.m_coord_id, &ack);
    m_pClient->sendSync(ack_msg);
    BOOST_LOG_TRIVIAL(trace) << "CANCEL sent" << endl;
}

// This COPIES the FieldMetadata definition for the record batch.  ColumnDefs held by this 
// class are used by the async callbacks.
status_t DrillClientQueryResult::setupColumnDefs(QueryResult* pQueryResult) {
    bool hasSchemaChanged=false;
    boost::lock_guard<boost::mutex> bufferLock(this->m_schemaMutex);

    std::vector<Drill::FieldMetadata*> prevSchema=this->m_columnDefs;
    std::map<string, Drill::FieldMetadata*> oldSchema;
    for(std::vector<Drill::FieldMetadata*>::iterator it = prevSchema.begin(); it != prevSchema.end(); ++it){
        // the key is the field_name + type
        char type[256];
        sprintf(type, ":%d:%d",(*it)->getMinorType(), (*it)->getDataMode() );
        std::string k= (*it)->getName()+type;
        oldSchema[k]=*it;
    }

    m_columnDefs.clear();
    size_t numFields=pQueryResult->def().field_size();
    for(size_t i=0; i<numFields; i++){
        //TODO: free this??
        Drill::FieldMetadata* fmd= new Drill::FieldMetadata;
        fmd->set(pQueryResult->def().field(i));
        this->m_columnDefs.push_back(fmd);

        // Look for field in oldSchema. If found remove it. If not trigger schema change.
        char type[256];
        sprintf(type, ":%d:%d",fmd->getMinorType(), fmd->getDataMode() );
        std::string k= fmd->getName()+type;
        std::map<string, Drill::FieldMetadata*>::iterator iter=oldSchema.find(k);
        if(iter==oldSchema.end()){
            // not found
            hasSchemaChanged=true;
        }else{
            oldSchema.erase(iter);
        }
    }
    if(oldSchema.size()>0){
        hasSchemaChanged=true;
    }
    //TODO:Look for changes in the vector and trigger a Schema change event if necessary. 
    //If vectors are different, then call the schema change listener.
    
    //free memory allocated for FieldMetadata objects saved in previous columnDefs;
    for(std::vector<Drill::FieldMetadata*>::iterator it = prevSchema.begin(); it != prevSchema.end(); ++it){
        delete *it;    
    }
    prevSchema.clear();
    this->m_bHasSchemaChanged=hasSchemaChanged;
    if(hasSchemaChanged){
        //TODO: invoke schema change Listener
    }
    return hasSchemaChanged?QRY_SUCCESS_WITH_INFO:QRY_SUCCESS;
}

status_t DrillClientQueryResult::defaultQueryResultsListener(void* ctx,  RecordBatch* b, DrillClientError* err) {
    // TODO the design of query result listener need to be simplified
    //ctx; // unused, we already have the this pointer
    BOOST_LOG_TRIVIAL(trace) << "Query result listener called" << endl;
    //check if the query has been canceled. IF so then return FAILURE. Caller will send cancel to the server.
    if(this->m_bCancel){
        return QRY_FAILURE;
    }
    if (!err) {
        // signal the cond var
        {
            BOOST_LOG_TRIVIAL(trace) << "Query result listener saved result to queue." << endl;
            boost::lock_guard<boost::mutex> bufferLock(this->m_cvMutex);
            this->m_recordBatches.push(b);
            this->m_bHasData=true;
        }
        m_cv.notify_one();
    }else{
        //TODO: BOOST_LOG_TRIVIAL(trace) << "Error: " << err->msg() << "\n";
        return QRY_FAILURE;
    }
    return QRY_SUCCESS;
}

RecordBatch*  DrillClientQueryResult::peekNext() {
    RecordBatch* pRecordBatch=NULL;
    //if no more data, return NULL;
    if(!m_bIsQueryPending) return NULL;
    boost::unique_lock<boost::mutex> bufferLock(this->m_cvMutex);
    BOOST_LOG_TRIVIAL(trace) << "Synchronous read waiting for data." << endl;
    while(!this->m_bHasData) {
        this->m_cv.wait(bufferLock);
    }
    // READ but not remove first element from queue
    pRecordBatch = this->m_recordBatches.front();
    return pRecordBatch;
}

RecordBatch*  DrillClientQueryResult::getNext() {
    RecordBatch* pRecordBatch=NULL;
    //if no more data, return NULL;
    if(!m_bIsQueryPending) return NULL;

    boost::unique_lock<boost::mutex> bufferLock(this->m_cvMutex);
    BOOST_LOG_TRIVIAL(trace) << "Synchronous read waiting for data." << endl;
    while(!this->m_bHasData) {
        this->m_cv.wait(bufferLock);
    }
    // remove first element from queue
    pRecordBatch = this->m_recordBatches.front();
    this->m_recordBatches.pop();
    this->m_bHasData=!this->m_recordBatches.empty();
    // if vector is empty, set m_bHasDataPending to false;
    m_bIsQueryPending=!(this->m_recordBatches.empty()&&m_bIsLastChunk);
    return pRecordBatch;
}

// Blocks until data is available
void DrillClientQueryResult::waitForData() {
    //if no more data, return NULL;
    if(!m_bIsQueryPending) return;
    boost::unique_lock<boost::mutex> bufferLock(this->m_cvMutex);
    while(!this->m_bHasData) {
        this->m_cv.wait(bufferLock);
    }
}

void DrillClientQueryResult::cancel() {
    this->m_bCancel=true;
}


void DrillClientQueryResult::clearAndDestroy(){
    if(this->m_pQueryId!=NULL){
        delete this->m_pQueryId; this->m_pQueryId=NULL;
    }
    //free memory allocated for FieldMetadata objects saved in m_columnDefs;
    for(std::vector<Drill::FieldMetadata*>::iterator it = m_columnDefs.begin(); it != m_columnDefs.end(); ++it){
        delete *it;    
    }
    m_columnDefs.clear();
    //TODO: Free any record batches or buffers remaining
    //TODO: Cancel any pending requests
}
