#ifndef DRILL_CLIENT_ASYNC_H
#define DRILL_CLIENT_ASYNC_H

#include <vector>
#include <queue>
#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include "proto-cpp/User.pb.h"
#include "proto-cpp/UserBitShared.pb.h"
#include "common.h"
#include "rpc-encoder.h"
#include "rpc-decoder.h"
#include "drillClient.hpp"

using namespace std;
using namespace boost;
using namespace exec::shared;
using namespace exec::user;
namespace asio = boost::asio;

namespace Drill {

    class vector;
    class queue;
    class DrillClientImpl;
    class InBoundRpcMessage;
    class OutBoundRpcMessage;
    class RecordBatch;
    class RpcEncoder;
    class RpcDecoder;
    struct UserServerEndPoint;

    class DrillClientQueryResult{
        friend class DrillClientImpl;
        public:
            DrillClientQueryResult(DrillClientImpl * pClient, int coordId):
                m_pClient(pClient),
                m_coordinationId(coordId),
                //m_rmsgLen(0),
                //m_currentBuffer(NULL),
                m_bHasData(false),
                m_bIsQueryPending(true),
                m_bIsLastChunk(false),
                m_bCancel(false),
                m_bHasSchemaChanged(false), 
                m_pQueryId(NULL) ,
                m_pResultsListener(NULL),
                m_pListenerCtx(NULL)
            {};

            ~DrillClientQueryResult(){
                this->clearAndDestroy();
            };

            //void getResult();

            // get data asynchronously
            void registerListener(pfnQueryResultsListener listener, void* listenerCtx){
                this->m_pResultsListener=listener;
				this->m_pListenerCtx = listenerCtx;
            }

            // Synchronous call to get data. Caller assumes ownership of the recod batch 
            // returned and it is assumed to have been consumed.
            RecordBatch*  getNext();
            // Synchronous call to get a look at the next Record Batch. This 
            // call does not move the current pointer forward. Repeatied calls 
            // to peekNext return the same value until getNext is called.
            RecordBatch*  peekNext();
            // Blocks until data is available.
            void waitForData();

            // placeholder to return an empty col def vector when calls are made out of order.
            static std::vector<const FieldMetadata*> s_emptyColDefs;

            std::vector<FieldMetadata*>& getColumnDefs(){ 
                boost::lock_guard<boost::mutex> bufferLock(this->m_schemaMutex);
                return this->m_columnDefs;
            }

            void cancel();    
            bool isCancelled(){return this->m_bCancel;};
            bool hasSchemaChanged(){return this->m_bHasSchemaChanged;};
            int getCoordinationId(){ return this->m_coordinationId;}

            void setQueryId(QueryId* q){this->m_pQueryId=q;}
			void* getListenerContext() {return this->m_pListenerCtx;}
			exec::shared::QueryId& getQueryId(){ return *(this->m_pQueryId); }
        private:

            DrillClientImpl* m_pClient;

            int m_coordinationId;

            // Vector of Buffers holding data returned by the server
            // Each data buffer is decoded into a RecordBatch
            std::vector<ByteBuf_t> m_dataBuffers;
            std::queue<RecordBatch*> m_recordBatches;
            std::vector<FieldMetadata*> m_columnDefs;

            // Mutex to protect read/write operations on the socket
            boost::mutex m_bufferMutex; 
            // Mutex to protect schema definitions
            boost::mutex m_schemaMutex; 
            // Mutex for Cond variable for read write to batch vector
            boost::mutex m_cvMutex; 
            // Condition variable to signal arrival of more data. Condition variable is signaled 
            // if the recordBatches queue is not empty
            boost::condition_variable m_cv; 
            bool m_bHasData;
            
            // if m_bIsQueryPending is true, we continue to wait for results
            bool m_bIsQueryPending;

            bool m_bIsLastChunk;

            bool m_bCancel;

            bool m_bHasSchemaChanged;

            QueryId* m_pQueryId;
			
            // Results callback
            pfnQueryResultsListener m_pResultsListener;

            // Listener context
            void * m_pListenerCtx;

            //void getNextRecordBatch();
            // get the length of the data being sent and start an async read to read that data
            //void handleReadLength(const boost::system::error_code & err, size_t bytes_transferred) ;
            //void handleReadMsg(const boost::system::error_code & err, size_t bytes_transferred) ;
            status_t setupColumnDefs(QueryResult* pQueryResult);
            void sendAck(InBoundRpcMessage& msg);
            void sendCancel(InBoundRpcMessage& msg);


            status_t defaultQueryResultsListener(void* ctx, RecordBatch* b, DrillClientError* err);

            void clearAndDestroy();
    };

    class DrillClientImpl {
        friend class DrillClientQueryResult;

        public:
        DrillClientImpl():m_socket(m_io_service), m_strand(m_io_service), m_pListenerThread(NULL),
        m_rbuf(1024), m_wbuf(1024), m_pendingRequests(0), m_coordinationId(1) {
            m_bIsConnected=false;
        };

        ~DrillClientImpl(){ 
            //TODO: Free any record batches or buffers remaining
            //TODO: Cancel any pending requests
            //TODO: Clear and destroy DrillClientQueryResults vector
            //Terminate and free the listener thread
            if(this->m_pListenerThread!=NULL){
                this->m_pListenerThread->interrupt();
                delete this->m_pListenerThread;
            }
        };

        // connects the client to a Drillbit UserServer
        void Connect(const UserServerEndPoint& endpoint);

        // test whether the client is active
        bool Active();

        // reconnet if sumbission failed, retry is setting by m_reconnect_times
        bool Reconnect();
        void Close() ;
        bool ValidateHandShake(); // throw expection if not valid

        DrillClientQueryResult* SubmitQuery(exec::user::QueryType t, const string& plan, pfnQueryResultsListener listener, void* listenerCtx);
        void waitForResults();

        private:


        bool m_bIsConnected;
        boost::asio::io_service m_io_service;
        boost::asio::ip::tcp::socket m_socket;
        boost::asio::strand m_strand;
        boost::thread * m_pListenerThread;

        //for synchronous messages, like validate handshake
        DataBuf m_rbuf; // buffer for receiving synchronous messages
        DataBuf m_wbuf; // buffer for sending synchronous message
        // Mutex to protect read/write buffer.
        boost::mutex m_bufferMutex; 
        // Mutex to protect coordinationId
        boost::mutex m_coordMutex; 

        Byte_t m_readLengthBuf[LEN_PREFIX_BUFLEN];

        int getNextCoordinationId(){ return ++m_coordinationId; };
        // end and receive synchronous messages
        void sendSync(OutBoundRpcMessage& msg);
        void recvSync(InBoundRpcMessage& msg);
        void getNextResult();
        status_t readMsg(ByteBuf_t _buf, InBoundRpcMessage& msg, boost::system::error_code& error);
        status_t processQueryResult(InBoundRpcMessage& msg);
        status_t processQueryId(InBoundRpcMessage& msg );
        void handleRead(ByteBuf_t _buf, const boost::system::error_code & err, size_t bytes_transferred) ;
        ByteBuf_t allocateBuffer(size_t len){
            ByteBuf_t b = (ByteBuf_t)malloc(len); memset(b, 0, len); return b;
        }
        void freeBuffer(ByteBuf_t b){ free(b); }


        static RpcEncoder s_encoder;
        static RpcDecoder s_decoder;

        struct compareQueryId{
            bool operator()(const QueryId* q1, const QueryId* q2) const {
                return q1->part1()<q2->part1() && q1->part2() < q2->part2();
            }
    };
        //static bool compareQueryId(const QueryId* q1, const QueryId* q2){
        //    return q1->part1<q2->part1 && q1->part2 < q2->part2;
        //}
        //coordination id to query result 
        std::map<int, DrillClientQueryResult*> m_queryIds;

        // query id to query result
        std::map<QueryId*, DrillClientQueryResult*, compareQueryId> m_queryResults;

        size_t m_pendingRequests;   // number of outstanding read requests. 
                                    // handleRead will keep asking for more results as long as this number is not zero.

        int m_coordinationId;

    };

    inline bool DrillClientImpl::Active() {
        return this->m_bIsConnected;;
    }

    inline void DrillClientImpl::Close() {
        //TODO: cancel pending query
        if(this->m_bIsConnected){
            m_socket.close();
            m_bIsConnected=false;
        }
    }

    inline bool DrillClientImpl::Reconnect() {
        if (Active()) {
            return true;
        }
        /*
           int retry = m_reconnect_times;
           while( retry > 0 ){
           retry--;
    // ask the cluster coordinator for available drillbit

    }
    */
        return true;
    }



} // namespace Drill

#endif
