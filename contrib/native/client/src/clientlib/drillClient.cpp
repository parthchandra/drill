#include <vector>
#include <boost/log/trivial.hpp>
#include "drillClient.hpp"
#include "../recordBatch.h"
#include "../drill-client-async.h"
#include "../proto-cpp/SchemaDef.pb.h"

using namespace std;
using namespace Drill;

RecordIterator::~RecordIterator(){
    for(std::vector<FieldMetadata*>::iterator it=m_pColDefs->begin(); it!=m_pColDefs->end(); ++it){
        delete *it;
    }
    delete this->m_pColDefs;
    this->m_pColDefs=NULL;
    delete this->m_pQueryResult;
    this->m_pQueryResult=NULL;
}

std::vector<FieldMetadata*>&  RecordIterator::getColDefs(){
    //NOTE: if query is cancelled, return whatever you have. Client applications job to deal with it.
    if(this->m_pColDefs==NULL){
        if(this->m_pCurrentRecordBatch==NULL){
            this->m_pQueryResult->waitForData();
        }
        std::vector<FieldMetadata*>* pColDefs = new std::vector<FieldMetadata*>;
        {   //lock after we come out of the  wait.
            boost::lock_guard<boost::mutex> bufferLock(this->m_recordBatchMutex);
            std::vector<const FieldMetadata*>&  currentColDefs=DrillClientQueryResult::s_emptyColDefs;
            if(this->m_pCurrentRecordBatch!=NULL){
                currentColDefs=this->m_pCurrentRecordBatch->getColumnDefs();
            }else{
                // This is reached only when the first results have been received but
                // the getNext call has not been made to retrieve the record batch
                RecordBatch* pR=this->m_pQueryResult->peekNext();
                if(pR!=NULL){
                    currentColDefs=pR->getColumnDefs();
                }
            }
            for(std::vector<const FieldMetadata*>::iterator it=currentColDefs.begin(); it!=currentColDefs.end(); ++it){
                FieldMetadata* fmd= new FieldMetadata();
                fmd->CopyFrom(*(*it));//Yup, that's 2 stars
                pColDefs->push_back(fmd);
            }
        }
        this->m_pColDefs = pColDefs;
    }
    return *this->m_pColDefs;
}

status_t RecordIterator::next(){
    status_t ret=QRY_SUCCESS;
    this->m_pQueryResult->waitForData();
    this->m_currentRecord++;

    if(!this->m_pQueryResult->isCancelled()){
        if(this->m_pCurrentRecordBatch==NULL || this->m_currentRecord==this->m_pCurrentRecordBatch->getNumRecords()){
            boost::lock_guard<boost::mutex> bufferLock(this->m_recordBatchMutex);
            delete this->m_pCurrentRecordBatch; //free the previous record batch
            this->m_currentRecord=0;
            this->m_pCurrentRecordBatch=this->m_pQueryResult->getNext();
            BOOST_LOG_TRIVIAL(trace) << "Fetched new Record batch " ;
            if(this->m_pCurrentRecordBatch==NULL || this->m_pCurrentRecordBatch->getNumRecords()==0){
                BOOST_LOG_TRIVIAL(trace) << "No more data." ;
                ret = QRY_NO_MORE_DATA;
            }
            if(this->m_pCurrentRecordBatch->hasSchemaChanged()){
                ret=QRY_SUCCESS_WITH_INFO;
            }
        }
    }else{
        ret=QRY_CANCEL;
    }
    return ret;
}

/* Gets the ith column in the current record. */
status_t RecordIterator::getCol(size_t i, void** b, size_t* sz){
    //TODO: check fields out of bounds without calling getColDefs
    //if(i>=getColDefs().size()) return QRY_OUT_OF_BOUNDS;
    //return raw byte buffer
    if(!this->m_pQueryResult->isCancelled()){
        *b=this->m_pCurrentRecordBatch->getFields()[i]->getVector()->getRaw(this->m_currentRecord);
        *sz=this->m_pCurrentRecordBatch->getFields()[i]->getVector()->getSize(this->m_currentRecord);
        return QRY_SUCCESS;
    }else{
        return QRY_CANCEL;
    }
}

status_t RecordIterator::cancel(){
    this->m_pQueryResult->cancel();
    return QRY_CANCEL;
}

void RecordIterator::registerSchemaChangeListener(pfnSchemaListener* l){
    //TODO:
}

DrillClient::DrillClient(){
    this->m_pImpl=new DrillClientImpl;
}

DrillClient::~DrillClient(){
    delete this->m_pImpl;
}

connectionStatus_t DrillClient::connect(const UserServerEndPoint& endpoint){
    connectionStatus_t ret=CONN_SUCCESS;
    //TODO: handle connection failure
    this->m_pImpl->Connect(endpoint);
    
    //TODO: handle Handshake failure
    ret=this->m_pImpl->ValidateHandShake()?CONN_SUCCESS:CONN_HANDSHAKE_FAILED;
    return ret;

}

bool DrillClient::isActive(){
    return this->m_pImpl->Active();
}

void DrillClient::close() {
    this->m_pImpl->Close();
}

status_t DrillClient::submitQuery(exec::user::QueryType t, const string& plan, pfnQueryResultsListener listener, void* listenerCtx, QueryHandle_t* qHandle){
    //TODO: Handle query execution errors
    DrillClientQueryResult* pResult=this->m_pImpl->SubmitQuery(t, plan, listener, listenerCtx);
    *qHandle=(QueryHandle_t)pResult;
    return QRY_SUCCESS; 
}

RecordIterator* DrillClient::submitQuery(exec::user::QueryType t, const string& plan, DrillClientError* err){
    //TODO: Handle query execution errors
    RecordIterator* pIter=NULL;
    DrillClientQueryResult* pResult=this->m_pImpl->SubmitQuery(t, plan, NULL, NULL);
    if(pResult){
        pIter=new RecordIterator(pResult);
    }
    return pIter;
}

void DrillClient::waitForResults(){
    this->m_pImpl->waitForResults();
}

void DrillClient::freeQueryResources(QueryHandle_t* handle){
    delete (DrillClientQueryResult*)(*handle);
    *handle=NULL;
}


