#ifndef DRILL_CLIENT_H
#define DRILL_CLIENT_H

#include <vector>
#include <boost/thread.hpp>
#include "common.hpp"
#include "User.pb.h"

using namespace std;
using namespace exec::shared;

#ifdef WIN32
#ifdef DRILL_CLIENT_EXPORTS
#define DECLSPEC_DRILL_CLIENT __declspec(dllexport)
#else
#define DECLSPEC_DRILL_CLIENT __declspec(dllimport)
#endif
#else
#define DECLSPEC_DRILL_CLIENT
#endif

#define DECLSPEC_DRILL_CLIENT

namespace Drill {

    struct UserServerEndPoint;
    class  DrillClientImpl;
    class  DrillClientQueryResult;
    class  FieldDefinition;
    class  RecordBatch;
    class  SchemaDef;

    class DECLSPEC_DRILL_CLIENT DrillClientError{
        public:
            DrillClientError(){errno=0; msg=NULL;};
            DrillClientError(int e, char* m){errno=e; msg=m;};
            int errno;
            char* msg;
    };

    typedef enum{
        QRY_SUCCESS=0, 
        QRY_FAILURE=1, 
        QRY_SUCCESS_WITH_INFO=2, 
        QRY_NO_MORE_DATA=3, 
        QRY_CANCEL=4, 
        QRY_OUT_OF_BOUNDS=5,
        QRY_CLIENT_OUTOFMEM=6
    } status_t;

    typedef enum{
        CONN_SUCCESS=0, 
        CONN_FAILURE=1, 
        CONN_HANDSHAKE_FAILED=2
    } connectionStatus_t;

    /*
     * Handle to the Query submitted for execution.
     * */
    typedef void* QueryHandle_t;

    /*
     * Query Results listener callback. This function is called for every record batch after it has 
     * been received and decoded. The listener function should return a status. 
     * If the listener returns failure, the query will be canceled.
     *
     * DrillClientQueryResult will hold a listener & listener contxt for the call back function
     */
    typedef status_t (*pfnQueryResultsListener)(QueryHandle_t ctx, RecordBatch* b, DrillClientError* err);

    /*
     * The schema change listener callback. This function is called if the record batch detects a
     * change in the schema. The client application can call getColDefs in the RecordIterator or 
     * get the field information from the RecordBatch itself and handle the change appropriately.
     */
    typedef uint32_t (*pfnSchemaListener)(void* ctx, SchemaDef* s, DrillClientError* err);

    /* 
     * A Record Iterator instance is returned by the SubmitQuery class. Calls block until some data 
     * is available, or until all data has been returned.
     */

    class DECLSPEC_DRILL_CLIENT RecordIterator{
        friend class DrillClient;
        public:

        ~RecordIterator();
            /* 
             * Returns a vector of column(i.e. field) definitions. The returned reference is guaranteed to be valid till the 
             * end of the query or until a schema change event is received. If a schema change event is received by the 
             * application, the application should discard the reference it currently holds and call this function again. 
             */
            vector<FieldMetadata*>& getColDefs();
            
            /* Move the current pointer to the next record. */
            status_t next();
            
            /* Gets the ith column in the current record. */
            status_t getCol(size_t i, void** b, size_t* sz);
            
            /* Cancels the query. */
            status_t cancel();

            //void waitForResults();
            void registerSchemaChangeListener(pfnSchemaListener* l);

        private:
            RecordIterator(DrillClientQueryResult* pResult){
                this->m_currentRecord=-1;
                this->m_pCurrentRecordBatch=NULL;
                this->m_pQueryResult=pResult;
            }

            DrillClientQueryResult* m_pQueryResult;
            size_t m_currentRecord;
            RecordBatch* m_pCurrentRecordBatch;
            boost::mutex m_recordBatchMutex; 
            vector<FieldMetadata*>* m_pColDefs; // Copy of the latest column defs made from the 
                                                // first record batch with this definition
            //bool m_cancel;
    };

    class DECLSPEC_DRILL_CLIENT DrillClient{
        public:
            DrillClient();
            ~DrillClient();

            /* connects the client to a Drillbit UserServer. */
            connectionStatus_t connect(const UserServerEndPoint& endpoint);

            /* test whether the client is active */
            bool isActive();

            /*  close the connection. cancel any pending requests. */
            void close() ;

            /*
             * Submit a query asynchronously and wait for results to be returned thru a callback. A query context handle is passed 
             * back. The listener callback will return the handle in the ctx parameter.
             */
            status_t submitQuery(exec::user::QueryType t, const string& plan, pfnQueryResultsListener listener, void* listenerCtx, QueryHandle_t* qHandle);

            /*
             * Submit a query asynchronously and wait for results to be returned thru an iterator that returns
             * results synchronously. The client app needs to call delete on the iterator when done.
             */
            RecordIterator* submitQuery(exec::user::QueryType t, const string& plan, DrillClientError* err);

            /* 
             * The client application should call this function to wait for results if it has registered a 
             * listener.
             */
            void waitForResults();

            /*
             * Applications using the async query submit method should call freeQueryResources to free up resources 
             * once the query is no longer being processed.
             * */
            void freeQueryResources(QueryHandle_t* handle);
            void freeQueryIterator(RecordIterator** pIter){ delete *pIter; *pIter=NULL;};

        private:

            DrillClientImpl * m_pImpl;

    };

} // namespace Drill

#endif
