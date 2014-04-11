#include <stdio.h>
#include <fstream>
#include <boost/asio.hpp>
#include "common.h"
#include "drill-client-async.h"
#include "recordBatch.h"
#include "proto-cpp/Types.pb.h"
#include "proto-cpp/User.pb.h"

using namespace exec;
using namespace common;
using namespace Drill;

status_t QueryResultsListener(void* ctx, RecordBatch* b, DrillClientError* err){
    b->print(10); // print at most 10 rows per batch
    delete b; // we're done with this batch, we an delete it
    return QRY_SUCCESS ;
}

void print(const FieldMetadata* pFieldMetadata, void* buf, size_t sz){
    const FieldDef& fieldDef = pFieldMetadata->def();
    const MajorType& majorType=fieldDef.major_type();
    int type = majorType.minor_type();
    int mode = majorType.mode();
    unsigned char printBuffer[10240];
    memset(printBuffer, 0, sizeof(printBuffer));
    switch (type) {
        case BIGINT:
            switch (mode) {
                case DM_REQUIRED:
                    sprintf((char*)printBuffer, "%lld", *(uint64_t*)buf);
                case DM_OPTIONAL:
                    break;
                case DM_REPEATED:
                    break;
            }
            break;
        case VARBINARY:
            switch (mode) {
                case DM_REQUIRED:
                    memcpy(printBuffer, buf, sz);
                case DM_OPTIONAL:
                    break;
                case DM_REPEATED:
                    break;
            }
            break;
        case VARCHAR:
            switch (mode) {
                case DM_REQUIRED:
                    memcpy(printBuffer, buf, sz);
                case DM_OPTIONAL:
                    break;
                case DM_REPEATED:
                    break;
            }
            break;
        default:
            //memcpy(printBuffer, buf, sz);
            sprintf((char*)printBuffer, "NIY");
            break;
    }
    printf("%s\t", (char*)printBuffer);
    return;
}

int main(int argc, char* argv[]) {
    try {

        string drill_addr = "127.0.0.1";
        int port=31010;
        string plan_filename = "test1.json";
        string plan2_filename;
        string queryType="sync";
        string apiType="usepublicapi";

        if (argc < 4) {
            std::cout << "Usage: async-client <server> <port> <sync|async> <usepublicapi|useinternalapi> <plan> [<plan2>] \n";
            std::cout << "Example:\n";
            std::cout << "  async_client 127.0.0.1 31010 async usepublicapi  ../resources/parquet_scan_union_screen_physical.json\n";
            return 1;
        }

        if (argc >= 6) {
            drill_addr = argv[1];
            port = atoi(argv[2]);
            queryType=argv[3];
            apiType=argv[4];
            plan_filename = argv[5];
            if(argc==7){
                plan2_filename = argv[6];
            }
        }

        cerr << "Connecting to the Server..." << endl;

        UserServerEndPoint user_server(drill_addr,port);

        ifstream f(plan_filename);
        string plan((std::istreambuf_iterator<char>(f)), (std::istreambuf_iterator<char>()));
        cerr << "plan = " << plan << endl;

        ifstream f2(plan2_filename);
        string plan2((std::istreambuf_iterator<char>(f2)), (std::istreambuf_iterator<char>()));
        cerr << "plan2 = " << plan2 << endl;

        if(apiType=="useinternalapi"){
            DrillClientImpl client;
            client.Connect(user_server);
            cerr << "Connected!\n" << endl;
            cerr << "Handshaking..." << endl;
            if (client.ValidateHandShake())
                cerr << "Handshake Succeeded!\n" << endl;

            DrillClientQueryResult* pClientQuery = NULL;
            DrillClientQueryResult* pClientQuery2 = NULL;
            if(queryType=="sync"){
                pClientQuery = client.SubmitQuery(exec::user::PHYSICAL, plan, NULL, NULL);
                RecordBatch* pR=NULL;
                while((pR=pClientQuery->getNext())!=NULL){
                    pR->print(2);
                    delete pR;
                }
                client.waitForResults();
            }else{
                pClientQuery = client.SubmitQuery(exec::user::PHYSICAL, plan, QueryResultsListener, NULL);
                if(!plan2_filename.empty()){
                    pClientQuery2 = client.SubmitQuery(exec::user::PHYSICAL, plan2, QueryResultsListener, NULL);
                }
                client.waitForResults();
            }
            client.Close();
            // It is not okay to delete pClientQuery until the query thread is done. waitForResults guarantees that 
            // the thread is done. In the sync case, the getNext call blocks and the thread essentially exits after the last
            // call to getNext, but it is still possible for the thread to be still around when we delete pClientQuery.
            delete pClientQuery;
        }else{
            DrillClient client;
            client.connect(user_server);
            cerr << "Connected!\n" << endl;

            if(queryType=="sync"){
                DrillClientError* err=NULL;
                status_t ret;
                RecordIterator* pIter = client.submitQuery(exec::user::PHYSICAL, plan, err);
                RecordIterator* pIter2 = client.submitQuery(exec::user::PHYSICAL, plan2, err);
                size_t row=0;
                if(pIter){
                    // get fields.
                    row=0;
                    std::vector<FieldMetadata*> fields = pIter->getColDefs();
                    while((ret=pIter->next())==QRY_SUCCESS){
                        row++;
                        if(row%4095==0){
                            for(size_t i=0; i<fields.size(); i++){
                                std::string name= fields[i]->def().name(0).name();
                                printf("%s\t", name.c_str());
                            }
                        }
                        printf("ROW: %ld\t", row);
                        for(size_t i=0; i<fields.size(); i++){
                            void* pBuf; size_t sz;
                            pIter->getCol(i, &pBuf, &sz);
                            print(fields[i], pBuf, sz);
                        }
                        printf("\n");
                    }
                }
                client.freeQueryIterator(&pIter);
                if(pIter2){
                    // get fields.
                    row=0;
                    std::vector<FieldMetadata*> fields = pIter2->getColDefs();
                    while((ret=pIter2->next())==QRY_SUCCESS){
                        row++;
                        if(row%4095==0){
                            for(size_t i=0; i<fields.size(); i++){
                                std::string name= fields[i]->def().name(0).name();
                                printf("%s\t", name.c_str());
                            }
                        }
                        printf("ROW: %ld\t", row);
                        for(size_t i=0; i<fields.size(); i++){
                            void* pBuf; size_t sz;
                            pIter2->getCol(i, &pBuf, &sz);
                            print(fields[i], pBuf, sz);
                        }
                        printf("\n");
                    }
                }
                client.freeQueryIterator(&pIter2);
            }else{
                QueryHandle_t qryHandle1=NULL, qryHandle2=NULL;
                client.submitQuery(exec::user::PHYSICAL, plan, QueryResultsListener, NULL, &qryHandle1);
                if(!plan2_filename.empty()){
                    client.submitQuery(exec::user::PHYSICAL, plan2, QueryResultsListener, NULL, &qryHandle2);
                }
                client.waitForResults();
                client.freeQueryResources(&qryHandle1);
                client.freeQueryResources(&qryHandle2);
            }
            client.close();
        }
    } catch (std::exception& e) {
        cerr << e.what() << endl;
    }

    return 0;
}
