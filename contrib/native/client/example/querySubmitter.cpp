#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <boost/asio.hpp>
#include "common.hpp"
#include "drillClient.hpp"
#include "recordBatch.hpp"
#include "Types.pb.h"
#include "User.pb.h"

using namespace exec;
using namespace common;
using namespace Drill;

status_t QueryResultsListener(void* ctx, RecordBatch* b, DrillClientError* err){
    b->print(10); // print at most 10 rows per batch
    delete b; // we're done with this batch, we an delete it
    return QRY_SUCCESS ;
}

void print(const Drill::FieldMetadata* pFieldMetadata, void* buf, size_t sz){
    int type = pFieldMetadata->getMinorType();
    int mode = pFieldMetadata->getDataMode();
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

int nOptions=5;

struct Option{
    char name[32];
    char desc[128];
    bool required;
}qsOptions[]= { 
    {"plan", "Plan files separated by semicolons", false},
    {"query", "Query strings, separated by semicolons", false},
    {"type", "Query type [physical|logical|sql]", true},
    {"url", "Connect url", true},
    {"api", "API type [sync|async]", true}
};

std::map<string, string> qsOptionValues;

void printUsage(){
    cerr<<"Usage: querySubmitter ";
    for(int j=0; j<nOptions ;j++){
        cerr<< " "<< qsOptions[j].name <<"="  << "[" <<qsOptions[j].desc <<"]" ;
    }
    cerr<<endl;
}

int parseArgs(int argc, char* argv[]){
    bool error=false;
    for(int i=1; i<argc; i++){
        char*a =argv[i];
        char* o=strtok(a, "=");
        char*v=strtok(NULL, "");

        bool found=false;
        for(int j=0; j<nOptions ;j++){
            if(!strcmp(qsOptions[j].name, o)){
                found=true; break;
            }
        }
        if(!found){
            cerr<< "Unknown option:"<< o <<". Ignoring" << endl;
            continue;
        }
        
        if(v==NULL){
            cerr<< ""<< qsOptions[i].name << " [" <<qsOptions[i].desc <<"] " << "requires a parameter."  << endl;
            error=true;
        }
        qsOptionValues[o]=v;
    }

    for(int j=0; j<nOptions ;j++){
        if(qsOptions[j].required ){
            if(qsOptionValues.find(qsOptions[j].name) == qsOptionValues.end()){
                cerr<< ""<< qsOptions[j].name << " [" <<qsOptions[j].desc <<"] " << "is required." << endl;
                error=true;
            }
        }
    }
    if(error){ 
        printUsage();
        exit(1);
    }
    return 0;
}

void parseUrl(string& url, string& protocol, string& host, string& port){
    char u[1024];
    strcpy(u,url.c_str());
    char* z=strtok(u, "=");
    char* h=strtok(NULL, ":");
    char* p=strtok(NULL, ":");
    protocol=z; host=h; port=p;
}

vector<string> &splitString(const string& s, char delim, vector<string>& elems){
    std::stringstream ss(s);
    string item;
    while (std::getline(ss, item, delim)){
        elems.push_back(item);
    }
    return elems;
}

int readPlans(const string& planList, vector<string>& plans){
    vector<string> planFiles;
    vector<string>::iterator iter;
    splitString(planList, ';', planFiles);
    for(iter = planFiles.begin(); iter != planFiles.end(); iter++) {
        ifstream f((*iter).c_str());
        string plan((std::istreambuf_iterator<char>(f)), (std::istreambuf_iterator<char>()));
        cout << "plan:" << plan << endl;
        plans.push_back(plan);
    } 
    return 0;
}

int readQueries(const string& queryList, vector<string>& queries){
    splitString(queryList, ';', queries);
    return 0;
}

bool validate(const string& type, const string& query, const string& plan){
    if(query.empty() && plan.empty()){
        cerr<< "Either query or plan must be specified"<<endl;
        return false;    }
    if(type=="physical" || type == "logical" ){
        if(plan.empty()){
        cerr<< "A logical or physical  plan must be specified"<<endl;
        return false;
        }
    }else
    if(type=="sql"){
        if(query.empty()){
        cerr<< "A drill SQL query must be specified"<<endl;
        return false;
        }
    }else{
        cerr<< "Unknown query type: "<< type << endl;
        return false;
    }
    return true;
}



int main(int argc, char* argv[]) {
    try {

        parseArgs(argc, argv);

        vector<string*> queries;
        string protocol, host, port_str;

        string url=qsOptionValues["url"];
        string queryList=qsOptionValues["query"];
        string planList=qsOptionValues["plan"];
        string api=qsOptionValues["api"];
        string type_str=qsOptionValues["type"];

        int port=31010;
        QueryType type;

        if(!validate(type_str, queryList, planList)){
            exit(1);
        }
        parseUrl(url, protocol, host, port_str);

        port=atoi(port_str.c_str());
        UserServerEndPoint userServer(host,port);



        vector<string> queryInputs;
        if(type_str=="sql" ){
            readQueries(queryList, queryInputs);
            type=exec::user::SQL;
        }else if(type_str=="physical" ){
            readPlans(planList, queryInputs);
            type=exec::user::PHYSICAL;
        }else if(type_str == "logical"){
            readPlans(planList, queryInputs);
            type=exec::user::LOGICAL;
        }else{
            readQueries(queryList, queryInputs);
            type=exec::user::SQL;
        }

        vector<string>::iterator queryInpIter;

        vector<RecordIterator*> recordIterators;
        vector<RecordIterator*>::iterator recordIterIter;

        vector<QueryHandle_t*> queryHandles;
        vector<QueryHandle_t*>::iterator queryHandleIter;

        DrillClient client;
        client.connect(userServer);
        cout<< "Connected!\n" << endl;

        if(api=="sync"){
            DrillClientError* err=NULL;
            status_t ret;
            for(queryInpIter = queryInputs.begin(); queryInpIter != queryInputs.end(); queryInpIter++) {
                RecordIterator* pRecIter = client.submitQuery(type, *queryInpIter, err);
                if(pRecIter!=NULL){
                    recordIterators.push_back(pRecIter);
                }
            }
            size_t row=0;
            for(recordIterIter = recordIterators.begin(); recordIterIter != recordIterators.end(); recordIterIter++) {
                // get fields.
                row=0;
                RecordIterator* pRecIter=*recordIterIter;
                std::vector<Drill::FieldMetadata*> fields = pRecIter->getColDefs();
                while((ret=pRecIter->next())==QRY_SUCCESS){
                    row++;
                    if(row%4095==0){
                        for(size_t i=0; i<fields.size(); i++){
                            std::string name= fields[i]->getName();
                            printf("%s\t", name.c_str());
                        }
                    }
                    printf("ROW: %ld\t", row);
                    for(size_t i=0; i<fields.size(); i++){
                        void* pBuf; size_t sz;
                        pRecIter->getCol(i, &pBuf, &sz);
                        print(fields[i], pBuf, sz);
                    }
                    printf("\n");
                }
                client.freeQueryIterator(&pRecIter);
            }
        }else{
            for(queryInpIter = queryInputs.begin(); queryInpIter != queryInputs.end(); queryInpIter++) {
                QueryHandle_t* qryHandle = new QueryHandle_t;
                client.submitQuery(type, *queryInpIter, QueryResultsListener, NULL, qryHandle);
                queryHandles.push_back(qryHandle);
            }
            client.waitForResults();
            for(queryHandleIter = queryHandles.begin(); queryHandleIter != queryHandles.end(); queryHandleIter++) {
                client.freeQueryResources(*queryHandleIter);
            }
        }
        client.close();
    } catch (std::exception& e) {
        cerr << e.what() << endl;
    }

    return 0;
}
