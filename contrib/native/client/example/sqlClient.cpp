#include <stdio.h>
#include <fstream>
#include <boost/asio.hpp>
#include "common.h"
#include "clientlib/drillClient.hpp"
#include "recordBatch.h"
#include "proto-cpp/Types.pb.h"
#include "proto-cpp/User.pb.h"

using namespace exec;
using namespace common;
using namespace Drill;

status_t QueryResultsListener(void* ctx, RecordBatch* b, DrillClientError* err){
    b->print(b->getNumRecords() < 10? b->getNumRecords() : 10); // print at most 10 records
    return QRY_SUCCESS ;
}

int main(int argc, char* argv[]) {
    try {

        //string drill_addr = "192.168.202.156";
        //string drill_addr = "10.0.28.124";
        string drill_addr = "127.0.0.1";
        int port=31010;
		UserServerEndPoint user_server(drill_addr,port);
		        
        if (argc > 3) {
            std::cout << "Usage: sqlClient <server> <port>\n";
            std::cout << "Example:\n";
            std::cout << "  sqlClient 127.0.0.1 31010\n";
            return 1;
        }

        if (argc == 2) {
            drill_addr = argv[1];
            port = atoi(argv[2]);
        }


		std::vector<std::string> plans; 
		plans.push_back("select * from `INFORMATION_SCHEMA`.`SCHEMATA`");
		//plans.push_back("select * from `INFORMATION_SCHEMA`.`TABLES`");
		//plans.push_back("select * from dfs.`/opt/drill/data/json/test.json`");
		//plans.push_back("select * from `hivestg`.`integer_table`");
         
        DrillClient client;
        client.connect(user_server);

		for(int i = 0; i< plans.size() ; i++){
			std::string& plan = plans[i];
			QueryHandle_t qryHandle=NULL;
			client.submitQuery(exec::user::SQL, plan, QueryResultsListener, NULL, &qryHandle);
			client.waitForResults();
			client.freeQueryResources(&qryHandle);
		}
		client.close();
        
    } catch (std::exception& e) {
        cerr << e.what() << endl;
    }

	std::cout << "\nContinue...\n";
	char placeholder;
	std::cin >> placeholder;
    return 0;
}

