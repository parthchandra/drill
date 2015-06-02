/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/lexical_cast.hpp>
#include "connection.hpp"
#include "errmsgs.hpp"
#include "logger.hpp"
#include "GeneralRPC.pb.h"

namespace Drill{

Connection::Connection(const char* connStr){
    m_connectString=connStr;
}

connectionStatus_t Connection::connect(){
    connectionStatus_t ret=CONN_SUCCESS;
    if(!this->m_bIsConnected){
        parseConnectString();
        if(isZookeeperConnection()){
            if((ret=getDrillbitEndpoint())!=CONN_SUCCESS){
                return ret;
            }
        }else if(isDirectConnection()){

        }else{
            return CONN_INVALID_INPUT;
        }
        DRILL_LOG(LOG_TRACE) << "Connecting to drillbit: " << m_host << ":" << m_port << "." << std::endl;
        ret=this->connect(m_host.c_str(), m_port.c_str());
    }
    m_bIsConnected=ret==CONN_SUCCESS;
    return ret;
}

template <typename SettableSocketOption> void Connection::setOption(SettableSocketOption& option){
    assert(0); 
}

void Connection::parseConnectString(){
    char u[MAX_CONNECT_STR+1];
    assert(!m_connectString.empty());
    strncpy(u, m_connectString.c_str(), MAX_CONNECT_STR); u[MAX_CONNECT_STR]=0;
    char* z=strtok(u, "=");
    char* c=strtok(NULL, "/");
    char* p=strtok(NULL, "");

    if(p!=NULL) m_pathToDrill=std::string("/")+p;
    m_protocol=z; 
    m_hostPortStr=c;
    // if the connection is to a zookeeper , we will get the host and the port only after connecting to the Zookeeper
    if(isDirectConnection()){
        char tempStr[MAX_CONNECT_STR+1];
        strncpy(tempStr, m_hostPortStr.c_str(), MAX_CONNECT_STR); tempStr[MAX_CONNECT_STR]=0;
        m_host=strtok(tempStr, ":");
        m_port=strtok(NULL, "");
    }
    return;
}

bool Connection::isDirectConnection(){
    assert(!m_protocol.empty());
    return (!strcmp(m_protocol.c_str(), "local") || !strcmp(m_protocol.c_str(), "drillbit"));
}

bool Connection::isZookeeperConnection(){
    assert(!m_protocol.empty());
    return (!strcmp(m_protocol.c_str(), "zk"));
}

connectionStatus_t Connection::getDrillbitEndpoint(){
    ZookeeperImpl zook;
    assert(!m_hostPortStr.empty());
    if(zook.connectToZookeeper(m_hostPortStr.c_str(), m_pathToDrill.c_str())!=0){
        return handleConnError(CONN_ZOOKEEPER_ERROR, getMessage(ERR_CONN_ZOOKEEPER, zook.getError().c_str()));
    }
    zook.debugPrint();
    exec::DrillbitEndpoint e=zook.getEndPoint();
    m_host=boost::lexical_cast<std::string>(e.address());
    m_port=boost::lexical_cast<std::string>(e.user_port());
    zook.close();
    return CONN_SUCCESS;
}

} // namespace Drill
