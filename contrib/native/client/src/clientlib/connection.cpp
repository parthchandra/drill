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

#include "drill/clientConfig.hpp"
#include "drill/clientError.hpp"
#include "connection.hpp"
#include "errmsgs.hpp"
#include "logger.hpp"
#include "zkCluster.hpp"

#include "GeneralRPC.pb.h"

namespace Drill{

Connection::Connection(const char* connStr){
    m_connectString=connStr;
    m_bIsConnected=false;
    m_bIsSSL=false;
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
        ret=this->connectInternal();
    }
    m_bIsConnected=ret==CONN_SUCCESS;
    return ret;
}

template <typename SettableSocketOption> void Connection::setOption(SettableSocketOption& option){
    //TODO:
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
    ZkCluster zook;
    assert(!m_hostPortStr.empty());
    zook.debugPrint();
    if(zook.connectToZookeeper(m_hostPortStr.c_str(), m_pathToDrill.c_str())!=0){
        return CONN_ZOOKEEPER_ERROR;
    }
    exec::DrillbitEndpoint e=zook.getEndPoint();
    m_host=boost::lexical_cast<std::string>(e.address());
    m_port=boost::lexical_cast<std::string>(e.user_port());
    zook.close();
    return CONN_SUCCESS;
}

connectionStatus_t Connection::connectInternal(){
    using boost::asio::ip::tcp;
    tcp::endpoint endpoint;
    const char* host=this->m_host.c_str();
    const char* port=this->m_port.c_str();
    try{
        tcp::resolver resolver(m_io_service);
        tcp::resolver::query query(tcp::v4(), host, port);
        tcp::resolver::iterator iter = resolver.resolve(query);
        tcp::resolver::iterator end;
        while (iter != end){
            endpoint = *iter++;
            DRILL_LOG(LOG_TRACE) << endpoint << std::endl;
        }
        boost::system::error_code ec;
        m_socket.connect(endpoint, ec);
        if(ec){
            return CONN_FAILURE;
        }
    }catch(std::exception e){
        // Handle case when the hostname cannot be resolved. "resolve" is hard-coded in boost asio resolver.resolve
        if (!strcmp(e.what(), "resolve")) {
            return CONN_HOSTNAME_RESOLUTION_ERROR;
        }
        return CONN_FAILURE;
    }

    // set socket keep alive
    boost::asio::socket_base::keep_alive keepAlive(true);
    m_socket.set_option(keepAlive);
	// set no_delay
    boost::asio::ip::tcp::no_delay noDelay(true);
    m_socket.set_option(noDelay);

    //
    // We put some OS dependent code here for timing out a socket. Mostly, this appears to
    // do nothing. Should we leave it in there?
    //
    setSocketTimeout(m_socket, DrillClientConfig::getSocketTimeout());

    return CONN_SUCCESS;
}

} // namespace Drill
