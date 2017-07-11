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

#include "drill/drillConfig.hpp"
#include "drill/drillError.hpp"
#include "channel.hpp"
#include "errmsgs.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include "zookeeperClient.hpp"

#include "GeneralRPC.pb.h"

namespace Drill{

ConnectionEndpoint::ConnectionEndpoint(const char* connStr){
    m_connectString=connStr;
    m_pError=NULL;
}

ConnectionEndpoint::~ConnectionEndpoint(){
    if(m_pError!=NULL){
        delete m_pError; m_pError=NULL;
    }
}

connectionStatus_t ConnectionEndpoint::getDrillbitEndpoint(){
    connectionStatus_t ret=CONN_SUCCESS;
    parseConnectString();
    if(isZookeeperConnection()){
        if((ret=getDrillbitEndpointFromZk())!=CONN_SUCCESS){
            return ret;
        }
    }else if(!this->isDirectConnection()){
        return handleError(CONN_INVALID_INPUT, getMessage(ERR_CONN_UNKPROTO, this->getProtocol().c_str()));
    }
    return ret;
}

void ConnectionEndpoint::parseConnectString(){
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

bool ConnectionEndpoint::isDirectConnection(){
    assert(!m_protocol.empty());
    return (!strcmp(m_protocol.c_str(), "local") || !strcmp(m_protocol.c_str(), "drillbit"));
}

bool ConnectionEndpoint::isZookeeperConnection(){
    assert(!m_protocol.empty());
    return (!strcmp(m_protocol.c_str(), "zk"));
}

connectionStatus_t ConnectionEndpoint::getDrillbitEndpointFromZk(){
    ZookeeperClient zook(m_pathToDrill);
    assert(!m_hostPortStr.empty());
    std::vector<std::string> drillbits;
    if(zook.getAllDrillbits(m_hostPortStr.c_str(), drillbits)!=0){
        return handleError(CONN_ZOOKEEPER_ERROR, getMessage(ERR_CONN_ZOOKEEPER, zook.getError().c_str()));
    }
    if (drillbits.empty()){
        return handleError(CONN_FAILURE, getMessage(ERR_CONN_ZKNODBIT));
    }
    Utils::shuffle(drillbits);
    exec::DrillbitEndpoint endpoint;
    int err = zook.getEndPoint(drillbits[drillbits.size() -1], endpoint);// get the last one in the list
    if(!err){
        m_host=boost::lexical_cast<std::string>(endpoint.address());
        m_port=boost::lexical_cast<std::string>(endpoint.user_port());
    }
    DRILL_MT_LOG(DRILL_LOG(LOG_TRACE) << "Choosing drillbit <" << (drillbits.size() - 1)  << ">. Selected " << endpoint.DebugString() << std::endl;)
        zook.close();
    return CONN_SUCCESS;
}

connectionStatus_t ConnectionEndpoint::handleError(connectionStatus_t status, std::string msg){
    DrillClientError* pErr = new DrillClientError(status, DrillClientError::CONN_ERROR_START+status, msg);
    if(m_pError!=NULL){ delete m_pError; m_pError=NULL;}
    m_pError=pErr;
    return status;
}

/*******************
 *  ChannelFactory
 * *****************/
Channel* ChannelFactory::getChannel(channelType_t t, const char* connStr){
    Channel* pChannel=NULL;
    switch(t){
        case CHANNEL_TYPE_SOCKET:
            pChannel=new SocketChannel(connStr);
            break;
#if defined(IS_SSL_ENABLED)
        case CHANNEL_TYPE_SSLSTREAM:
            pChannel=new SSLStreamChannel(connStr);
            break;
#endif
        default:
            DRILL_LOG(LOG_ERROR) << "Channel type " << t << " is not supported." << std::endl;
            break;
    }
    return pChannel;
}



/*******************
 *  Channel
 * *****************/

Channel::Channel(const char* connStr){
    m_pEndpoint=new ConnectionEndpoint(connStr);
    m_pSocket=NULL;
    m_bIsConnected=false;
    m_pError=NULL;
}

Channel::~Channel(){
    if(m_pEndpoint!=NULL){
        delete m_pEndpoint; m_pEndpoint=NULL;
    }
    if(m_pSocket!=NULL){
        delete m_pSocket; m_pSocket=NULL;
    }
    if(m_pError!=NULL){
        delete m_pError; m_pError=NULL;
    }
}

template <typename SettableSocketOption> void Channel::setOption(SettableSocketOption& option){
    //TODO: May be useful some day. For the moment, we only need to set some well known options after we connect.
    assert(0); 
}

connectionStatus_t Channel::init(ChannelContext_t* pContext){
    connectionStatus_t ret=CONN_SUCCESS;
    return ret;
}

connectionStatus_t Channel::connect(){
    connectionStatus_t ret=CONN_SUCCESS;
    if(!this->m_bIsConnected){
        ret=m_pEndpoint->getDrillbitEndpoint();
        if(ret==CONN_SUCCESS){
            DRILL_LOG(LOG_TRACE) << "Connecting to drillbit: " 
                << m_pEndpoint->getHost() 
                << ":" << m_pEndpoint->getPort() 
                << "." << std::endl;
            ret=this->connectInternal();
        }
    }
    m_bIsConnected=(ret==CONN_SUCCESS);
    return ret;
}

connectionStatus_t Channel::handleError(connectionStatus_t status, std::string msg){
    DrillClientError* pErr = new DrillClientError(status, DrillClientError::CONN_ERROR_START+status, msg);
    if(m_pError!=NULL){ delete m_pError; m_pError=NULL;}
    m_pError=pErr;
    return status;
}

connectionStatus_t Channel::connectInternal(){
    using boost::asio::ip::tcp;
    tcp::endpoint endpoint;
    const char* host=m_pEndpoint->getHost().c_str();
    const char* port=m_pEndpoint->getPort().c_str();
    try{
        tcp::resolver resolver(m_ioService);
        tcp::resolver::query query(tcp::v4(), host, port);
        tcp::resolver::iterator iter = resolver.resolve(query);
        tcp::resolver::iterator end;
        while(iter != end){
            endpoint = *iter++;
            DRILL_LOG(LOG_TRACE) << endpoint << std::endl;
        }
        boost::system::error_code ec;
        m_pSocket->getInnerSocket().connect(endpoint, ec);
        if(ec){
            return handleError(CONN_FAILURE, getMessage(ERR_CONN_FAILURE, host, port, ec.message().c_str()));
        }
    }catch(std::exception e){
        // Handle case when the hostname cannot be resolved. "resolve" is hard-coded in boost asio resolver.resolve
        if (!strcmp(e.what(), "resolve")) {
            return handleError(CONN_HOSTNAME_RESOLUTION_ERROR, getMessage(ERR_CONN_EXCEPT, e.what()));
        }
        return handleError(CONN_FAILURE, getMessage(ERR_CONN_EXCEPT, e.what()));
    }

    // set socket keep alive
    boost::asio::socket_base::keep_alive keepAlive(true);
    m_pSocket->getInnerSocket().set_option(keepAlive);
	// set no_delay
    boost::asio::ip::tcp::no_delay noDelay(true);
    m_pSocket->getInnerSocket().set_option(noDelay);
    // set reuse addr
    boost::asio::socket_base::reuse_address reuseAddr(true);
    m_pSocket->getInnerSocket().set_option(reuseAddr);
    return this->protocolHandshake();

}

connectionStatus_t SocketChannel::init(ChannelContext_t* pContext){
    connectionStatus_t ret=CONN_SUCCESS;
    m_pSocket=new Socket(m_ioService);
    if(m_pSocket!=NULL){
        ret=Channel::init(pContext);
    }else{
        DRILL_LOG(LOG_ERROR) << "Channel initialization failure. " << std::endl;
        ret=CONN_FAILURE;
    }
    return ret;
}

#if defined(IS_SSL_ENABLED)
connectionStatus_t SSLStreamChannel::init(ChannelContext_t* pContext){
    connectionStatus_t ret=CONN_SUCCESS;
    m_pSocket=new SslSocket(m_ioService, *pContext->getSslContext());
    if(m_pSocket!=NULL){
        ret=Channel::init(pContext);
    }else{
        DRILL_LOG(LOG_ERROR) << "Channel initialization failure. " << std::endl;
        ret=CONN_FAILURE;
    }
    return ret;
}
#endif

} // namespace Drill
