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

#ifndef CHANNEL_HPP
#define CHANNEL_HPP

#include "drill/common.hpp"
#include "streamSocket.hpp"

class DrillClientError;

namespace Drill {

    class ConnectionEndpoint{
        public:
            ConnectionEndpoint(const char* connStr);
            ~ConnectionEndpoint();

            //parse the connection string and set up the host and port to connect to
            connectionStatus_t getDrillbitEndpoint();

            std::string& getProtocol(){return m_protocol;}
            std::string& getHost(){return m_host;}
            std::string& getPort(){return m_port;}

        private:
            void parseConnectString();
            connectionStatus_t validateConnectionString();
            bool isDirectConnection();
            bool isZookeeperConnection();
            connectionStatus_t getDrillbitEndpointFromZk();
            connectionStatus_t handleError(connectionStatus_t status, std::string msg);

            std::string m_connectString;
            std::string m_pathToDrill;
            std::string m_protocol; 
            std::string m_hostPortStr;
            std::string m_host;
            std::string m_port;

            DrillClientError* m_pError;

    };

    class ChannelContext{
        public:
            ChannelContext(){ m_pSslContext=NULL;};
            ~ChannelContext(){};
            void setSslContext(boost::asio::ssl::context* c){
                this->m_pSslContext=c;
            }
            boost::asio::ssl::context* getSslContext(){ return m_pSslContext;}
        private:
            boost::asio::ssl::context* m_pSslContext;
    };

    typedef ChannelContext ChannelContext_t; 

    /***
     * The Channel class encapsulates a connection to a drillbit. Based on 
     * the connection string and the options, the connection will be either 
     * a simple socket or a socket using an ssl stream. The class also encapsulates
     * connecting to a drillbit directly or thru zookeeper.
     * The channel class owns the socket and the io_service that the applications
     * will use to communicate with the server.
     ***/
    class Channel{
        public: 
            Channel(const char* connStr);
            virtual ~Channel();
            virtual connectionStatus_t init(ChannelContext_t* context)=0;
            connectionStatus_t connect();
            connectionStatus_t protocolClose();
            template <typename SettableSocketOption> void setOption(SettableSocketOption& option);
            DrillClientError* getError(){ return m_pError;}

            boost::asio::io_service& getIOService(){
                return m_ioService;
            }

            // returns a reference to the underlying socket 
            // This access should really be removed and encapsulated in calls that 
            // manage async_send and async_recv 
            // Until then we will let DrillClientImpl have direct access
            streamSocket_t& getInnerSocket(){
                return m_pSocket->getInnerSocket();
            }
            
            AsioStreamSocket& getSocketStream(){
                return *m_pSocket;
            }

        protected:
            connectionStatus_t handleError(connectionStatus_t status, std::string msg);

            boost::asio::io_service m_ioService;
            AsioStreamSocket* m_pSocket;

        private:
            
            connectionStatus_t connectInternal();
            connectionStatus_t protocolHandshake(){
                connectionStatus_t status = CONN_SUCCESS;
                try{
                    m_pSocket->protocolHandshake();
                } catch (boost::system::system_error e) {
                    status = handleError(CONN_HANDSHAKE_FAILED, e.what());
                }
                return status;
            }

            ConnectionEndpoint* m_pEndpoint;

            bool m_bIsConnected;
            DrillClientError* m_pError;

    };

    class SocketChannel: public Channel{
        public:
            SocketChannel(const char* connStr):Channel(connStr){
            }
            connectionStatus_t init(ChannelContext_t* context=NULL);
    };

    class SSLStreamChannel: public Channel{
        public:
            SSLStreamChannel(const char* connStr):Channel(connStr){
            }
            connectionStatus_t init(ChannelContext_t* context);
    };

    class ChannelFactory{
        public:
            static Channel* getChannel(channelType_t t, const char* connStr);
    };


} // namespace Drill

#endif // CHANNEL_HPP

