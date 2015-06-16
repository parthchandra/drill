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
            std::string& getPort(){return getPort();}

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
            ChannelContext();
            ~ChannelContext();
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
     * The class also provides interfaces to handle async send and receive methods (with timeout)
     * The channel class owns the socket and the io_service that the applications
     * will use to communicate with the server.
     ***/
    class Channel{
        public: 
            Channel(const char* connStr);
            virtual ~Channel();
            virtual connectionStatus_t init(ChannelContext_t* context)=0;
            connectionStatus_t connect();
            template <typename SettableSocketOption> void setOption(SettableSocketOption& option);

        protected:
            connectionStatus_t handleError(connectionStatus_t status, std::string msg);

            boost::asio::io_service m_ioService;
            AsioStreamSocket* m_pSocket;

        private:
            
            connectionStatus_t connectInternal();

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
        static Channel* getChannel(channelType_t t, const char* connStr);
    };


} // namespace Drill

#endif // CHANNEL_HPP

