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

            std::string& getProtocol(){return m_protocol;}
            std::string& getHost(){return m_host;}
            std::string& getPort(){return getPort();}

        private:
            void parseConnectString();
            connectionStatus_t validateConnectionString();
            bool isDirectConnection();
            bool isZookeeperConnection();
            connectionStatus_t getDrillbitEndpoint();
            connectionStatus_t handleError(connectionStatus_t status, std::string msg);

            std::string m_connectString;
            std::string m_pathToDrill;
            std::string m_protocol; 
            std::string m_hostPortStr;
            std::string m_host;
            std::string m_port;

            DrillClientError* m_pError;

    };

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
            ~Channel();
            connectionStatus_t connect();
            template <typename SettableSocketOption> void setOption(SettableSocketOption& option);

        private:
            
            connectionStatus_t connectInternal();

            ConnectionEndpoint* m_pEndpoint;
            boost::asio::io_service m_ioService;
            //boost::asio::ip::tcp::socket m_Socket;
            AsioStreamSocket* m_pSocket;

            //bool m_bIsSSL;
            bool m_bIsConnected;

            DrillClientError* m_pError;

    };

    class SocketChannel: public Channel{
        public:
        SocketChannel(const char* connStr):Channel(connStr){
        }
    };

    class SSLStreamChannel: public Channel{
        public:
        SSLStreamChannel(const char* connStr):Channel(connStr){
        }
    };

    class ChannelFactory{
        static Channel* getChannel(channelType_t t, const char* connStr);
    };


} // namespace Drill

#endif // CHANNEL_HPP

