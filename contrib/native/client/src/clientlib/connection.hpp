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

#ifndef CONNECTION_HPP
#define CONNECTION_HPP

#include "drill/common.hpp"
#include "streamSocket.hpp"

class DrillClientError;

namespace Drill {

    /***
     * The Connection class encapsulates a connection to a drillbit. Based on 
     * the connection string and the options, the connection will be either 
     * a simple socket or a socket using an ssl stream. The class also encapsulates
     * connecting to a drillbit directly of thru zookeeper.
     * The Connection class owns the socket but not the io_service that the applications
     * will use to communicate with the server.
     ***/
    class Connection{
        public:
            Connection(const char* connStr, bool useSSL);
            ~Connection();
            connectionStatus_t connect();
            template <typename SettableSocketOption> void setOption(SettableSocketOption& option);

        private:
            void parseConnectString();
            connectionStatus_t validateConnectionString();
            bool isDirectConnection();
            bool isZookeeperConnection();
            connectionStatus_t getDrillbitEndpoint();
            connectionStatus_t connectInternal();
            connectionStatus_t handleError(connectionStatus_t status, std::string msg);

            std::string m_connectString;
            std::string m_pathToDrill;
            std::string m_protocol; 
            std::string m_hostPortStr;
            std::string m_host;
            std::string m_port;

            bool m_bIsConnected;
            bool m_bIsSSL;

            DrillClientError* m_pError;

    };
} // namespace Drill

#endif // CONNECTION_HPP

