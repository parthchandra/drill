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


#ifndef STREAMSOCKET_HPP
#define STREAMSOCKET_HPP

#include "drill/common.hpp"
#include "env.h"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

namespace Drill {

typedef boost::asio::ip::tcp::socket::lowest_layer_type streamSocket_t;

class AsioStreamSocket{
    public:
        virtual streamSocket_t& getSocket() = 0;
        virtual ~AsioStreamSocket(){};
        //connectionStatus_t connect();
    private:
        //DrillClientError* m_pError;
};

class Socket: 
    public AsioStreamSocket, 
    public boost::asio::buffered_stream<boost::asio::ip::tcp::socket>{

    public:
        Socket(boost::asio::io_service& ioService) :
            boost::asio::buffered_stream<boost::asio::ip::tcp::socket>(ioService) {
            }
        streamSocket_t& getSocket(){ return this->lowest_layer();}
        ~Socket(){};
};


#if defined(IS_SSL_ENABLED)

class SslSocket: 
    public AsioStreamSocket, 
    public boost::asio::ssl::stream<boost::asio::ip::tcp::socket>{

    public:
        SslSocket(boost::asio::io_service& ioService, boost::asio::ssl::context &sslContext) :
            boost::asio::ssl::stream<boost::asio::ip::tcp::socket>(ioService, sslContext) {
            }

        streamSocket_t& getSocket(){ return this->lowest_layer();}
        ~SslSocket(){};
};
#endif


} // namespace Drill

#endif //STREAMSOCKET_HPP

