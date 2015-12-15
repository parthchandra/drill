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


#ifndef SOCKWRAPPER_HPP
#define SOCKWRAPPER_HPP

#include "drill/common.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

namespace Drill {

class AsioSocketWrapper{
    public:
    virtual boost::asio::ip::tcp::socket::lowest_layer_type& getSocket() = 0;
};

class SslSocket: 
    public AsioSocketWrapper, 
    public boost::asio::ssl::stream<boost::asio::ip::tcp::socket>{

        SslSocket(boost::asio::io_service& ioService, boost::asio::ssl::context &sslContext) :
            boost::asio::ssl::stream<boost::asio::ip::tcp::socket>(ioService, sslContext) {
            }

        boost::asio::ip::tcp::socket::lowest_layer_type& getSocket(){ return this->lowest_layer();}
};

class Socket: 
    public AsioSocketWrapper, 
    public boost::asio::buffered_stream<boost::asio::ip::tcp::socket>{

    public:
        Socket(boost::asio::io_service& ioService) :
            boost::asio::buffered_stream<boost::asio::ip::tcp::socket>(ioService) {
            }
        boost::asio::ip::tcp::socket::lowest_layer_type& getSocket(){ return this->lowest_layer();}

};


} // namespace Drill

#endif //SOCKWRAPPER_HPP 

