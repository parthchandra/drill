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
typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket> sslTCPSocket_t;
typedef boost::asio::ip::tcp::socket basicTCPSocket_t;


// Some helper typedefs to define the highly templatized boost::asio methods
typedef boost::asio::const_buffers_1 ConstBufferSequence; 
typedef boost::asio::mutable_buffers_1 MutableBufferSequence;

// ReadHandlers have different possible signatures.
//
// As a standard C-type callback
//    typedef void (*ReadHandler)(const boost::system::error_code& ec, std::size_t bytes_transferred);
//
// Or as a C++ functor
//    struct ReadHandler {
//        virtual void operator()(
//                const boost::system::error_code& ec,
//                std::size_t bytes_transferred) = 0;
//};
//
// We need a different signature though, since we need to pass in a member function of a drill client 
// class (which is C++), as a functor generated by boost::bind as a ReadHandler
// 
typedef boost::function<void (const boost::system::error_code& ec, std::size_t bytes_transferred) > ReadHandler;

class AsioStreamSocket{
    public:
        virtual ~AsioStreamSocket(){};
        virtual streamSocket_t& getInnerSocket() = 0;

        virtual std::size_t writeSome(
                const ConstBufferSequence& buffers,
                boost::system::error_code & ec) = 0;

        virtual std::size_t readSome(
                const MutableBufferSequence& buffers,
                boost::system::error_code & ec) = 0;

        //
        // boost::asio::async_read has the signature 
        // template<
        //     typename AsyncReadStream,
        //     typename MutableBufferSequence,
        //     typename ReadHandler>
        // void-or-deduced async_read(
        //     AsyncReadStream & s,
        //     const MutableBufferSequence & buffers,
        //     ReadHandler handler);
        //
        // For our use case, the derived class will have an instance of a concrete type for AsyncReadStream which 
        // will implement the requirements for the AsyncReadStream type. We need not pass that in as a parameter 
        // since the class already has the value
        // The method is templatized since the ReadHandler type is dependent on the class implementing the read 
        // handler (basically the class using the asio stream)
        //
        virtual void asyncRead( const MutableBufferSequence & buffers, ReadHandler handler) = 0;

        virtual void protocolHandshake() = 0;
        virtual void protocolClose() = 0;
};

class Socket: 
    public AsioStreamSocket, 
    public basicTCPSocket_t{

    public:
        Socket(boost::asio::io_service& ioService) : basicTCPSocket_t(ioService) {
            }

        ~Socket(){
            boost::system::error_code ignorederr;
            this->shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignorederr);
            this->close();
        };

        basicTCPSocket_t& getSocketStream(){ return *this;}

        streamSocket_t& getInnerSocket(){ return this->lowest_layer();}

        std::size_t writeSome(
                const ConstBufferSequence& buffers,
                boost::system::error_code & ec){
            return this->write_some(buffers, ec);
        }

        std::size_t readSome(
                const MutableBufferSequence& buffers,
                boost::system::error_code & ec){
            return this->read_some(buffers, ec);
        }

        void asyncRead( const MutableBufferSequence & buffers, ReadHandler handler){
            return async_read(*this, buffers, handler);
        }

        void protocolHandshake(){}; //nothing to do
        void protocolClose(){ 
            // shuts down the socket!
            boost::system::error_code ignorederr;
            ((basicTCPSocket_t*)this)->shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                ignorederr
                );         
        } 
};


#if defined(IS_SSL_ENABLED)

class SslSocket: 
    public AsioStreamSocket, 
    public sslTCPSocket_t{

    public:
        SslSocket(boost::asio::io_service& ioService, boost::asio::ssl::context &sslContext) :
            sslTCPSocket_t(ioService, sslContext) {
            }

        ~SslSocket(){};

        sslTCPSocket_t& getSocketStream(){ return *this;}

        streamSocket_t& getInnerSocket(){ return this->lowest_layer();}

        std::size_t writeSome(
                const ConstBufferSequence& buffers,
                boost::system::error_code & ec){
            return this->write_some(buffers, ec);
        }

        std::size_t readSome(
                const MutableBufferSequence& buffers,
                boost::system::error_code & ec){
            return this->read_some(buffers, ec);
        }

        void asyncRead( const MutableBufferSequence & buffers, ReadHandler handler){
            return async_read(*this, buffers, handler);
        }

        //
        // public method that can be invoked by callers to invoke the ssl handshake
        // throws: boost::system::system_error
        void protocolHandshake(){
            this->handshake(boost::asio::ssl::stream<boost::asio::ip::tcp::socket>::client);
            return;
        };
        //
        // public method that can be invoked by callers to invoke a clean ssl shutdown
        // throws: boost::system::system_error
        void protocolClose(){
            try{
                this->shutdown();
            }catch(boost::system::system_error e){
                //swallow the exception. The channel is unusable anyway
            }
            // shuts down the socket!
            boost::system::error_code ignorederr;
            this->lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                ignorederr
                );         
            return;
        };

};
#endif


} // namespace Drill

#endif //STREAMSOCKET_HPP

