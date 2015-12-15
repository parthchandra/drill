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

#include <fstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "drill/drillc.hpp"
#include "clientlib/sockWrapper.hpp"

int main(int argc, char* argv[]){
    boost::asio::io_service ioService;
    Drill::AsioSocketWrapper  *pChannel;
    boost::asio::ssl::context sslContext(boost::asio::ssl::context::sslv23);
    bool isSSL = argc==2 && !(strcmp(argv[2], "ssl"));
    if(isSSL){
        sslContext.set_options(
                boost::asio::ssl::context::default_workarounds
                | boost::asio::ssl::context::no_sslv2
                | boost::asio::ssl::context::single_dh_use
        /*  sslContext.set_password_callback(boost::bind(&server::get_password, this)*/);
        sslContext.use_certificate_chain_file("drillCppTestCert.pem");
        sslContext.use_private_key_file("drillCppTestServer.pem", boost::asio::ssl::context::pem);
        sslContext.use_tmp_dh_file("dh512.pem");
        pChannel = new Drill::SslSocket(ioService, sslContext);
    }else{
        pChannel = new Drill::Socket(ioService);
    }

    boost::asio::ip::tcp::socket::lowest_layer_type& socket =  pChannel->getSocket(); 

}
