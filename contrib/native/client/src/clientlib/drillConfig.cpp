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


#include "drill/common.hpp"
#include "drill/drillConfig.hpp"

//#include "drill/drillClient.hpp"
//#include "drill/recordBatch.hpp"
//#include "drillClientImpl.hpp"
//#include "env.h"
//#include "errmsgs.hpp"
#include "logger.hpp"

//#include "Types.pb.h"

namespace Drill{

// Initialize static member of DrillClientConfig
logLevel_t DrillClientConfig::s_logLevel=LOG_ERROR;
uint64_t DrillClientConfig::s_bufferLimit=MAX_MEM_ALLOC_SIZE;
int32_t DrillClientConfig::s_socketTimeout=0;
int32_t DrillClientConfig::s_handshakeTimeout=5;
int32_t DrillClientConfig::s_queryTimeout=180;
int32_t DrillClientConfig::s_heartbeatFrequency=15; // 15 seconds

boost::mutex DrillClientConfig::s_mutex;

DrillClientConfig::DrillClientConfig(){
    initLogging(NULL);
}

DrillClientConfig::~DrillClientConfig(){
    Logger::close();
}

void DrillClientConfig::initLogging(const char* path){
    Logger::init(path);
    setLogLevel(LOG_INFO);
    DRILL_LOG(LOG_INFO) << "Drill Client library." << std::endl;
    DRILL_LOG(LOG_INFO) <<  GIT_COMMIT_PROP << std::endl;
}

void DrillClientConfig::setLogLevel(logLevel_t l){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    s_logLevel=l;
    Logger::s_level=l;
    //boost::log::core::get()->set_filter(boost::log::trivial::severity >= s_logLevel);
}

void DrillClientConfig::setBufferLimit(uint64_t l){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    s_bufferLimit=l;
}

uint64_t DrillClientConfig::getBufferLimit(){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    return s_bufferLimit;
}

void DrillClientConfig::setSocketTimeout(int32_t t){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    s_socketTimeout=t;
}

void DrillClientConfig::setHandshakeTimeout(int32_t t){
    if (t > 0) {
        boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
        s_handshakeTimeout = t;
    }
}

void DrillClientConfig::setQueryTimeout(int32_t t){
    if (t>0){
        boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
        s_queryTimeout=t;
    }
}

void DrillClientConfig::setHeartbeatFrequency(int32_t t){
    if (t>0){
        boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
        s_heartbeatFrequency=t;
    }
}

int32_t DrillClientConfig::getSocketTimeout(){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    return s_socketTimeout;
}

int32_t DrillClientConfig::getHandshakeTimeout(){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    return  s_handshakeTimeout;
}

int32_t DrillClientConfig::getQueryTimeout(){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    return s_queryTimeout;
}

int32_t DrillClientConfig::getHeartbeatFrequency(){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    return s_heartbeatFrequency;
}

logLevel_t DrillClientConfig::getLogLevel(){
    boost::lock_guard<boost::mutex> configLock(DrillClientConfig::s_mutex);
    return s_logLevel;
}


} // namespace Drill
