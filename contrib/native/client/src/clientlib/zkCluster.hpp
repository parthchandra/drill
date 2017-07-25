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

#ifndef ZKCLUSTER_HPP
#define ZKCLUSTER_HPP

#include "drill/common.hpp"
#include <boost/thread.hpp>
#ifdef _WIN32
#include <zookeeper.h>
#else
#include <zookeeper/zookeeper.h>
#endif

#include "UserBitShared.pb.h"

namespace Drill {

    /***
     * The Connection class encapsulates a connection to a drillbit. Based on 
     * the connection string and the options, the connection will be either 
     * a simple socket or a socket using an ssl stream. The class also encapsulates
     * connecting to a drillbit directly of thru zookeeper.
     * The Connection class owns the socket but not the io_service that the applications
     * will use to communicate with the server.
     ***/
    class ZkCluster{
        public:
            ZkCluster();
            ~ZkCluster();
            static ZooLogLevel getZkLogLevel();
            // comma separated host:port pairs, each corresponding to a zk
            // server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002
            int connectToZookeeper(const char* connectStr, const char* pathToDrill);
            void close();
            static void watcher(zhandle_t *zzh, int type, int state, const char *path, void* context);
            void debugPrint();
            std::string& getError(){return m_err;}
            const exec::DrillbitEndpoint& getEndPoint(){ return m_drillServiceInstance.endpoint();}

        private:
            static char s_drillRoot[];
            static char s_defaultCluster[];
            zhandle_t* m_zh;
            clientid_t m_id;
            int m_state;
            std::string m_err;

            struct String_vector* m_pDrillbits;

            boost::mutex m_cvMutex;
            // Condition variable to signal connection callback has been processed
            boost::condition_variable m_cv;
            bool m_bConnecting;
            exec::DrillServiceInstance m_drillServiceInstance;
    };
} // namespace Drill

#endif // ZKCLUSTER_HPP


