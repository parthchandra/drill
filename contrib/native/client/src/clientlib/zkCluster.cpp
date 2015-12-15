#include "drill/common.hpp"
#include <boost/thread.hpp>
#ifdef _WIN32
#include <zookeeper.h>
#else
#include <zookeeper/zookeeper.h>
#endif
#include "drill/drillClient.hpp"
#include "errmsgs.hpp"
#include "logger.hpp"
#include "zkCluster.hpp"

namespace Drill{

char ZkCluster::s_drillRoot[]="/drill/";
char ZkCluster::s_defaultCluster[]="drillbits1";

ZkCluster::ZkCluster(){
    m_pDrillbits=new String_vector;
    srand (time(NULL));
    m_bConnecting=true;
    memset(&m_id, 0, sizeof(m_id));
}

ZkCluster::~ZkCluster(){
    delete m_pDrillbits;
}

ZooLogLevel ZkCluster::getZkLogLevel(){
    //typedef enum {ZOO_LOG_LEVEL_ERROR=1,
    //    ZOO_LOG_LEVEL_WARN=2,
    //    ZOO_LOG_LEVEL_INFO=3,
    //    ZOO_LOG_LEVEL_DEBUG=4
    //} ZooLogLevel;
    switch(DrillClientConfig::getLogLevel()){
        case LOG_TRACE:
        case LOG_DEBUG:
            return ZOO_LOG_LEVEL_DEBUG;
        case LOG_INFO:
            return ZOO_LOG_LEVEL_INFO;
        case LOG_WARNING:
            return ZOO_LOG_LEVEL_WARN;
        case LOG_ERROR:
        case LOG_FATAL:
        default:
            return ZOO_LOG_LEVEL_ERROR;
    }
    return ZOO_LOG_LEVEL_ERROR;
}

int ZkCluster::connectToZookeeper(const char* connectStr, const char* pathToDrill){
    uint32_t waitTime=30000; // 10 seconds
    zoo_set_debug_level(getZkLogLevel());
    zoo_deterministic_conn_order(1); // enable deterministic order
    m_zh = zookeeper_init(connectStr, watcher, waitTime, 0, this, 0);
    if(!m_zh) {
        m_err = getMessage(ERR_CONN_ZKFAIL);
        return CONN_FAILURE;
    }else{
        m_err="";
        //Wait for the completion handler to signal successful connection
        boost::unique_lock<boost::mutex> bufferLock(this->m_cvMutex);
        boost::system_time const timeout=boost::get_system_time()+ boost::posix_time::milliseconds(waitTime);
        while(this->m_bConnecting) {
            if(!this->m_cv.timed_wait(bufferLock, timeout)){
                m_err = getMessage(ERR_CONN_ZKTIMOUT);
                return CONN_FAILURE;
            }
        }
    }
    if(m_state!=ZOO_CONNECTED_STATE){
        return CONN_FAILURE;
    }
    int rc = ZOK;
    char rootDir[MAX_CONNECT_STR+1];
    if(pathToDrill==NULL || strlen(pathToDrill)==0){
        strcpy(rootDir, (char*)s_drillRoot);
        strcat(rootDir, s_defaultCluster);
    }else{
        strncpy(rootDir, pathToDrill, MAX_CONNECT_STR); rootDir[MAX_CONNECT_STR]=0;
    }
    rc=zoo_get_children(m_zh, (char*)rootDir, 0, m_pDrillbits);
    if(rc!=ZOK){
        m_err=getMessage(ERR_CONN_ZKERR, rc);
        zookeeper_close(m_zh);
        return -1;
    }

    //Let's pick a random drillbit.
    if(m_pDrillbits && m_pDrillbits->count >0){
        int r=rand()%(this->m_pDrillbits->count);
        assert(r<this->m_pDrillbits->count);
        char * bit=this->m_pDrillbits->data[r];
        std::string s;
        s=rootDir +  std::string("/") + bit;
        int buffer_len=MAX_CONNECT_STR;
        char buffer[MAX_CONNECT_STR+1];
        struct Stat stat;
        buffer[MAX_CONNECT_STR]=0;
        rc= zoo_get(m_zh, s.c_str(), 0, buffer,  &buffer_len, &stat);
        if(rc!=ZOK){
            m_err=getMessage(ERR_CONN_ZKDBITERR, rc);
            zookeeper_close(m_zh);
            return -1;
        }
        m_drillServiceInstance.ParseFromArray(buffer, buffer_len);
    }else{
        m_err=getMessage(ERR_CONN_ZKNODBIT);
        zookeeper_close(m_zh);
        return -1;
    }
    return 0;
}

void ZkCluster::close(){
    zookeeper_close(m_zh);
}

void ZkCluster::watcher(zhandle_t *zzh, int type, int state, const char *path, void* context) {
    //From cli.c

    /* Be careful using zh here rather than zzh - as this may be mt code
     * the client lib may call the watcher before zookeeper_init returns */

    ZkCluster* self=(ZkCluster*)context;
    self->m_state=state;
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
        } else if (state == ZOO_AUTH_FAILED_STATE) {
            self->m_err= getMessage(ERR_CONN_ZKNOAUTH);
            zookeeper_close(zzh);
            self->m_zh=0;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            self->m_err= getMessage(ERR_CONN_ZKEXP);
            zookeeper_close(zzh);
            self->m_zh=0;
        }
    }
    // signal the cond var
    {
        if (state == ZOO_CONNECTED_STATE){
            DRILL_LOG(LOG_TRACE) << "Connected to Zookeeper." << std::endl;
        }
        boost::lock_guard<boost::mutex> bufferLock(self->m_cvMutex);
        self->m_bConnecting=false;
    }
    self->m_cv.notify_one();
}

void ZkCluster:: debugPrint(){
    if(m_zh!=NULL && m_state==ZOO_CONNECTED_STATE){
        DRILL_LOG(LOG_TRACE) << m_drillServiceInstance.DebugString() << std::endl;
    }
}

} // namespace Drill
