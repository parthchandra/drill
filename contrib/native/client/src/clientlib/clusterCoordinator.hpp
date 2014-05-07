#ifndef _CLUSTER_COORDINATOR_H
#define _CLUSTER_COORDINATOR_H


namespace Drill {
class ClusterCoordinator {
  public:
    // TODO
    // connect to zookeeper, timeout mills
    bool Start(uint64_t mills) {
        return true;
    }

    // TODO
    // get available endpoints
    vector<UserServerEndPoint> GetAvailableEndpoints();
}

}

#endif
