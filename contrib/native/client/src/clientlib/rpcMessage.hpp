#ifndef RPC_MESSAGE_H
#define RPC_MESSAGE_H

#include <ostream>
#include <google/protobuf/message_lite.h>
#include "GeneralRPC.pb.h"

using std::ostream;
using exec::rpc::RpcMode;

namespace Drill {

class InBoundRpcMessage {
  public:
    RpcMode m_mode;
    int m_rpc_type;
    int m_coord_id;
    DataBuf m_pbody;
    ByteBuf_t m_dbody;
    friend ostream& operator<< (ostream & out, InBoundRpcMessage& msg);

};

class OutBoundRpcMessage {
  public:
    RpcMode m_mode;
    int m_rpc_type;
    int m_coord_id;
    const google::protobuf::MessageLite* m_pbody;

    OutBoundRpcMessage(RpcMode mode, int rpc_type, int coord_id, const google::protobuf::MessageLite* pbody):
        m_mode(mode), m_rpc_type(rpc_type), m_coord_id(coord_id), m_pbody(pbody) { }
    friend ostream& operator<< (ostream & out, OutBoundRpcMessage& msg);
};
}

#endif
