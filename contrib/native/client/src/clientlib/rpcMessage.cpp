#include "common.hpp"
#include "rpcMessage.hpp"
using Drill::InBoundRpcMessage;
using Drill::OutBoundRpcMessage;
ostream& Drill::operator<< (ostream & out, InBoundRpcMessage & msg) {

    out << "InboundRpcMessage [pBody= (" << msg.m_pbody.size() << " bytes)"  \
        << ", mode=" << msg.m_mode  << ", rpcType=" << msg.m_rpc_type  << ", coordinationId=" << msg.m_coord_id \
        << ", dBodies= (" << /*msg.m_dbody.size() << */ " bytes)" \
        << "]" ;
    return out;
}
ostream& Drill::operator<< (ostream & out, OutBoundRpcMessage & msg) {
    out << "OutboundRpcMessage [pBody= (" << msg.m_pbody->ByteSize() << " bytes)"\
        << ", mode=" << msg.m_mode  << ", rpcType=" << msg.m_rpc_type  << ", coordinationId=" << msg.m_coord_id \
        // << ", dBodies=" << m_dBodies
        << "]";
    return out;
}
