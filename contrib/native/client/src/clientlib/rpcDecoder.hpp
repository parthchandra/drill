#ifndef RPC_DECODER_H
#define RPC_DECODER_H

#include "rpcMessage.hpp"

namespace Drill {
class RpcDecoder {
  public:
    RpcDecoder() { }
    ~RpcDecoder() { }
    // bool Decode(const DataBuf& buf);
    // bool Decode(const DataBuf& buf, InBoundRpcMessage& msg);
    static int LengthDecode(const uint8_t* buf, uint32_t* length); // read the length prefix (at most 4 bytes)
    static int Decode(const uint8_t* buf, int size, InBoundRpcMessage& msg);
};



} // namespace Drill
#endif
