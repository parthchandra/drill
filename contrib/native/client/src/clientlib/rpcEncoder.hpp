#ifndef RPC_ENCODER_H
#define RPC_ENCODER_H

#include "rpcMessage.hpp"
namespace Drill {

class RpcEncoder {
  public:
    RpcEncoder() {}
    ~RpcEncoder() { }
    bool Encode(DataBuf& buf,OutBoundRpcMessage& msg);
    static const uint32_t HEADER_TAG;
    static const uint32_t PROTOBUF_BODY_TAG;
    static const uint32_t RAW_BODY_TAG;
    static const uint32_t HEADER_TAG_LENGTH;
    static const uint32_t PROTOBUF_BODY_TAG_LENGTH;
    static const uint32_t RAW_BODY_TAG_LENGTH;
};

// copy from java code
inline int getRawVarintSize(uint32_t value) {
    int count = 0;
    while (true) {
        if ((value & ~0x7F) == 0) {
            count++;
            return count;
        } else {
            count++;
            value >>= 7;
        }
    }
}
} // namespace Drill
#endif
