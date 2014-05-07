#ifndef _COMMON_H_
#define _COMMON_H_

#include <stdint.h>
#include <string>
#include <vector>

#define LENGTH_PREFIX_MAX_LENGTH 5
#define LEN_PREFIX_BUFLEN LENGTH_PREFIX_MAX_LENGTH

using std::string;
using std::vector;

typedef vector<uint8_t> DataBuf;

#ifdef _DEBUG
    #define EXTRA_DEBUGGING
    #define CODER_DEBUGGING 
#endif

namespace Drill {

typedef uint8_t Byte_t;
typedef Byte_t * ByteBuf_t;

struct UserServerEndPoint{
    string m_addr;
    int m_port;
    UserServerEndPoint(string addr, int port):m_addr(addr),m_port(port) { }
};

} // namespace Drill
#endif

