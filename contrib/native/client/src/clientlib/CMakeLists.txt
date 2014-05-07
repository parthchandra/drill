# Drill Client library

set (CLIENTLIB_SRC_FILES 
    ${CMAKE_CURRENT_SOURCE_DIR}/decimalUtils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/drillClient.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/drillClientImpl.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/recordBatch.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/rpcEncoder.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/rpcDecoder.cpp 
    ${CMAKE_CURRENT_SOURCE_DIR}/rpcMessage.cpp 
    )

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../include )
include_directories(${PROTOBUF_INCLUDE_DIR})

add_library(drillClient SHARED ${CLIENTLIB_SRC_FILES})
target_link_libraries(drillClient ${Boost_LIBRARIES} ${PROTOBUF_LIBRARY}  protomsgs)
