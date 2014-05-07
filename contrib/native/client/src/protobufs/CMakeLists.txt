# Find Protobufs
find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})

message("PROTOBUF_INCLUDE_DIR =  ${PROTOBUF_INCLUDE_DIR}")
message("PROTOBUF_LIBRARY =  ${PROTOBUF_LIBRARY}")

#Generate Protobuf code

set (PROTO_SRC_FILES 
    ${CMAKE_BINARY_DIR}/protobuf/BitControl.proto 
    ${CMAKE_BINARY_DIR}/protobuf/Coordination.proto 
    ${CMAKE_BINARY_DIR}/protobuf/GeneralRPC.proto 
    ${CMAKE_BINARY_DIR}/protobuf/Types.proto 
    ${CMAKE_BINARY_DIR}/protobuf/UserBitShared.proto 
    ${CMAKE_BINARY_DIR}/protobuf/BitData.proto 
    ${CMAKE_BINARY_DIR}/protobuf/ExecutionProtos.proto 
    ${CMAKE_BINARY_DIR}/protobuf/SchemaDef.proto 
    ${CMAKE_BINARY_DIR}/protobuf/User.proto
    )

message("PROTO_SRC_FILES = ${PROTO_SRC_FILES}")

PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${PROTO_SRC_FILES})
message("ProtoSources =  ${ProtoSources}" )
message("ProtoHeaders =  ${ProtoHeaders}" )

#
#   TARGETS
#

#Preprocess to fix protobuf .proto definitions
SET (SRCDIR ${PROJECT_SOURCE_DIR}/../../../protocol/src/main/protobuf )
SET (TARGDIR ${PROJECT_SOURCE_DIR}/build/protobuf)
SET (FNAME Types.proto)

add_custom_target(fixProtobufs
    COMMAND ${PROJECT_SOURCE_DIR}/scripts/fixProtodefs.sh ${SRCDIR} ${TARGDIR} ${FNAME}
)

set(ProtobufIncludes ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)
set(ProtoSources ${ProtoSources} PARENT_SCOPE)
set(ProtoHeaders ${ProtoHeaders} PARENT_SCOPE)

add_library(protomsgs STATIC ${ProtoSources} ${ProtoHeaders})
