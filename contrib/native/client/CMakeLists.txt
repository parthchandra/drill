cmake_minimum_required(VERSION 2.8)

project(drillclient)

message("CMAKE_SOURCE_DIR = " ${CMAKE_SYSTEM_NAME})
#set(THIRD_PARTY_DIR ${CMAKE_SOURCE_DIR}/thirdparty)
#set(BOOST_ROOT ${THIRD_PARTY_DIR}/boost)
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost REQUIRED COMPONENTS regex system date_time chrono thread log)
include_directories(${Boost_INCLUDE_DIRS})

if(CMAKE_COMPILER_IS_GNUCXX) 
    set(CMAKE_EXE_LINKER_FLAGS "-lrt -lpthread")                                                   
endif()    

add_definitions(-DBOOST_ALL_DYN_LINK)

find_package(Protobuf REQUIRED)
#set(PROTOBUF_INCLUDE_DIR ${THIRD_PARTY_DIR}/protobuf/include)
#set(PROTOBUF_LIBRARY_DIR ${THIRD_PARTY_DIR}/protobuf/lib)
include_directories(${PROTOBUF_INCLUDE_DIR})
#link_directories(${PROTOBUF_LIBRARY_DIR})
link_directories(/usr/local/lib)
message("PROTOBUF_INCLUDE_DIR = " ${PROTOBUF_INCLUDE_DIR})
message("PROTOBUF_LIBRARY = " ${PROTOBUF_LIBRARY})

include_directories("${CMAKE_SOURCE_DIR}/src/clientlib")
add_subdirectory("${CMAKE_SOURCE_DIR}/src/clientlib") 

include_directories(${CMAKE_SOURCE_DIR}/src)
file(GLOB ProtoCpp src/proto-cpp/*.cc)
file(GLOB AsyncDrillClientCpp src/drill-client-async.cc src/rpc-encoder.cc src/rpc-decoder.cc src/rpc-message.cc src/recordBatch.cc)

#add a DEBUG preprocessor macro
set_property(
    DIRECTORY
    PROPERTY COMPILE_DEFINITIONS_DEBUG DEBUG DEBUG=1
    )

message("ProtoCpp = " ${ProtoCpp})
add_executable(async-client example/async-client.cc ${AsyncDrillClientCpp} ${ProtoCpp}) 
add_executable(sqlClient example/sqlClient.cpp ${AsyncDrillClientCpp} ${ProtoCpp})
target_link_libraries(async-client ${Boost_LIBRARIES} ${PROTOBUF_LIBRARY} drillClient)
target_link_libraries(sqlClient ${Boost_LIBRARIES} ${PROTOBUF_LIBRARY} drillClient)
