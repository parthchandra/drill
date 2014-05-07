cmake_minimum_required(VERSION 2.8)

project(drillclient)

message("Project Dir = ${PROJECT_SOURCE_DIR}")
message("Source Dir = ${CMAKE_SOURCE_DIR} ")

# Find Boost
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost REQUIRED COMPONENTS regex system date_time chrono thread log)
include_directories(${Boost_INCLUDE_DIRS})

if(CMAKE_COMPILER_IS_GNUCXX) 
    set(CMAKE_EXE_LINKER_FLAGS "-lrt -lpthread")                                                   
endif()    

add_definitions(-DBOOST_ALL_DYN_LINK)

# Find Protobufs
find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})

#
#   TARGETS
#

# Preprocess to fix protobuf .proto definitions
add_subdirectory("${CMAKE_SOURCE_DIR}/src/protobufs") 
include_directories(${ProtobufIncludes})
message("ProtoSources in root =  ${ProtoSources}" )
set(ProtoSources ${ProtoSources})

# Build the Client Library as a shared library
# include_directories("${CMAKE_SOURCE_DIR}/src/clientlib")
add_subdirectory("${CMAKE_SOURCE_DIR}/src/clientlib") 

include_directories(${CMAKE_SOURCE_DIR}/src/include)

# add a DEBUG preprocessor macro
set_property(
    DIRECTORY
    PROPERTY COMPILE_DEFINITIONS_DEBUG DEBUG DEBUG=1
    )

# Link directory
link_directories(/usr/local/lib)

add_executable(querySubmitter example/querySubmitter.cpp ) 
add_executable(sqlClient example/sqlClient.cpp)

target_link_libraries(querySubmitter ${Boost_LIBRARIES} ${PROTOBUF_LIBRARY} drillClient protomsgs )
target_link_libraries(sqlClient ${Boost_LIBRARIES} ${PROTOBUF_LIBRARY} drillClient protomsgs)


