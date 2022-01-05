cmake_minimum_required(VERSION 3.16)
project(rafter)

set(CMAKE_CXX_STANDARD 20)

find_package(Seastar REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines-ts -Wall -Werror")

# set(CMAKE_CXX_CLANG_TIDY "clang-tidy")

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fno-optimize-sibling-calls")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
endif()

include_directories(${PROJECT_SOURCE_DIR})

add_subdirectory(3rd)
add_subdirectory(test)

set(SOURCES
    core/logger.cc
    core/logger.hh
#    core/peer.cc
#    core/peer.hh
#    core/raft.cc
#    core/raft.hh
#    core/raft_log.cc
#    core/raft_log.hh
#    core/read_index.cc
#    core/read_index.hh
    core/remote.cc
    core/remote.hh
#    protocol/client.hh
    protocol/raft.cc
    protocol/raft.hh
    protocol/serializer.cc
    protocol/serializer.hh
    rafter/config.cc
    rafter/config.hh
    rafter/logger.cc
    rafter/logger.hh
    rafter/nodehost.cc
    rafter/nodehost.hh
#    rsm/logger.cc
#    rsm/logger.hh
    storage/index.cc
    storage/index.hh
#    storage/logdb.cc
#    storage/logdb.hh
    storage/logger.cc
    storage/logger.hh
    storage/segment.cc
    storage/segment.hh
    storage/segment_manager.cc
    storage/segment_manager.hh
#    storage/snapshot.cc
#    storage/snapshot.hh
    storage/stats.cc
    storage/stats.hh
    transport/exchanger.cc
    transport/exchanger.hh
    transport/logger.cc
    transport/logger.hh
#    transport/registry.cc
#    transport/registry.hh
    util/backoff.hh
    util/endian.hh
    util/error.cc
    util/error.hh
    util/fragmented_temporary_buffer.cc
    util/fragmented_temporary_buffer.hh
    util/serializer.hh
    util/signal.cc
    util/signal.hh
    util/types.hh
    util/util.hh
    util/worker.hh)

add_library(${PROJECT_NAME} ${SOURCES})

target_link_libraries(${PROJECT_NAME}
    PUBLIC Seastar::seastar)

#add_executable(${PROJECT_NAME} ${SOURCES})
#
#target_link_libraries(${PROJECT_NAME}
#    PUBLIC Seastar::seastar)
