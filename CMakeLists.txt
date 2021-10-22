cmake_minimum_required(VERSION 3.16)
project(rafter)

set(CMAKE_CXX_STANDARD 20)

find_package(Seastar REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines-ts -Wall -Werror")

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
    util/fragmented_temporary_buffer.cc
    storage/segment_manager.h
    storage/segment_manager.cc
    storage/snapshot.cc
    storage/snapshot.hh
    storage/index.cc
    storage/index.hh
    storage/logdb.cc
    storage/logdb.hh
    util/util.hh)

add_executable(${PROJECT_NAME} ${SOURCES})

target_link_libraries(${PROJECT_NAME}
    PUBLIC Seastar::seastar)
