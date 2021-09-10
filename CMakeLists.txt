cmake_minimum_required(VERSION 3.16)
project(seastar_playground)

set(CMAKE_CXX_STANDARD 20)

find_package(Seastar REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines-ts")

add_subdirectory(storage)

# enable LTO/LTCG
#if (CMAKE_BUILD_TYPE STREQUAL "Release")
#    message("lto enabled in release build")
#    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
#else()
#    message("address sanitizer enabled in debug build")
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
#endif()

add_executable(seastar_playground main.cpp)

target_link_libraries(seastar_playground
        PUBLIC Seastar::seastar)