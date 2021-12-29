project(rafter_test)

set(SOURCES
    base.cc
    base.hh
    storage_index_test.cc
    storage_segment_manager_test.cc
    storage_segment_test.cc
    transport_exchanger_test.cc
    util.cc
    util.hh
    util_fragmented_temporary_buffer_test.cc
    util_worker_test.cc)

add_executable(${PROJECT_NAME} ${SOURCES})

target_link_libraries(${PROJECT_NAME}
    PUBLIC rafter
    PUBLIC Seastar::seastar
    PUBLIC gtest)
