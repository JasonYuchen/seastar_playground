project(rafter_test)

set(SOURCES
    base.cc
    storage_index_test.cc
    util_fragmented_temporary_buffer_test.cc)

add_executable(${PROJECT_NAME} ${SOURCES})

target_link_libraries(${PROJECT_NAME}
    PUBLIC rafter
    PUBLIC Seastar::seastar
    PUBLIC gtest)
