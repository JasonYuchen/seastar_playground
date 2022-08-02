project(rafter_test)

set(SOURCES

    core/helper.hh
    core/peer_test.cc
    core/quiesce_test.cc
    core/raft_log_test.cc
    core/raft_test.cc
    core/rate_limiter_test.cc
    core/read_index_test.cc
    core/remote_test.cc

    protocol/client_test.cc
    protocol/helper.hh
    protocol/raft_test.cc
    protocol/serializer_test.cc

    rafter/api_server_test.cc
    rafter/config_test.cc
    rafter/helper.hh
    rafter/node_test.cc
    rafter/nodehost_test.cc
    rafter/request_test.cc

    rsm/files_test.cc
    rsm/helper.hh
    rsm/membership_test.cc
    rsm/session_manager_test.cc
    rsm/session_test.cc
    rsm/snapshotter_test.cc
    rsm/statemachine_manager_test.cc

    server/environment_test.cc
    server/helper.hh
    server/snapshot_context_test.cc

    storage/helper.hh
    storage/index_test.cc
    storage/logdb_test.cc
    storage/segment_manager_test.cc
    storage/segment_test.cc

    transport/exchanger_test.cc
    transport/express_test.cc
    transport/helper.hh
    transport/registry_test.cc
    transport/snapshot_chunk_tracker_test.cc

    util/fragmented_temporary_buffer_test.cc
    util/helper.hh
    util/worker_test.cc

    base.cc
    base.hh
    test_logdb.cc
    test_logdb.hh
    util.cc
    util.hh
    )

add_executable(${PROJECT_NAME} ${SOURCES})

target_compile_options(${PROJECT_NAME}
    PUBLIC ${RAFTER_CXX_FLAGS}
    )

target_link_libraries(${PROJECT_NAME}
    PUBLIC rafter
    PUBLIC Seastar::seastar
    PUBLIC gtest
    )
