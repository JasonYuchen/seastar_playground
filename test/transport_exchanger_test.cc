//
// Created by jason on 2021/12/23.
//

#include <seastar/core/sleep.hh>

#include "test/base.hh"
#include "test/util.hh"
#include "transport/exchanger.hh"
#include "transport/registry.hh"

using namespace rafter;
using namespace seastar;

using rafter::test::base;
using rafter::test::l;
using rafter::transport::exchanger;
using rafter::transport::registry;

namespace {

class exchanger_test : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    _config = test::util::default_config();
    _config.listen_address = "::1";
    _config.listen_port = 20615;
  }

  void SetUp() override {
    base::submit([this]() -> future<> {
      _registry = std::make_unique<sharded<registry>>();
      co_await _registry->start();
      co_await _registry->invoke_on_all([](registry& reg) -> future<> {
        reg.update(_gid, _peer);
        return make_ready_future<>();
      });
      _exchanger = std::make_unique<sharded<exchanger>>();
      co_await _exchanger->start(std::ref(_config), std::ref(*_registry));
      co_await _exchanger->invoke_on_all(&exchanger::start_listen);
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      co_await _exchanger->invoke_on_all(&exchanger::shutdown);
      co_await _exchanger->stop();
      co_await _registry->stop();
      _exchanger.reset();
      _registry.reset();
    });
  }

  static inline rafter::config _config;
  static inline rafter::protocol::group_id _gid = {1, 1};
  static inline rafter::transport::peer_address _peer = {
      net::inet_address("::1"), 20615, 10};
  std::unique_ptr<seastar::sharded<registry>> _registry;
  std::unique_ptr<seastar::sharded<exchanger>> _exchanger;
};

RAFTER_TEST_F(exchanger_test, connect) {
  protocol::message_type type = protocol::message_type::noop;
  co_await _exchanger->invoke_on_all([&type](exchanger& exc) -> future<> {
    exc.register_message([&type](auto&, protocol::message_ptr msg) {
      type = msg->type;
      EXPECT_EQ(msg->type, protocol::message_type::election);
      return rpc::no_wait;
    });
    return make_ready_future<>();
  });
  auto msg = make_lw_shared<protocol::message>();
  msg->cluster = 1;
  msg->to = 1;
  msg->type = protocol::message_type::election;
  co_await _exchanger->invoke_on(0, &exchanger::send_message, msg);
  co_await sleep(std::chrono::milliseconds(500));
  EXPECT_EQ(type, protocol::message_type::election);
  co_return;
}

RAFTER_TEST_F(exchanger_test, streaming) {
  enum class fiber_state {
    noop,
    created,
    fetched,
    exited,
  };
  uint64_t source_stream_id = 0;
  fiber_state state = fiber_state::noop;
  std::string payload;
  auto fiber =
      [&](rpc::source<protocol::snapshot_chunk_ptr> source) -> future<> {
    l.info("fiber running at {}", this_shard_id());
    source_stream_id = source.get_id().id;
    state = fiber_state::created;
    while (true) {
      auto m = co_await source();
      if (!m) {
        break;
      }
      payload = std::get<0>(*m)->data;
      state = fiber_state::fetched;
    }
    state = fiber_state::exited;
    co_return;
  };

  co_await _exchanger->invoke_on_all([fiber](exchanger& exc) -> future<> {
    exc.register_snapshot_chunk(
        [fiber](
            auto& info,
            uint64_t cluster,
            uint64_t from,
            uint64_t to,
            auto source) -> future<> {
          // use this source to make a sink for bidirectional communication
          // save this source/sink in other coroutine
          l.info(
              "source from {}, cluster:{}, from:{}, to:{}",
              info.addr,
              cluster,
              from,
              to);
          (void)fiber(source).handle_exception([](std::exception_ptr ep) {
            ADD_FAILURE() << "exception: " << ep;
          });
          co_return;
        });
    return make_ready_future<>();
  });
  auto sink = co_await _exchanger->invoke_on(
      0, &exchanger::make_sink_for_snapshot_chunk, _gid.cluster, 2, _gid.node);
  EXPECT_EQ(state, fiber_state::created);
  EXPECT_EQ(sink.get_id().id, source_stream_id);
  auto chunk = make_lw_shared<protocol::snapshot_chunk>();
  chunk->group_id = _gid;
  chunk->data = "hello world";
  co_await sink(chunk);
  co_await sink.flush();
  co_await sleep(std::chrono::milliseconds(500));
  EXPECT_EQ(state, fiber_state::fetched);
  EXPECT_EQ(payload, chunk->data);
  co_await sink.close();
  co_await _exchanger->invoke_on_all(&exchanger::shutdown);
  EXPECT_EQ(state, fiber_state::exited);
  co_return;
}

RAFTER_TEST_F(exchanger_test, DISABLED_broken_pipe_is_reported) { co_return; }

RAFTER_TEST_F(exchanger_test, DISABLED_route_requests_by_group) { co_return; }

RAFTER_TEST_F(exchanger_test, DISABLED_new_peer_discovery) { co_return; }

}  // namespace
