//
// Created by jason on 2021/12/23.
//

#include <seastar/core/sleep.hh>

#include "transport/exchanger.hh"
#include "test/base.hh"
#include "test/util.hh"

using namespace rafter;
using namespace seastar;

using rafter::test::base;
using rafter::test::l;
using rafter::transport::exchanger;

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
      _exchanger = std::make_unique<sharded<exchanger>>();
      co_await _exchanger->start(std::ref(_config));
      co_await _exchanger->invoke_on_all(&exchanger::start_listen);
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      co_await _exchanger->invoke_on_all(&exchanger::shutdown);
      co_await _exchanger->stop();
      _exchanger.reset();
    });
  }

  static inline rafter::config _config;
  static inline rafter::transport::peer_address _peer = {net::inet_address("::1"), 20615, 10};
  std::unique_ptr<seastar::sharded<exchanger>> _exchanger;
  // TODO
};

RAFTER_TEST_F(exchanger_test, connect) {
  enum class fiber_state {
    noop,
    created,
    fetched,
    exited,
  };
  uint64_t source_stream_id = 0;
  fiber_state state = fiber_state::noop;
  protocol::message_type type = protocol::message_type::noop;
  auto fiber = [&](rpc::source<protocol::message_ptr> source) -> future<> {
    l.info("fiber running at {}", this_shard_id());
    source_stream_id = source.get_id().id;
    state = fiber_state::created;
    while (true) {
      auto m = co_await source();
      if (!m) {
        break;
      } else {
        type = std::get<0>(*m)->type;
        state = fiber_state::fetched;
      }
    }
    state = fiber_state::exited;
    co_return;
  };

  co_await _exchanger->invoke_on_all([fiber](exchanger& exc) -> future<> {
    exc.register_message([fiber](auto& info, auto source) ->future<> {
      // use this source to make a sink for bidirectional communication
      // save this source/sink in other coroutine
      l.info("source from {}", info.addr);
      (void)fiber(source).handle_exception([] (std::exception_ptr ep) {
        ADD_FAILURE() << "exception: " << ep;
      });
      co_return;
    });
    return make_ready_future<>();
  });
  auto sink = co_await _exchanger->invoke_on(0, &exchanger::make_sink_for_message, _peer);
  EXPECT_EQ(state, fiber_state::created);
  EXPECT_EQ(sink.get_id().id, source_stream_id);
  auto msg = make_lw_shared<protocol::message>();
  msg->type = protocol::message_type::election;
  co_await sink(msg);
  co_await sink.flush();
  co_await sleep(std::chrono::milliseconds(500));
  EXPECT_EQ(state, fiber_state::fetched);
  EXPECT_EQ(type, protocol::message_type::election);
  co_await sink.close();
  co_await _exchanger->invoke_on_all(&exchanger::shutdown);
  EXPECT_EQ(state, fiber_state::exited);
  co_return;
}

}  // namespace
