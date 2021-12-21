//
// Created by jason on 2021/9/15.
//

#include "base.hh"

#include <seastar/core/coroutine.hh>

#include "util/signal.hh"

using namespace std;
using namespace seastar;

namespace rafter::test {

void base::SetUp() {
  app_template::config app_cfg;
  app_cfg.auto_handle_sigint_sigterm = false;
  _app = std::make_unique<app_template>(std::move(app_cfg));
  std::promise<void> pr;
  auto fut = pr.get_future();
  _engine_thread = std::thread([this, pr = std::move(pr)]() mutable {
    return _app->run(
        _argc,
        _argv,
        // We cannot use `pr = std::move(pr)` here as it will forbid compilation
        // see https://taylorconor.com/blog/noncopyable-lambdas/
        [&pr]() mutable -> seastar::future<> {
          l.info("reactor engine starting...");
          rafter::util::stop_signal stop_signal;
          pr.set_value();
          auto signum = co_await stop_signal.wait();
          l.info(
              "reactor engine exiting..., caught signal {}:{}",
              signum,
              ::strsignal(signum));
          co_return;
        });
  });
  fut.get();
}

void base::TearDown() {
  vector<std::future<void>> futs;
  for (auto shard = 0; shard < smp::count; ++shard) {
    futs.emplace_back(
        alien::submit_to(*alien::internal::default_instance, shard, [] {
          return make_ready_future<>();
        }));
  }
  for (auto&& fut : futs) {
    fut.get();
  }
  auto ret = ::pthread_kill(_engine_thread.native_handle(), SIGTERM);
  if (ret) {
    l.error("send SIGTERM failed: {}", ret);
    std::abort();
  }
  _engine_thread.join();
}

void base::submit(std::function<seastar::future<>()> func) {
  seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, std::move(func))
      .get();
}

seastar::logger l{"rafter_test"};

}  // namespace rafter::test

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new rafter::test::base(argc, argv));
  return RUN_ALL_TESTS();
}