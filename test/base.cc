//
// Created by jason on 2021/9/15.
//

#include "base.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/util/file.hh>

#include "rafter/config.hh"
#include "test/util.hh"
#include "util/signal.hh"

using namespace std;
using namespace seastar;

namespace rafter::test {

void base::SetUp() {
  app_template::config app_cfg;
  app_cfg.auto_handle_sigint_sigterm = false;
  _app = make_unique<app_template>(std::move(app_cfg));
  std::promise<void> pr;
  auto fut = pr.get_future();
  // We cannot use `pr = std::move(pr)` here as it will forbid compilation
  // see https://taylorconor.com/blog/noncopyable-lambdas/
  auto engine_func = [&pr]() mutable -> seastar::future<> {
    l.info("reactor engine starting...");
    l.info("initialize config {}", test::util::default_config());
    config::initialize(test::util::default_config());
    pr.set_value();
    rafter::util::stop_signal stop_signal;
    auto signum = co_await stop_signal.wait();
    l.info(
        "reactor engine exiting..., caught signal {}:{}",
        signum,
        ::strsignal(signum));
    co_return;
  };
  _engine_thread = std::thread([this, func = std::move(engine_func)]() mutable {
    return _app->run(_argc, _argv, std::move(func));
  });
  fut.get();
  // FIXME（jyc): move to engine initialization
  submit([] {
    return config::broadcast()
        .then([]() {
          return recursive_remove_directory(config::shard().data_dir);
        })
        .handle_exception([](std::exception_ptr ex) {
          l.warn("remove directory with exception:{}", ex);
        });
  });
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

void base::submit(std::function<seastar::future<>()> func, unsigned shard_id) {
  if (shard_id >= seastar::smp::count) {
    l.error("invalid shard:{}, maximal:{}", shard_id, seastar::smp::count);
    std::abort();
  }
  seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, shard_id, std::move(func))
      .get();
}

seastar::logger l{"rafter_test"};

}  // namespace rafter::test

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new rafter::test::base(argc, argv));
  return RUN_ALL_TESTS();
}