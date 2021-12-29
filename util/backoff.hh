//
// Created by jason on 2021/12/29.
//

#pragma once

#include <chrono>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

namespace rafter::util {

class backoff {
 public:
  template <typename Rep, typename Period>
  static backoff linear(
      uint64_t try_limit, std::chrono::duration<Rep, Period> base_interval) {
    return exponential(try_limit, base_interval, 1.0);
  }

  template <typename Rep, typename Period>
  static backoff exponential(
      uint64_t try_limit,
      std::chrono::duration<Rep, Period> base_interval,
      double factor) {
    using namespace std::chrono;
    backoff b;
    b._count = try_limit < 1 ? 1 : try_limit;
    b._base_ns = duration_cast<nanoseconds>(base_interval).count();
    b._factor = factor < 1.0 ? 1.0 : factor;
    return b;
  }

  template <typename Func>
  requires requires(Func f) {
    { f() } -> std::same_as<seastar::future<bool>>;
  }
  seastar::future<bool> attempt(Func func) const {
    bool done = false;
    auto count = _count;
    auto next_wait = std::chrono::nanoseconds(_base_ns);
    while (count--) {
      done = co_await func();
      if (done) {
        break;
      }
      co_await seastar::sleep(next_wait);
      next_wait *= _factor;
    }
    co_return done;
  }

 private:
  uint64_t _count = 0;
  uint64_t _base_ns = 0;
  double _factor = 1.0;
};

}  // namespace rafter::util
