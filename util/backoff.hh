//
// Created by jason on 2021/12/29.
//

#pragma once

#include <chrono>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

namespace rafter::util {

template <typename Clock = std::chrono::system_clock>
class backoff_waiter {
 public:
  using duration = typename Clock::duration;

  explicit backoff_waiter(duration base_interval, double factor = 1.0)
    : _base(base_interval), _next(base_interval), _factor(factor) {}

  duration next() {
    auto ret = _next;
    _next *= _factor;
    return ret;
  }

  duration next_duration() const noexcept { return _next; }

  void reset() { _next = _base; }

 private:
  duration _base;
  duration _next;
  double _factor = 1.0;
};

template <typename Clock = std::chrono::system_clock>
class backoff {
 public:
  using duration = typename Clock::duration;

  static backoff linear(uint64_t try_limit, duration base_interval) {
    return exponential(try_limit, base_interval, 1.0);
  }

  static backoff exponential(
      uint64_t try_limit, duration base_interval, double factor) {
    using namespace std::chrono;
    backoff b;
    b._count = try_limit < 1 ? 1 : try_limit;
    b._base = base_interval;
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
    auto waiter = backoff_waiter<Clock>(_base, _factor);
    while (count--) {
      done = co_await func();
      if (done) {
        break;
      }
      co_await seastar::sleep(waiter.next());
    }
    co_return done;
  }

 private:
  uint64_t _count = 0;
  duration _base;
  double _factor = 1.0;
};

}  // namespace rafter::util
