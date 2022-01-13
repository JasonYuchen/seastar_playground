//
// Created by jason on 2022/1/10.
//

#pragma once

#include "util/backoff.hh"

namespace rafter::util {

template <typename Clock = std::chrono::system_clock>
class circuit_breaker {
 public:
  using duration = typename Clock::duration;
  using time_point = typename Clock::time_point;

  enum class state : uint8_t {
    open,
    half_open,
    closed,
  };

  class window {
   public:
    window(duration time, uint64_t buckets)
      : _buckets(buckets)
      , _bucket_time(time / buckets)
      , _last_access(Clock::now()) {}
    void fail() { latest_bucket().failure++; }
    void success() { latest_bucket().success++; }
    uint64_t failures() const noexcept {
      uint64_t count = 0;
      for (const auto& b : _buckets) {
        count += b.failure;
      }
      return count;
    }
    uint64_t successes() const noexcept {
      uint64_t count = 0;
      for (const auto& b : _buckets) {
        count += b.success;
      }
      return count;
    }
    void reset() {
      for (auto& b : _buckets) {
        b = bucket{};
      }
    }

   private:
    struct bucket {
      uint64_t failure = 0;
      uint64_t success = 0;
    };

    bucket& latest_bucket() {
      auto elapsed = Clock::now() - _last_access;
      if (elapsed > _bucket_time) {
        for (size_t i = 0; i < _buckets.size(); ++i) {
          _idx++;
          _buckets[_idx % _buckets.size()].reset();
          elapsed = elapsed - _bucket_time;
          if (elapsed < _bucket_time) {
            break;
          }
        }
        _last_access = Clock::now();
      }
      return _buckets[_idx % _buckets.size()];
    }

    std::vector<bucket> _buckets;
    uint64_t _idx = 0;
    duration _bucket_time;
    time_point _last_access;
  };

  circuit_breaker()
    : _waiter(std::chrono::milliseconds(500), 2.0)
    , _max_wait(std::chrono::seconds(60))
    , _window(std::chrono::seconds(10), 10) {}

  circuit_breaker(
      backoff_waiter<Clock> waiter,
      duration max_wait,
      duration window_time,
      uint64_t window_buckets,
      std::function<bool(circuit_breaker&) noexcept>&& should_trip = {})
    : _waiter(waiter)
    , _max_wait(max_wait)
    , _window(window_time, window_buckets)
    , _should_trip(std::move(should_trip)) {}

  uint64_t successes() const noexcept { return _window.successes(); }

  uint64_t failures() const noexcept { return _window.failures(); }

  uint64_t consecutive_failures() const noexcept {
    return _consecutive_failures;
  }

  void set_broken() noexcept {
    _broken = true;
    trip();
  }

  void success() {
    _waiter.reset();
    if (state() == state::half_open) {
      reset();
    }
    _consecutive_failures = 0;
    _window.success();
  }

  void fail() {
    _window.fail();
    _consecutive_failures++;
    _last_failure = Clock::now();
    if (_should_trip && _should_trip(*this)) {
      trip();
    }
  }

  bool ready() {
    auto s = state();
    if (s == state::half_open) {
      _half_open = false;
    }
    return s == state::closed || s == state::half_open;
  }

  void trip() noexcept {
    _tripped = true;
    _last_failure = Clock::now();
  }

  bool tripped() const noexcept { return _tripped; }

  void reset() noexcept {
    _broken = false;
    _tripped = false;
    _half_open = false;
    _consecutive_failures = 0;
    _window.reset();
  }

  void state() {
    if (!_tripped) {
      return state::closed;
    }

    if (_broken) {
      return state::open;
    }

    auto elapsed = Clock::now() - _last_failure;
    if (_waiter.next_duration() < _max_wait &&
        elapsed > _waiter.next_duration()) {
      if (!_half_open) {
        _half_open = true;
        _waiter.next();
        return state::half_open;
      }
      return state::open;
    }

    return state::open;
  }

 private:
  backoff_waiter<Clock> _waiter;
  duration _max_wait;
  uint64_t _consecutive_failures = 0;
  time_point _last_failure;
  window _window;

  bool _broken = false;
  bool _tripped = false;
  bool _half_open = false;
  std::function<bool(circuit_breaker&) noexcept> _should_trip;
};

}  // namespace rafter::util
