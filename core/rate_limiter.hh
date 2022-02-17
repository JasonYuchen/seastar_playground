//
// Created by jason on 2022/2/14.
//

#pragma once

#include <unordered_map>

#include "util/rate_limiter.hh"

namespace rafter::core {

class rate_limiter {
 public:
  explicit rate_limiter(uint64_t max_bytes) : _limiter(max_bytes) {}
  bool enabled() const noexcept { return _limiter.enabled(); }
  void tick() noexcept { _tick++; }
  uint64_t get_tick() const noexcept { return _tick; }
  void increase(uint64_t bytes) noexcept { _limiter.increase(bytes); }
  void decrease(uint64_t bytes) noexcept { _limiter.decrease(bytes); }
  void set(uint64_t bytes) noexcept { _limiter.set(bytes); }
  uint64_t get() const noexcept { return _limiter.get(); }
  void reset() noexcept { _peers.clear(); }
  void set_peer(uint64_t node_id, uint64_t bytes) {
    _peers[node_id] = peer{.tick = _tick, .in_memory_log_bytes = bytes};
  }
  bool rate_limited() {
    auto l = limited();
    if (l != _limited) {
      if (_tick_limited == 0 || _tick - _tick_limited > 10) {
        _limited = l;
        _tick_limited = _tick;
      }
    }
    return _limited;
  }

 private:
  struct peer {
    uint64_t tick = 0;
    uint64_t in_memory_log_bytes = 0;
  };

  bool limited() {
    if (!enabled()) {
      return false;
    }
    uint64_t max_bytes = 0;
    bool gc = false;
    for (auto [id, p] : _peers) {
      if (_tick - p.tick > _gc_tick) {
        gc = true;
        continue;
      }
      max_bytes = std::max(max_bytes, p.in_memory_log_bytes);
    }
    max_bytes = std::max(max_bytes, _limiter.get());
    if (gc) {
      std::erase_if(
          _peers, [this](auto p) { return _tick - p.second.tick > _gc_tick; });
    }
    if (!_limited) {
      return max_bytes > _limiter.get_max();
    }
    return max_bytes >= _limiter.get_max() * 7 / 10;
  }

  uint64_t _tick = 1;
  uint64_t _tick_limited = 0;
  uint64_t _gc_tick = 3;
  bool _limited = false;
  std::unordered_map<uint64_t, peer> _peers;
  util::rate_limiter _limiter;
};

}  // namespace rafter::core
