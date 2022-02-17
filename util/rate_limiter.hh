//
// Created by jason on 2022/1/22.
//

#pragma once

#include <stdint.h>

namespace rafter::util {

class rate_limiter {
 public:
  explicit rate_limiter(uint64_t max_size) : _size(0), _max_size(max_size) {}
  bool enabled() const noexcept {
    return _max_size > 0 && _max_size != UINT64_MAX;
  }
  void increase(uint64_t quota) noexcept { _size += quota; }
  void decrease(uint64_t quota) noexcept { _size -= quota; }
  void set(uint64_t size) noexcept { _size = size; }
  uint64_t get() const noexcept { return _size; }
  bool rate_limited() const noexcept {
    if (!enabled()) {
      return false;
    }
    return _size > _max_size;
  }
  uint64_t get_max() const noexcept { return _max_size; }

 private:
  uint64_t _size;
  uint64_t _max_size;
};

}  // namespace rafter::util
