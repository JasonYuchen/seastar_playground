//
// Created by jason on 2021/10/8.
//

#pragma once

#include <exception>
#include <fmt/format.h>
#include <string_view>

#include <seastar/util/log.hh>

namespace rafter::util {

enum class code : uint8_t {
  ok = 0,
  short_read,
  short_write,
  compacted,
  unavailable,
  out_of_range,
  closed,
  timed_out,
  cancelled,
  corruption,
  failed_precondition,
  unknown,
  invalid,
  no_data,
  num_of_codes,
};

const char* status_string(enum code e);

class base_error : public std::runtime_error {
 public:
  explicit base_error(enum code e)
      : std::runtime_error(status_string(e)), _e(e) {}
  template<typename S, typename... Args>
  base_error(enum code e, const S& pattern, Args&&... args)
      : std::runtime_error(fmt::format(pattern, std::forward<Args>(args)...))
      , _e(e) {}
  template<typename S, typename... Args>
  base_error(seastar::logger& l, enum code e, const S& pattern, Args&&... args)
      : std::runtime_error(status_string(e)), _e(e) {
    l.error(pattern, std::forward<Args>(args)...);
  }

  code error_code() const noexcept {
    return _e;
  }

 protected:
  enum code _e;
};

class runtime_error : public base_error {
 public:
  using base_error::base_error;
};

class io_error : public base_error {
 public:
  using base_error::base_error;
};

class logic_error : public base_error {
 public:
  using base_error::base_error;
};

}  // namespace rafter::util
