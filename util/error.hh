//
// Created by jason on 2021/10/8.
//

#pragma once

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <exception>
#include <string_view>

#include "protocol/raft.hh"

namespace rafter::util {

enum class code : uint8_t {
  ok = 0,
  configuration,
  serialization,
  short_read,
  short_write,
  peer_not_found,
  compacted,
  unavailable,
  out_of_range,
  closed,
  timed_out,
  cancelled,
  corruption,
  failed_precondition,
  failed_postcondition,
  unknown,
  invalid,
  no_data,
  num_of_codes,
};

const char* status_string(enum code e);

class base_error : public std::exception {
 public:
  explicit base_error(enum code e) : _e(e) {}
  base_error(enum code e, std::string msg) : _e(e), _msg(std::move(msg)) {}
  template <typename... Args>
  base_error(const char* s, enum code e, Args&&... args)
    : _e(e)
    , _msg(fmt::format(s, status_string(e), std::forward<Args>(args)...)) {}

  code error_code() const noexcept { return _e; }

  const char* what() const noexcept override { return _msg.c_str(); }

 protected:
  enum code _e;
  std::string _msg;
};

class configuration_error : public base_error {
 public:
  using base_error::base_error;
  configuration_error(std::string_view key, std::string_view msg)
    : base_error("{}: key:{}, reason:{}", code::configuration, key, msg) {}
};

class serialization_error : public base_error {
 public:
  using base_error::base_error;
  serialization_error() : base_error(code::serialization) {}
  serialization_error(std::string_view type)
    : base_error("{}: failed type:{}", code::serialization, type) {}
};

class io_error : public base_error {
 public:
  using base_error::base_error;
};

class short_read_error : public io_error {
 public:
  using io_error::io_error;
  short_read_error() : io_error(code::short_read) {}
  short_read_error(std::string_view msg)
    : io_error("{}: {}", code::short_read, msg) {}
};

class short_write_error : public io_error {
 public:
  using io_error::io_error;
  short_write_error() : io_error(code::short_write) {}
  short_write_error(std::string_view msg)
    : io_error("{}: {}", code::short_write, msg) {}
};

class peer_not_found_error : public io_error {
 public:
  using io_error::io_error;
  peer_not_found_error() : io_error(code::peer_not_found) {}
  peer_not_found_error(protocol::group_id gid)
    : io_error("{}: cannot resolve {}", code::peer_not_found, gid) {}
};

class corruption_error : public io_error {
 public:
  using io_error::io_error;
  corruption_error() : io_error(code::corruption) {}
  corruption_error(std::string_view msg)
    : io_error("{}: {}", code::corruption, msg) {}
};

class raft_error : public base_error {
 public:
  using base_error::base_error;
};

// log entry compacted, e.g. ask for index=13 but only have [15, 20]
class compacted_error : public raft_error {
 public:
  using raft_error::raft_error;
  compacted_error() : raft_error(code::compacted) {}
  compacted_error(uint64_t index, uint64_t first_index)
    : raft_error("{}: {} < {}", code::compacted, index, first_index) {}
};

// log entry unavailable, e.g. ask for index=13 but only have [5, 10]
class unavailable_error : public raft_error {
 public:
  using raft_error::raft_error;
  unavailable_error() : raft_error(code::unavailable) {}
  unavailable_error(uint64_t index, uint64_t last_index)
    : raft_error("{}: {} > {}", code::unavailable, index, last_index) {}
};

class closed_error : public base_error {
 public:
  using base_error::base_error;
  closed_error() : base_error(code::closed) {}
  closed_error(std::string_view service)
    : base_error("{}: service {} closed", code::closed, service) {}
};

class logic_error : public base_error {
 public:
  using base_error::base_error;
};

class out_of_range_error : public logic_error {
 public:
  using logic_error::logic_error;
  out_of_range_error() : logic_error(code::out_of_range) {}
  out_of_range_error(std::string_view msg)
    : logic_error("{}: {}", code::out_of_range, msg) {}
};

class failed_precondition_error : public logic_error {
 public:
  using logic_error::logic_error;
  failed_precondition_error() : logic_error(code::failed_precondition) {}
  failed_precondition_error(std::string_view msg)
    : logic_error("{}: {}", code::failed_precondition, msg) {}
};

class failed_postcondition_error : public logic_error {
 public:
  using logic_error::logic_error;
  failed_postcondition_error() : logic_error(code::failed_postcondition) {}
  failed_postcondition_error(std::string_view msg)
    : logic_error("{}: {}", code::failed_postcondition, msg) {}
};

}  // namespace rafter::util
