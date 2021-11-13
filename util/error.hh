//
// Created by jason on 2021/10/8.
//

#pragma once

#include <exception>
#include <fmt/format.h>
#include <string_view>

namespace rafter::util {

enum class error {
  ok,
  short_read,
  short_write,
};

const char* error_string(enum class error e);

class io_error : std::runtime_error {
 public:
  io_error(const char* msg) : std::runtime_error(msg) {}
};

class out_of_range : std::logic_error {
 public:
  out_of_range(const char* msg);
};

}  // namespace rafter::util
