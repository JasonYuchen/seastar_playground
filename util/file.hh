//
// Created by jason on 2022/5/8.
//

#pragma once

#include <seastar/core/future.hh>

#include "util/seastarx.hh"

namespace rafter::util {

// dir/name should be valid util returned future is completed

future<> create_file(
    std::string_view dir, std::string_view name, std::string msg);

future<temporary_buffer<char>> read_file(
    std::string_view dir, std::string_view name);

future<bool> exist_file(std::string_view dir, std::string_view name);

future<> remove_file(std::string_view dir, std::string_view name);

}  // namespace rafter::util
