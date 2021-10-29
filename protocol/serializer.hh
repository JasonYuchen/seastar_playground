//
// Created by jason on 2021/10/27.
//

#pragma once

#include <seastar/core/future.hh>

#include "protocol/raft.hh"

namespace rafter::protocol {

template<typename T>
seastar::future<uint64_t> serialize(const struct update& up, T& out) {
  // TODO
}
template<typename U, typename T>
static seastar::future<std::pair<U, uint64_t>> deserialize(T& in) {
  // TODO
}

}  // namespace rafter::protocol
