//
// Created by jason on 2022/4/16.
//

#pragma once

namespace seastar {

template <typename T>
class shared_ptr;

template <typename T, typename... A>
shared_ptr<T> make_shared(A&&... a);

}  // namespace seastar

namespace rafter {

using namespace seastar;
using seastar::make_shared;
using seastar::shared_ptr;

template <typename T = void>
future<T> not_implemented() {
  throw_with_backtrace<std::runtime_error>("not implemented");
}

}  // namespace rafter
