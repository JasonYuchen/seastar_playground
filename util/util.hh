//
// Created by jason on 2021/10/14.
//

#pragma once

#include <functional>
#include <utility>

namespace rafter::util {

struct pair_hasher {
  template<typename T>
  std::size_t operator()(T&& pair) const {
    auto&& [one, two] = pair;
    return std::hash<decltype(one)>{}(one) ^ std::hash<decltype(two)>{}(two);
  }
};

}  // namespace rafter::util
