//
// Created by jason on 2021/10/14.
//

#pragma once

#include <functional>
#include <sstream>
#include <type_traits>
#include <utility>

namespace rafter::util {

struct pair_hasher {
  template <typename T>
  std::size_t operator()(T&& pair) const noexcept {
    auto&& [one, two] = pair;
    return std::hash<std::remove_cvref_t<decltype(one)>>()(one) ^
           std::hash<std::remove_cvref_t<decltype(two)>>()(two);
  }
};

template <typename T>
std::string print(T items) {
  std::stringstream ss;
  for (const auto& item : items) {
    ss << item.debug_string() << std::endl;
  }
  return ss.str();
}

}  // namespace rafter::util
