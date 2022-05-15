//
// Created by jason on 2021/10/14.
//

#pragma once

#include <charconv>
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

struct tri_hasher {
  template <typename T>
  std::size_t operator()(T&& tri) const noexcept {
    auto&& [one, two, thr] = tri;
    return std::hash<std::remove_cvref_t<decltype(one)>>()(one) ^
           std::hash<std::remove_cvref_t<decltype(two)>>()(two) ^
           std::hash<std::remove_cvref_t<decltype(thr)>>()(thr);
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

template <typename I>
  requires(std::is_integral_v<I>)
uint8_t parse_file_name(std::string_view name, char, I& i0) {
  auto r = std::from_chars(name.begin(), name.end(), i0);
  if (r.ec != std::errc{} || r.ptr != name.end()) {
    return 0;
  }
  return 1;
}

template <typename I, typename... Is>
  requires(std::is_integral_v<I>)
uint8_t parse_file_name(std::string_view name, char delim, I& i0, Is&... i) {
  auto pos = name.find(delim);
  if (pos == std::string_view::npos) {
    return 0;
  }
  auto r = std::from_chars(name.begin(), name.begin() + pos, i0);
  if (r.ec != std::errc{} || r.ptr != name.begin() + pos) {
    return 0;
  }
  return 1 + parse_file_name(name.substr(pos + 1), delim, i...);
}

}  // namespace rafter::util
