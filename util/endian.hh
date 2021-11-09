//
// Created by jason on 2021/9/21.
//

#pragma once

#include <byteswap.h>
#include <endian.h>
#include <string.h>

#include <concepts>

namespace rafter::util {

namespace endian::detail {

template <typename T>
concept numerical =
    std::integral<T> || std::floating_point<T> || std::is_enum_v<T>;

template <numerical T, unsigned N>
struct swapImpl;

template <numerical T>
struct swapImpl<T, 1> {
  static constexpr T swap(const T &t) { return t; }
};

template <numerical T>
struct swapImpl<T, 2> {
  static constexpr T swap(const T &t) { return bswap_16(t); }
};

template <numerical T>
struct swapImpl<T, 4> {
  static constexpr T swap(const T &t) { return bswap_32(t); }
};

template <numerical T>
struct swapImpl<T, 8> {
  static constexpr T swap(const T &t) { return bswap_64(t); }
};
}  // namespace endian::detail

template <endian::detail::numerical T>
void write_le(T t, void *des) {
  T v = t;
  if constexpr (__BYTE_ORDER != __LITTLE_ENDIAN) {
    v = endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  ::memcpy(des, &v, sizeof(T));
}

template <endian::detail::numerical T>
constexpr T read_le(const void *src) {
  T t;
  ::memcpy(&t, src, sizeof(T));
  if constexpr (__BYTE_ORDER != __LITTLE_ENDIAN) {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template <endian::detail::numerical T>
void write_be(T t, void *des) {
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN) {
    t = endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  ::memcpy(des, &t, sizeof(T));
}

template <endian::detail::numerical T>
constexpr T read_be(const void *src) {
  T t;
  ::memcpy(&t, src, sizeof(T));
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN) {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template <endian::detail::numerical T>
inline constexpr T hton(T t) {
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN) {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template <endian::detail::numerical T>
inline constexpr T ntoh(T t) {
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN) {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template <endian::detail::numerical T>
inline constexpr T htole(T t) {
  if constexpr (__BYTE_ORDER != __LITTLE_ENDIAN) {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template <endian::detail::numerical T>
inline constexpr T letoh(T t) {
  if constexpr (__BYTE_ORDER != __LITTLE_ENDIAN) {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template <endian::detail::numerical T>
inline constexpr T htobe(T t) {
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN) {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template <endian::detail::numerical T>
inline constexpr T betoh(T t) {
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN) {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

}  // namespace rafter::util
