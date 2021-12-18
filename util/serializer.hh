//
// Created by jason on 2021/12/17.
//

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdint.h>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/simple-stream.hh>

#include "util/endian.hh"
#include "util/types.hh"

namespace rafter::util {

template<typename T>
struct serializer;

template<typename T, typename Output>
inline void serialize(Output& o, const T& v) {
  serializer<T>::write(o, v);
}

template<typename T, typename Input>
inline T deserialize(Input& i, type<T>) {
  return serializer<T>::read(i);
}

template<typename T, typename Input>
inline void skip(Input& i, type<T>) {
  serializer<T>::skip(i);
}

namespace detail {

template<typename T>
requires endian::detail::numerical<T>
struct numeric_serializer {
  template<typename Input>
  static T read(Input& i) {
    T data;
    i.read(reinterpret_cast<char*>(&data), sizeof(T));
    return letoh(data);
  }

  template<typename Output>
  static void write(Output& o, T v) {
    v = htole(v);
    o.write(reinterpret_cast<const char*>(&v), sizeof(T));
  }
};

struct string_serializer {
  template<typename Input>
  static std::string read(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    std::string s;
    s.resize(size);
    i.read(s.data(), size);
    return s;
  }
  template<typename Output>
  static void write(Output& o, const std::string& s) {
    serialize(o, s.size());
    o.write(s.data(), s.size());
  }
  template<typename Input>
  static void skip(Input& i) {
    i.skip(deserialize(i, type<uint64_t>()));
  }
};

template<typename T>
struct lw_shared_ptr_serializer {
  template<typename Input>
  static seastar::lw_shared_ptr<T> read(Input& i) {
    if (deserialize(i, type<bool>())) {
      return seastar::make_lw_shared<T>(deserialize(i, type<T>()));
    }
    return {};
  }
  template<typename Output>
  static void write(Output& o, const seastar::lw_shared_ptr<T>& p) {
    serialize(o, p.operator bool());
    if (p) {
      serialize(o, *p);
    }
  }
  template<typename Input>
  static void skip(Input& i) {
    if (deserialize(i, type<bool>())) {
      serializer<T>::skip(i);
    }
  }
};

template<typename T>
struct unique_ptr_serializer {
  template<typename Input>
  static std::unique_ptr<T> read(Input& i) {
    if (deserialize(i, type<bool>())) {
      return std::make_unique<T>(deserialize(i, type<T>()));
    }
    return {};
  }
  template<typename Output>
  static void write(Output& o, const std::unique_ptr<T>& p) {
    serialize(o, p.operator bool());
    if (p) {
      serialize(o, *p);
    }
  }
  template<typename Input>
  static void skip(Input& i) {
    if (deserialize(i, type<bool>())) {
      serializer<T>::skip(i);
    }
  }
};

template<typename T>
struct optional_serializer {
  template<typename Input>
  static std::optional<T> read(Input& i) {
    std::optional<T> v;
    if (deserialize(i, type<bool>())) {
      v.emplace(deserialize(i, type<T>()));
    }
    return v;
  }
  template<typename Output>
  static void write(Output& o, const std::optional<T>& p) {
    serialize(o, p.operator bool());
    if (p) {
      serialize(o, *p);
    }
  }
  template<typename Input>
  static void skip(Input& i) {
    if (deserialize(i, type<bool>())) {
      serializer<T>::skip(i);
    }
  }
};

template<typename T>
constexpr bool fast_serialize() {
  // special handling for std::vector<bool>
  return !std::is_same_v<T, bool> && std::is_integral_v<T> &&
      (sizeof(T) == 1 || __BYTE_ORDER == __LITTLE_ENDIAN);
}

template<typename Container>
struct container_traits;

template<typename T>
struct container_traits<std::vector<T>> {
  struct inserter {
    std::vector<T>& c;
    inserter(std::vector<T>& c) : c(c) {}
    void operator()(T&& v) {
      c.emplace_back(std::move(v));
    }
  };
};

template<typename T>
struct container_traits<std::set<T>> {
  struct inserter {
    std::set<T>& c;
    inserter(std::set<T>& c) : c(c) {}
    void operator()(T&& v) {
      c.emplace(std::move(v));
    }
  };
};

template<typename T>
struct container_traits<std::unordered_set<T>> {
  struct inserter {
    std::unordered_set<T>& c;
    inserter(std::unordered_set<T>& c) : c(c) {}
    void operator()(T&& v) {
      c.emplace(std::move(v));
    }
  };
};

template<typename T, typename Output, typename Container>
static inline void serialize_container(Output& o, const Container& c) {
  for (const auto& v : c) {
    serialize(o, v);
  }
}

template<typename T, typename Input, typename Container>
static inline void deserialize_container(
    Input& i, Container& c, uint64_t size) {
  typename container_traits<Container>::inserter f(c);
  while (size--) {
    f(deserialize(i, type<T>()));
  }
}

template<typename T, typename Input>
static inline void skip_container(Input& i, uint64_t size) {
  while (size--) {
    serializer<T>::skip(i);
  }
}

template<typename T>
struct vector_serializer {
  template<typename Input>
  static std::vector<T> read(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    std::vector<T> c;
    c.reserve(size);
    if constexpr (fast_serialize<T>()) {
      c.resize(size);
      i.read(reinterpret_cast<char*>(c.data()), size * sizeof(T));
    } else {
      deserialize_container<T>(i, c, size);
    }
    return c;
  }
  template<typename Output>
  static void write(Output& o, const std::vector<T>& c) {
    serialize(o, c.size());
    if constexpr (fast_serialize<T>()) {
      o.write(reinterpret_cast<const char*>(c.data()), c.size() * sizeof(T));
    } else {
      serialize_container<T>(o, c);
    }
  }
  template<typename Input>
  static void skip(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    if constexpr (fast_serialize<T>()) {
      i.skip(size * sizeof(T));
    } else {
      skip_container<T>(i, size);
    }
  }
};

template<typename K, typename V>
struct map_serializer {
  template<typename Input>
  static std::map<K, V> read(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    std::map<K, V> m;
    while (size--) {
      K k = deserialize(i, type<K>());
      V v = deserialize(i, type<K>());
      m.emplace(std::move(k), std::move(v));
    }
    return m;
  }
  template<typename Output>
  static void write(Output& o, const std::map<K, V>& m) {
    serialize(o, m.size());
    for (const auto& [k, v] : m) {
      serialize(o, k);
      serialize(o, v);
    }
  }
  template<typename Input>
  static void skip(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    while (size--) {
      serializer<K>::skip(i);
      serializer<V>::skip(i);
    }
  }
};

template<typename K, typename V>
struct unordered_map_serializer {
  template<typename Input>
  static std::unordered_map<K, V> read(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    std::unordered_map<K, V> m;
    m.reserve(size);
    while (size--) {
      K k = deserialize(i, type<K>());
      V v = deserialize(i, type<V>());
      m.emplace(std::move(k), std::move(v));
    }
    return m;
  }
  template<typename Output>
  static void write(Output& o, const std::unordered_map<K, V>& m) {
    serialize(o, m.size());
    for (const auto& [k, v] : m) {
      serialize(o, k);
      serialize(o, v);
    }
  }
  template<typename Input>
  static void skip(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    while (size--) {
      serializer<K>::skip(i);
      serializer<V>::skip(i);
    }
  }
};

template<typename T>
struct set_serializer {
  template<typename Input>
  static std::set<T> read(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    std::set<T> s;
    deserialize_container<T>(i, s, size);
    return s;
  }
  template<typename Output>
  static void write(Output& o, const std::set<T>& s) {
    serialize(o, s.size());
    serialize_container<T>(o, s);
  }
  template<typename Input>
  static void skip(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    skip_container<T>(i, size);
  }
};

template<typename T>
struct unordered_set_serializer {
  template<typename Input>
  static std::unordered_set<T> read(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    std::unordered_set<T> s;
    s.reserve(size);
    deserialize_container<T>(i, s, size);
    return s;
  }
  template<typename Output>
  static void write(Output& o, const std::unordered_set<T>& s) {
    serialize(o, s.size());
    serialize_container<T>(o, s);
  }
  template<typename Input>
  static void skip(Input& i) {
    auto size = deserialize(i, type<uint64_t>());
    skip_container<T>(i, size);
  }
};

}  // namespace detail

template<typename Output>
class measuring_output_stream_adapter {
 public:
  explicit measuring_output_stream_adapter(Output& output) : _output(output) {}
  void write(const char* data, size_t size) {
    _output.write(data, size);
    _size += size;
  }
  size_t size() const {
    return _size;
  }
 private:
  Output& _output;
  size_t _size = 0;
};

template<> struct serializer<bool>
    : public detail::numeric_serializer<bool> {};
template<> struct serializer<int8_t>
    : public detail::numeric_serializer<int8_t> {};
template<> struct serializer<uint8_t>
    : public detail::numeric_serializer<uint8_t> {};
template<> struct serializer<int16_t>
    : public detail::numeric_serializer<int16_t> {};
template<> struct serializer<uint16_t>
    : public detail::numeric_serializer<uint16_t> {};
template<> struct serializer<int32_t>
    : public detail::numeric_serializer<int32_t> {};
template<> struct serializer<uint32_t>
    : public detail::numeric_serializer<uint32_t> {};
template<> struct serializer<int64_t>
    : public detail::numeric_serializer<int64_t> {};
template<> struct serializer<uint64_t>
    : public detail::numeric_serializer<uint64_t> {};
template<> struct serializer<float>
    : public detail::numeric_serializer<float> {};
template<> struct serializer<double>
    : public detail::numeric_serializer<double> {};
template<> struct serializer<std::string>
    : public detail::string_serializer {};

template<typename T> struct serializer<seastar::lw_shared_ptr<T>>
    : public detail::lw_shared_ptr_serializer<T> {};
template<typename T> struct serializer<std::unique_ptr<T>>
    : public detail::unique_ptr_serializer<T> {};
template<typename T> struct serializer<std::vector<T>>
    : public detail::vector_serializer<T> {};
template<typename T> struct serializer<std::set<T>>
    : public detail::set_serializer<T> {};
template<typename T> struct serializer<std::unordered_set<T>>
    : public detail::unordered_set_serializer<T> {};
template<typename K, typename V> struct serializer<std::map<K, V>>
    : public detail::map_serializer<K, V> {};
template<typename K, typename V> struct serializer<std::unordered_map<K, V>>
    : public detail::unordered_map_serializer<K, V> {};

}  // namespace rafter::util
