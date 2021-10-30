//
// Created by jason on 2021/9/12.
//

#pragma once

#include <vector>

#include <seastar/core/iostream.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>

namespace rafter::util {

class fragmented_temporary_buffer {
  using fragment_vector = std::vector<seastar::temporary_buffer<char>>;

 public:
  class view;
  class istream;
  // TODO: redesign ostream to make fragmented_temporary_buffer appendable
  using ostream = seastar::memory_output_stream<fragment_vector::iterator>;

  static constexpr size_t default_fragment_size = 128 * 1024;

  fragmented_temporary_buffer(size_t bytes, size_t alignment);

  size_t bytes() const noexcept { return _bytes; }

  bool empty() const noexcept { return !_bytes; }

  void remove_prefix(size_t n) noexcept;

  void remove_suffix(size_t n) noexcept;

  istream as_istream() const noexcept;

  ostream as_ostream() noexcept;

  class iterator;

  iterator begin() const noexcept;

  iterator end() const noexcept;

 private:
  fragment_vector _fragments;
  size_t _bytes = 0;
};

class fragmented_temporary_buffer::iterator {
 public:
  iterator() = default;
  iterator(fragment_vector::const_iterator it, size_t left);

  const std::string_view &operator*() const noexcept { return _current; }

  const std::string_view *operator->() const noexcept { return &_current; }

  iterator &operator++() noexcept;

  iterator operator++(int) noexcept;

  bool operator==(const iterator &other) const noexcept {
    return _left == other._left;
  }

  bool operator!=(const iterator &other) const noexcept {
    return !(*this == other);
  }

 private:
  fragment_vector::const_iterator _it;
  std::string_view _current;
  size_t _left = 0;
};

class fragmented_temporary_buffer::istream {
 public:
  istream(fragment_vector::const_iterator it, size_t size) noexcept;

  size_t bytes_left() const noexcept;

  void skip(size_t n) noexcept;

  template<typename T>
  T read() {
    if (_curr_end - _curr_pos < sizeof(T)) [[unlikely]] {
      // TODO: check out of range
      T obj;
      size_t left = sizeof(T);
      while (left) {
        auto len = std::min(left, static_cast<size_t>(_curr_end - _curr_pos));
        std::copy_n(
            _curr_pos, len, reinterpret_cast<char*>(&obj) + sizeof(T) - left);
        left -= len;
        if (left) {
          next_fragment();
        } else {
          _curr_pos += len;
        }
      }
      return obj;
    }
    T obj;
    std::copy_n(_curr_pos, sizeof(T), reinterpret_cast<char*>(&obj));
    _curr_pos += sizeof(T);
    return obj;
  }

  view read(size_t n);
  std::string read_string(size_t n);

 private:
  void next_fragment();

 private:
  fragment_vector::const_iterator _current;
  const char* _curr_pos = nullptr;
  const char* _curr_end = nullptr;
  size_t _bytes_left = 0;
};

class fragmented_temporary_buffer::view {
 public:
  view() = default;
  view(fragment_vector::const_iterator it, size_t pos, size_t size);

  class iterator;

  iterator begin() const noexcept;

  iterator end() const noexcept;

  bool empty() const noexcept { return !_total_size; }
  size_t size() const noexcept { return _total_size; }

 private:
  fragment_vector::const_iterator _current;
  const char* _curr_pos = nullptr;
  size_t _curr_size = 0;
  size_t _total_size = 0;
};

class fragmented_temporary_buffer::view::iterator {
 public:
  iterator() = default;
  iterator(
      fragment_vector::const_iterator it,
      std::string_view current,
      size_t left) noexcept;

  const std::string_view& operator*() const noexcept { return _current; }

  const std::string_view* operator->() const noexcept { return &_current; }

  iterator& operator++() noexcept;

  iterator operator++(int) noexcept;

  bool operator==(const iterator& other) const noexcept {
    return _left == other._left;
  }

  bool operator!=(const iterator& other) const noexcept {
    return !(*this == other);
  }

 private:
  fragment_vector ::const_iterator _it;
  size_t _left = 0;
  std::string_view _current;
};

}  // namespace rafter::util
