//
// Created by jason on 2021/9/12.
//

#pragma once

#include <list>

#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "util/endian.hh"

namespace rafter::util {

// fragmented_temporary_buffer is not designed as a producer-consumer queue
//
// common use case 1: buffer data in memory and write to other module as a batch
//
// fragmented_temporary_buffer buffer(estimated_total_size);
// buffer.as_ostream().write(some_data);
// buffer.remove_suffix(buffer.bytes() - some_data.size()); // trim empty tail
// for (auto buf : buffer) {
//   other_system.use(buf);
// }
//
// common use case 2: read from stream and prepare for deserialization
//
// auto buffer = fragmented_temporary_buffer::from_stream_exactly();
// some_structure.deserialize(buffer.as_istream());

// TODO: add tests for fragmented_temporary_buffer

class fragmented_temporary_buffer {
 public:
  using fragment_list = std::list<seastar::temporary_buffer<char>>;

  class iterator;
  class istream;
  class ostream;
  class view;

  static constexpr size_t default_fragment_size = 128 * 1024;
  static constexpr size_t default_fragment_alignment = 4096;

  fragmented_temporary_buffer() = default;

  explicit fragmented_temporary_buffer(
      size_t bytes,
      size_t alignment = default_fragment_alignment,
      size_t fragment_size = default_fragment_size);

  fragmented_temporary_buffer(fragment_list fragments, size_t bytes);

  static seastar::future<fragmented_temporary_buffer> from_stream_exactly(
      seastar::input_stream<char>& in, size_t size);
  static seastar::future<fragmented_temporary_buffer> from_stream_up_to(
      seastar::input_stream<char>& in, size_t size);

  size_t bytes() const noexcept { return _bytes; }

  bool empty() const noexcept { return !_bytes; }

  void remove_prefix(size_t n) noexcept;

  void remove_suffix(size_t n) noexcept;

  // read from current buffer
  istream as_istream() const noexcept;

  // write to current buffer (will overwrite all existing content and add new
  // fragments in need)
  // do not use as_ostream after remove_prefix/remove_suffix
  ostream as_ostream() noexcept;

  iterator begin() const noexcept;

  iterator end() const noexcept;

  char* get_write(size_t index = 0);

  void add_fragment(size_t fragment_size = default_fragment_size);

 private:
  fragment_list _fragments;
  size_t _bytes = 0;
  size_t _alignment = default_fragment_alignment;
  size_t _fragment_size = default_fragment_size;
};

class fragmented_temporary_buffer::iterator {
 public:
  iterator() = default;
  iterator(fragment_list::const_iterator it, size_t left);

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
  fragment_list::const_iterator _it;
  std::string_view _current;
  size_t _left = 0;
};

class fragmented_temporary_buffer::istream {
 public:
  istream(fragment_list::const_iterator it, size_t size) noexcept;

  size_t bytes_left() const noexcept;

  void skip(size_t n) noexcept;

  template<endian::detail::numerical T>
  T read() {
    if (_curr_end - _curr_pos < sizeof(T)) [[unlikely]] {
      check_range(sizeof(T));
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

  template<endian::detail::numerical T>
  T read_le() {
    return letoh(read<T>());
  }

  template<endian::detail::numerical T>
  T read_be() {
    return betoh(read<T>());
  }

  void read(char* data, size_t n);

  view read(size_t n);
  std::string read_string(size_t n);

 private:
  void next_fragment();
  void check_range(size_t size);

 private:
  fragment_list::const_iterator _current;
  const char* _curr_pos = nullptr;
  const char* _curr_end = nullptr;
  size_t _bytes_left = 0;
};

class fragmented_temporary_buffer::ostream {
 public:
  explicit ostream(
      fragmented_temporary_buffer& buffer,
      fragment_list::iterator it,
      size_t size) noexcept;

  template<endian::detail::numerical T>
  void write(T data) {
    write(reinterpret_cast<const char*>(&data), sizeof(T));
  }

  template<endian::detail::numerical T>
  void write_le(T data) {
    write(htole(data));
  }

  template<endian::detail::numerical T>
  void write_be(T data) {
    write(htobe(data));
  }

  void write(std::string_view data) {
    write(data.data(), data.size());
  }

  void write(const char* data, size_t size);

  void fill(char c, size_t size);

  void remove_suffix_to_fit() noexcept;

 private:
  void next_fragment();

 private:
  fragmented_temporary_buffer& _buffer;
  fragment_list::iterator _current;
  char* _curr_pos = nullptr;
  char* _curr_end = nullptr;
  size_t _bytes_left = 0;
};

class fragmented_temporary_buffer::view {
 public:
  view() = default;
  view(fragment_list::const_iterator it, size_t pos, size_t size);
  view(std::string_view s) noexcept;
  view(const std::string& s) noexcept;

  bool operator==(const view& rhs) const noexcept;
  bool operator!=(const view& rhs) const noexcept;

  class iterator;

  iterator begin() const noexcept;

  iterator end() const noexcept;

  bool empty() const noexcept { return !_total_size; }
  size_t size() const noexcept { return _total_size; }

 private:
  fragment_list::const_iterator _current;
  const char* _curr_pos = nullptr;
  size_t _curr_size = 0;
  size_t _total_size = 0;
};

class fragmented_temporary_buffer::view::iterator {
 public:
  iterator() = default;
  iterator(
      fragment_list::const_iterator it,
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
  fragment_list::const_iterator _it;
  size_t _left = 0;
  std::string_view _current;
};

}  // namespace rafter::util
