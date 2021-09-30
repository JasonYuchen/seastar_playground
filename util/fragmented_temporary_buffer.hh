//
// Created by jason on 2021/9/12.
//

#pragma once

#include <vector>

#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>

namespace rafter::util {

class fragmented_temporary_buffer {
  using fragment_vector = std::vector<seastar::temporary_buffer<char>>;

 public:
  using ostream = seastar::memory_output_stream<fragment_vector::iterator>;

  static constexpr size_t default_fragment_size = 128 * 1024;

  fragmented_temporary_buffer(size_t bytes, size_t alignment);

  size_t bytes() const noexcept { return _bytes; }

  bool empty() const noexcept { return !_bytes; }

  void remove_prefix(size_t n) noexcept;

  void remove_suffix(size_t n) noexcept;

  ostream as_ostream() noexcept;

  class iterator {
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

  iterator begin() const noexcept {
    return _bytes ? iterator{_fragments.begin(), _bytes} : iterator{};
  }

  iterator end() const noexcept { return {}; }

 private:
  fragment_vector _fragments;
  size_t _bytes = 0;
};

}  // namespace rafter::util
