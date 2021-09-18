//
// Created by jason on 2021/9/12.
//

#pragma once

#include <vector>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/align.hh>

namespace rafter::util
{

class fragmented_temporary_buffer {
  using fragment_vector = std::vector<seastar::temporary_buffer<char>>;
 public:
  using ostream = seastar::memory_output_stream<fragment_vector::iterator>;

  static constexpr size_t default_fragment_size = 128 * 1024;

  fragmented_temporary_buffer(size_t size, size_t alignment)
  {
    size = seastar::align_up(size, default_fragment_size);
    auto count = size / default_fragment_size;
    fragment_vector fragments;
    fragments.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      fragments.emplace_back(
        seastar::temporary_buffer<char>::aligned(alignment,
                                                 default_fragment_size));
    }
    fragments_ = std::move(fragments);
    bytes_ = size;
  }

  size_t size() const noexcept
  {
    return bytes_;
  }

  bool empty() const noexcept
  {
    return !bytes_;
  }

  void remove_prefix(size_t n) noexcept
  {
    bytes_ -= n;
    auto it = fragments_.begin();
    while (it->size() < n) {
      n -= it->size();
      ++it;
    }
    if (n) {
      it->trim_front(n);
    }
    fragments_.erase(fragments_.begin(), it);
  }

  void remove_suffix(size_t n) noexcept
  {
    bytes_ -= n;
    auto it = fragments_.rbegin();
    while (it->size() < n) {
      n -= it->size();
      ++it;
    }
    if (n) {
      it->trim(it->size() - n);
    }
    fragments_.erase(it.base(), fragments_.end());
  }

  ostream as_ostream() noexcept
  {
    if (fragments_.size() != 1) {
      return ostream::fragmented(fragments_.begin(), bytes_);
    }
    return ostream::simple(fragments_.begin()->get_write(),
                           fragments_.begin()->size());
  }

  class iterator {
   public:
    iterator() = default;
    iterator(fragment_vector::const_iterator it, size_t left)
      : it_(it), current_(it->get(), std::min(it->size(), left)), left_(left)
    {}

    const std::string_view &operator*() const noexcept
    {
      return current_;
    }

    const std::string_view *operator->() const noexcept
    {
      return &current_;
    }

    iterator &operator++() noexcept
    {
      left_ -= current_.size();
      if (left_) {
        ++it_;
        current_ = std::string_view(it_->get(), std::min(left_, it_->size()));
      }
      return *this;
    }

    iterator operator++(int) noexcept
    {
      auto it = *this;
      operator++();
      return it;
    }

    bool operator==(const iterator &other) const noexcept
    {
      return left_ == other.left_;
    }

    bool operator!=(const iterator &other) const noexcept
    {
      return !(*this == other);
    }

   private:
    fragment_vector::const_iterator it_;
    size_t left_ = 0;
    std::string_view current_;
  };

  iterator begin() const noexcept
  {
    if (!bytes_) {
      return iterator();
    }
    return iterator(fragments_.begin(), bytes_);
  }

  iterator end() const noexcept
  {
    return iterator();
  }

 private:
  fragment_vector fragments_;
  size_t bytes_ = 0;
};

}  // namespace rafter::util
