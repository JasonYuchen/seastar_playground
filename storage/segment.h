//
// Created by jason on 2021/9/10.
//

#ifndef SEASTAR_PLAYGROUND_STORAGE_SEGMENT_H_
#define SEASTAR_PLAYGROUND_STORAGE_SEGMENT_H_

#include <vector>
#include <utility>
#include <stdint.h>
#include <assert.h>
#include <map>
#include <new>
#include <seastar/core/future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>
#include <filesystem>
#include <span>

#include <endian.h>
#include <byteswap.h>
#include <concepts>

namespace endian::detail
{

template<typename T>
concept numerical = std::integral<T> || std::floating_point<T>;

template<numerical T, unsigned N>
struct swapImpl;

template<numerical T>
struct swapImpl<T, 1> {
  static constexpr T swap(const T &t)
  {
    return t;
  }
};

template<numerical T>
struct swapImpl<T, 2> {
  static constexpr T swap(const T &t)
  {
    return bswap_16(t);
  }
};

template<numerical T>
struct swapImpl<T, 4> {
  static constexpr T swap(const T &t)
  {
    return bswap_32(t);
  }
};

template<numerical T>
struct swapImpl<T, 8> {
  static constexpr T swap(const T &t)
  {
    return bswap_64(t);
  }
};

} // endian::detail

template<endian::detail::numerical T>
void write_le(T t, void *des) {
  T v = t;
  if constexpr (__BYTE_ORDER != __LITTLE_ENDIAN)
  {
    v = endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  ::memcpy(des, &v, sizeof(T));
}

template<endian::detail::numerical T>
constexpr T read_le(const void *src) {
  T t;
  ::memcpy(&t, src, sizeof(T));
  if constexpr (__BYTE_ORDER != __LITTLE_ENDIAN)
  {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template<endian::detail::numerical T>
void write_be(T t, void *des) {
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN)
  {
    t = endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  ::memcpy(des, &t, sizeof(T));
}

template<endian::detail::numerical T>
constexpr T read_be(const void *src) {
  T t;
  ::memcpy(&t, src, sizeof(T));
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN)
  {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template<endian::detail::numerical T>
inline constexpr T hton(T t) {
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN)
  {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

template<endian::detail::numerical T>
inline constexpr T ntoh(T t) {
  if constexpr (__BYTE_ORDER == __LITTLE_ENDIAN)
  {
    return endian::detail::swapImpl<T, sizeof(T)>::swap(t);
  }
  return t;
}

struct group_id {
  uint64_t cluster;
  uint64_t node;
};

struct log_id {
  uint64_t term;
  uint64_t index;
  std::strong_ordering operator<=>(const log_id&) const = default;
};

struct log_locator {
  log_id id;
  uint64_t offset;
};

class segment {
 public:
  segment() {
    // TODO
  }
  class index {
   public:
    bool contains(uint64_t index) const noexcept {
      return index >= start_index_ && index < start_index_ + locators_.size();
    }
    uint64_t term(uint64_t index) const noexcept {
      assert(contains(index));
      return locators_[index - start_index_].id.term;
    }
    seastar::future<> serialize(seastar::output_stream<char>& out) const {
      // TODO
      co_return;
    }
    seastar::future<> deserialize() const {
      // TODO
      co_return;
    }
   private:
    uint64_t start_index_; // == locators_.front().id.index;
    std::vector<log_locator> locators_;
  };
  segment() {
    file_ = seastar::open_file_dma(path_.string(), seastar::open_flags::create | seastar::open_flags::dsync | seastar::open_flags::rw);
    out_ = seastar::make_file_output_stream(file_);
    header_.data = seastar::temporary_buffer<char>::aligned(file_.memory_dma_alignment(), header::HEADER_SIZE);
    header_.set_term(0);
    header_.set_vote(0);
    header_.fill_checksum(0);
    file_.dma_write(0, header_.data.share());
    co_await out_.write(header_.data.share());
    co_await out_.flush();
  }

  future<> append(std::span<int> entries) {

  }

 private:
  struct header {
    static constexpr uint64_t HEADER_SIZE = 4096;
    static constexpr uint64_t TERM = 0;
    static constexpr uint64_t VOTE = 8;
    static constexpr uint64_t COMMIT = 16;
    static constexpr uint64_t START_INDEX = 24;
    static constexpr uint64_t END_INDEX = 32;
    static constexpr uint64_t CHECKSUM_TYPE = 36;
    static constexpr uint64_t CHECKSUM = 40;
    header() = default;
    uint64_t term() const noexcept {
      return read_le<uint64_t>(data.get() + TERM);
    }
    header& set_term(uint64_t term) noexcept {
      write_le(term, data.get_write() + TERM);
      return *this;
    }
    uint64_t vote() const noexcept {
      return read_le<uint64_t>(data.get() + VOTE)
    }
    header& set_vote(uint64_t vote) noexcept {
      write_le(vote, data.get_write() + VOTE);
      return *this;
    }

    header& fill_checksum(uint32_t type) {
      write_le(type, data.get_write() + CHECKSUM_TYPE);
      uint32_t checksum = 0; // calculate checksum
      write_le(checksum, data.get_write() + CHECKSUM);
      return *this;
    }

    seastar::temporary_buffer<char> data;
  };

  const group_id group_id_;
  std::filesystem::path path_;
  seastar::file file_;
  seastar::output_stream<char> out_;
  index index_;
  const uint64_t rolling_size_; // in MB
  header header_;
};

class segment_manager {
 public:
 private:
  // start entry -> segment
  std::map<log_id, std::unique_ptr<segment>> segments_;
};

#endif //SEASTAR_PLAYGROUND_STORAGE_SEGMENT_H_
