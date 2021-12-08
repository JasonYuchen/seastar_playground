//
// Created by jason on 2021/9/10.
//

#pragma once

#include <seastar/core/queue.hh>

#include <filesystem>
#include <numeric>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>
#include <span>
#include <utility>
#include <vector>

#include "protocol/raft.hh"
#include "storage/index.hh"
#include "util/fragmented_temporary_buffer.hh"
#include "util/types.hh"

namespace rafter::storage {

class segment {
 public:
  // TODO: use make_lw_shared and 2 phase construction
  // TODO: use lock to protect append
  segment() = default;
  DEFAULT_MOVE_AND_ASSIGN(segment);

  static seastar::future<std::unique_ptr<segment>> open(
      uint64_t filename, std::string filepath, bool existing = false);

  uint64_t bytes() const noexcept;

  // return the file length after appending the update
  seastar::future<uint64_t> append(const protocol::update& update);
  seastar::future<> query(
      index::entry i,
      std::function<seastar::future<>(const protocol::update& up)> f) const;
  // read segment and append saved entries to entries
  seastar::future<> query(
      std::span<const index::entry> indexes,
      protocol::log_entry_vector& entries,
      size_t& left_bytes) const;
  seastar::future<> sync();
  seastar::future<> close();
  seastar::future<> list_update(
      std::function<seastar::future<>(
          const protocol::update& up, index::entry e)> next) const;
  operator bool() const;

  std::string debug() const;

 private:
  // TODO: scheduling group
  uint64_t _filename = 0;
  std::string _filepath;
  seastar::file _file;
  uint64_t _bytes = 0;
  uint64_t _aligned_pos = 0;
  std::string _tail;
};

}  // namespace rafter::storage
