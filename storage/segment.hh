//
// Created by jason on 2021/9/10.
//

#pragma once

#include <map>
#include <numeric>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/queue.hh>
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
  segment() = default;
  DEFAULT_MOVE_AND_ASSIGN(segment);

  static seastar::future<std::unique_ptr<segment>> open(
      uint64_t filename, std::string filepath, bool existing = false);

  uint64_t filename() const noexcept;
  uint64_t bytes() const noexcept;

  // return the file length after appending the update
  seastar::future<uint64_t> append(const protocol::update& update);
  seastar::future<protocol::update> query(index::entry i) const;
  // read segment and append saved entries to entries, return left bytes
  seastar::future<size_t> query(
      std::span<const index::entry> indexes,
      protocol::log_entry_vector& entries,
      size_t left_bytes) const;
  seastar::future<> sync();
  seastar::future<> close();
  seastar::future<> remove();
  seastar::future<> list_update(
      std::function<seastar::future<>(
          const protocol::update& up, index::entry e)> next) const;
  operator bool() const;

  static constexpr char SUFFIX[] = "log";
  static constexpr uint64_t INVALID_FILENAME = 0;
  // generate segment name, format {shard_id:05d}_{filename:020d}.log
  static std::string form_name(uint64_t filename);
  static std::string form_path(std::string_view dir, uint64_t filename);
  static std::string form_path(
      std::string_view dir, unsigned shard, uint64_t filename);
  // parse segment name, extract shard_id and filename
  static std::pair<unsigned, uint64_t> parse_name(std::string_view name);

  std::string debug_string() const;

 private:
  // TODO(jyc): scheduling group
  uint64_t _filename = 0;
  std::string _filepath;
  seastar::file _file;
  uint64_t _bytes = 0;
  uint64_t _aligned_pos = 0;
  std::string _tail;
};

}  // namespace rafter::storage
