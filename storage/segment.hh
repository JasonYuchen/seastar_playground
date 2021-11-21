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
  segment() = default;
  DEFAULT_MOVE_AND_ASSIGN(segment);

  static seastar::future<std::unique_ptr<segment>> open(
      std::string filepath, bool existing = false);

  uint64_t bytes() const noexcept;

  // return the file length after appending the update
  seastar::future<uint64_t> append(const protocol::update& update);
  // TODO(jason): add batch append method
  //seastar::future<uint64_t> append(std::span<const protocol::update> updates);
  seastar::future<protocol::update> query(const index::entry& entry) const;
  // TODO(jason): add batch query method
  //seastar::future<std::vector<protocol::update>> query(
  //    std::span<const index::entry> entries) const;
  seastar::future<> sync();
  seastar::future<> close();
  seastar::future<std::vector<index::entry>> generate_index() const;

 private:
  // TODO: scheduling group
  std::string _filepath;
  seastar::file _file;
  uint64_t _bytes = 0;
  uint64_t _aligned_pos = 0;
  std::string _tail;
};

}  // namespace rafter::storage
