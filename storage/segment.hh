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
#include "util/fragmented_temporary_buffer.hh"
#include "util/types.hh"

namespace rafter::storage {

class segment {
 public:
  DEFAULT_MOVE_AND_ASSIGN(segment);

  static segment create(std::filesystem::path filepath);
  static segment parse(std::filesystem::path filepath);

  uint64_t bytes() const noexcept;

  seastar::future<uint64_t> append(std::span<protocol::log_entry_ptr> entries);
  seastar::future<> finalize();
  seastar::future<> sync();

 private:
  segment() = default;

 private:
  // TODO: scheduling group
  uint64_t _filename;
  seastar::file _file;
  uint64_t _bytes = 0;
  bool _archived = false;
  bool _loaded = false;
  uint64_t _pos = 0;
  std::string _tail;
};

}  // namespace rafter::storage
