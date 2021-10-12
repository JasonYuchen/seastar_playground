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

  struct log_locator {
    uint64_t term = 0;
    uint64_t offset = 0;
  };

  static segment create(std::filesystem::path filepath);
  static segment parse(std::filesystem::path filepath);

  uint64_t bytes() const noexcept;

  // load the index block for future read
  seastar::future<> load();
  // offload the index to release memory
  seastar::future<> offload();

  seastar::future<> update_hard_state(protocol::hard_state state);
  seastar::future<uint64_t> append(std::span<protocol::log_entry_ptr> entries);
  seastar::future<> finalize();
  seastar::future<> sync();

 private:
  segment() = default;

  struct header {
    static constexpr uint64_t HEADER_SIZE = 4096;
    static constexpr uint64_t TERM = 0;
    static constexpr uint64_t VOTE = 8;
    static constexpr uint64_t COMMIT = 16;
    static constexpr uint64_t START_INDEX = 24;
    static constexpr uint64_t END_INDEX = 32;
    static constexpr uint64_t CHECKSUM_TYPE = 36;
    static constexpr uint64_t CHECKSUM = 40;
    static constexpr uint64_t INDEX_OFFSET = 48;

    header() = default;
    DEFAULT_MOVE_AND_ASSIGN(header);

    header& allocate(uint64_t alignment);
    uint64_t term() const noexcept;
    header& set_term(uint64_t term) noexcept;
    uint64_t vote() const noexcept;
    header& set_vote(uint64_t vote) noexcept;
    uint64_t commit() const noexcept;
    header& set_commit(uint64_t commit) const noexcept;
    header& fill_checksum(uint32_t type);
    header& set_index_offset(uint64_t index_offset);
    seastar::future<size_t> serialize(seastar::file& file) const;
    seastar::future<> deserialize(seastar::file& file);

    seastar::temporary_buffer<char> data;
  };

  class index {
   public:
    DEFAULT_COPY_MOVE_AND_ASSIGN(index);
    bool contains(uint64_t index) const noexcept;
    uint64_t first_index() const noexcept;
    uint64_t last_index() const noexcept;
    uint64_t term(uint64_t index) const noexcept;
    uint64_t bytes() const noexcept;
    void append(log_locator locator);
    void append(std::span<log_locator> entries);
    seastar::future<> serialize(seastar::output_stream<char>& out) const {
      // TODO
      co_return;
    }
    seastar::future<> deserialize() const {
      // TODO
      co_return;
    }

   private:
    uint64_t _start_index = 0;  // the index of locators_.front();
    std::vector<log_locator> _locators;
  };

 private:
  header _header;
  // TODO: scheduling group
  seastar::file _file;
  index _index;
  uint64_t _bytes = 0;
  bool _archived = false;
  bool _loaded = false;
  uint64_t _pos = 0;
  std::string _tail;
};

class segment_manager {
 public:
  // TODO: storage configuration class
  segment_manager(group_id id, std::filesystem::path data_dir);

  future<> append(protocol::hard_state state, protocol::log_entry_span entries);

 private:
  const group_id _group_id;
  // segment dir, e.g. <data_dir>/<group_id>
  std::filesystem::path _path;
  const uint64_t _rolling_size = 1024 * 1024 * 1024;  // in B
  // start entry -> archived segment
  std::map<log_id, std::unique_ptr<segment>> _segments;
  std::unique_ptr<segment> _active_segment;
};

}  // namespace rafter::storage
