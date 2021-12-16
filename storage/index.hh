//
// Created by jason on 2021/10/13.
//

#pragma once

#include <stdint.h>

#include <memory>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "protocol/raft.hh"
#include "util/util.hh"

namespace rafter::storage {

using protocol::group_id;

class index {
 public:
  class entry {
   public:
    enum type : uint8_t {
      normal, state, snapshot, compaction,
    };
    // the first included raft log entry index
    uint64_t first_index = protocol::log_id::invalid_index;
    // the last included raft log entry index
    uint64_t last_index = protocol::log_id::invalid_index;
    // the filename of the segment file
    uint64_t filename = 0;
    // the offset of the raw data in the segment file
    uint64_t offset = 0;
    // the length of the raw data in the segment file
    uint64_t length = 0;
    // the type of the raw data
    enum type type = type::normal;

    std::strong_ordering operator<=>(const entry &) const = default;
    bool empty() const noexcept;
    bool is_normal() const noexcept;
    bool is_state() const noexcept;
    bool is_snapshot() const noexcept;
    bool is_compaction() const noexcept;
    bool try_merge(entry& e) noexcept;

    std::string debug_string() const;
  };

  index(group_id gid) : _gid(gid) {}

  index& set_compacted_to(uint64_t compacted_to) noexcept;

  uint64_t compacted_to() const noexcept;

  size_t size() const noexcept;

  bool empty() const noexcept;

  index& update(entry e);

  // make it private
  std::pair<uint64_t, bool> binary_search(
      uint64_t start,
      uint64_t end,
      uint64_t raft_index) const noexcept;

  std::span<const entry> query(protocol::hint range) const noexcept;

  bool file_in_use(uint64_t filename) const noexcept;

  // return the max filename of obsolete files
  // any segment files with filename <= max obsolete filename can be deleted
  uint64_t compaction();

  std::vector<uint64_t> remove_obsolete_entries(uint64_t max_obsolete_filename);

  bool operator==(const index& rhs) const noexcept;
  bool operator!=(const index& rhs) const noexcept;

  std::string debug_string() const;

 private:
  group_id _gid;
  // entries within (0, _compacted_to) can be compacted
  uint64_t _compacted_to = protocol::log_id::invalid_index;
  std::vector<entry> _entries;
};


class node_index {
 public:
  explicit node_index(group_id gid) : _gid(gid), _index(gid) {}

  bool update_entry(const index::entry& e);

  bool update_snapshot(const index::entry& e);

  bool update_state(const index::entry& e);

  bool file_in_use(uint64_t filename) const noexcept;

  bool file_in_tracking(uint64_t filename) const noexcept;

  std::span<const index::entry> query(protocol::hint range) const noexcept;

  index::entry query_state() const noexcept;

  index::entry query_snapshot() const noexcept;

  uint64_t compacted_to() const noexcept;

  void set_compacted_to(uint64_t index) noexcept;

  std::vector<uint64_t> compaction();

  bool operator==(const node_index& rhs) const noexcept;
  bool operator!=(const node_index& rhs) const noexcept;

 private:
  group_id _gid;
  index::entry _snapshot;
  index::entry _state;
  index _index;
  std::unordered_set<uint64_t> _filenames;
};

class index_group {
 public:
  index_group() = default;
  protocol::hard_state get_hard_state(group_id id);

  seastar::lw_shared_ptr<node_index> get_node_index(group_id id);

  std::span<const index::entry> query(group_id id, protocol::hint range);

  index::entry query_state(group_id id);

  index::entry query_snapshot(group_id id);

  // update the index and return if this filename is newly referenced
  // e should contain filename, offset, length
  bool update(const protocol::update& u, index::entry e);

  uint64_t compacted_to(group_id id);

  void set_compacted_to(group_id id, uint64_t index);

  // for testing
  bool operator==(const index_group& rhs) const noexcept;
  bool operator!=(const index_group& rhs) const noexcept;

 private:
  std::unordered_map<
      group_id, seastar::lw_shared_ptr<node_index>, util::pair_hasher> _indexes;
  std::unordered_map<
      group_id, protocol::hard_state, util::pair_hasher> _states;
};

}  // namespace rafter::storage
