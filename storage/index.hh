//
// Created by jason on 2021/10/13.
//

#pragma once

#include <stdint.h>

#include <memory>
#include <span>
#include <unordered_map>
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
    enum type {
      normal, state, snapshot
    };
    // the raft group of this entry
    group_id id;
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

    bool empty() const noexcept {
      return filename == 0;
    }

    bool is_normal() const noexcept {
      return type == type::normal;
    }

    bool is_state() const noexcept {
      return type == type::state;
    }

    bool is_snapshot() const noexcept {
      return type == type::snapshot;
    }

    bool try_merge(entry& e) noexcept {
      assert(id == e.id);
      if (last_index + 1 == e.first_index &&
          offset + length == e.offset &&
          filename == e.filename &&
          type == e.type) {
        last_index = e.last_index;
        length += e.length;
        e = entry{};
        return true;
      }
      return false;
    }

    // return (merged, need_more_merge)
    std::pair<bool, bool> _update(entry& e) noexcept {
      if (try_merge(e)) {
        return {true, false};
      }
      // overwrite
      if (first_index == e.first_index) {
        *this = std::exchange(e, {});
        return {true, false};
      }
      // overwrite and need more merge
      if (first_index > e.first_index) {
        *this = std::exchange(e, {});
        return {true, true};
      }
      // partial overwrite
      if (first_index < e.first_index && e.first_index <= last_index) {
        last_index = e.first_index - 1;
      }
      return {false, false};
    }
  };

  index& set_compacted_to(uint64_t compacted_to) noexcept {
    _compacted_to = std::max(_compacted_to, compacted_to);
    return *this;
  }

  uint64_t compacted_to() const noexcept {
    return _compacted_to;
  }

  size_t size() const noexcept {
    return _entries.size();
  }

  bool empty() const noexcept {
    return _entries.empty();
  }

  index& append(entry e) {
    _entries.emplace_back(std::move(e));
    return *this;
  }

  index& update(entry e) {
    if (empty()) [[unlikely]] {
      _entries.emplace_back(std::move(e));
      return *this;
    }
    if (e.first_index == _entries.back().last_index + 1) [[likely]] {
      _entries.emplace_back(std::move(e));
      return *this;
    }
    auto [idx, found] = binary_search(
        0, _entries.size() - 1, e.first_index);
    if (!found) [[unlikely]] {
      return *this;
    }
    auto st = _entries.begin();
    std::advance(st, idx + 1);
    if (_entries[idx].first_index == e.first_index) {
      std::advance(st, -1);
    } else {
      _entries[idx].last_index = e.first_index - 1;
    }
    _entries.erase(st, _entries.end());
    _entries.emplace_back(std::move(e));
    return *this;
    // TODO:
    //  partial overwrite? need split index and relocate index positions
    //  e.g.  entries: [1-4, 5-12, 13-55]
    //        update a new index entry 9-14 (if the commit index = 4)
    //        entries: [1-4, 5-9, 9-14] (all indexes > 14 will be dropped)
  }

  // we cannot easily update the indexes due to the underlying WAL serving more
  // than 1 Raft group
  index& _update(entry e) {
    if (empty()) {
      _entries.emplace_back(std::move(e));
      return *this;
    }
    auto last = _entries.back();
    auto [merged, more] = last._update(e);
    if (more) {
      if (size() == 1) {
        _entries.resize(1, e);
        return *this;
      }
      _entries.pop_back();
      _update(e);
      return *this;
    }
    _entries.back() = last;
    if (!merged) {
      _entries.emplace_back(std::move(e));
    }
    return *this;
  }

  // make it private
  std::pair<uint64_t, bool> binary_search(
      uint64_t start,
      uint64_t end,
      uint64_t raft_index) const noexcept {
    if (start == end) {
      if (_entries[start].first_index <= raft_index &&
          _entries[start].last_index >= raft_index) {
        return {start, true};
      }
      return {0, false};
    }
    uint64_t mid = start + (end - start) / 2;
    if (_entries.front().first_index <= raft_index &&
        _entries[mid].last_index >= raft_index) {
      return binary_search(start, mid, raft_index);
    }
    return binary_search(mid + 1, end, raft_index);
  }

  std::span<const entry> query(uint64_t low, uint64_t high) const noexcept {
    // assert(low <= high)
    if (_entries.empty()) {
      return {};
    }
    auto [start, found] = binary_search(0, _entries.size() - 1, low);
    if (!found) {
      return {};
    }
    uint64_t end = start;
    for (; end < _entries.size(); ++end) {
      if (high <= _entries[end].first_index) {
        break;
      }
      if (end > start &&
          _entries[end - 1].last_index + 1 != _entries[end].first_index) {
        break;
      }
    }
    return {_entries.begin() + start, _entries.begin() + end};
  }

  bool file_in_use(uint64_t filename) const noexcept {
    for (auto&& e : _entries) {
      if (e.filename == filename) {
        return true;
      }
      if (e.filename > filename) {
        return false;
      }
    }
    return false;
  }

  // return the max filename of obsolete files
  // any segment files with filename <= max obsolete filename can be deleted
  uint64_t compaction() {
    if (empty()) {
      return UINT64_MAX;
    }
    uint64_t max_obsolete = _entries.front().filename - 1;
    for (auto&& e : _entries) {
      if (e.last_index <= _compacted_to) {
        max_obsolete = e.filename;
        if (!e.is_normal()) {
          // throw not a regular entry error
        }
      }
    }
    return max_obsolete;
  }

  std::vector<uint64_t> remove_obsolete_files(uint64_t max_obsolete_filename) {
    std::vector<uint64_t> obsolete_files;
    uint64_t prev_filename = 0;
    int i = 0;
    for (; i < size(); ++i) {
      auto&& e = _entries[i];
      if (e.filename <= max_obsolete_filename && e.filename != prev_filename) {
        obsolete_files.emplace_back(e.filename);
        prev_filename = e.filename;
      }
      if (e.filename > max_obsolete_filename) {
        break;
      }
    }
    if (!obsolete_files.empty()) {
      _entries.erase(_entries.begin(), _entries.begin() + i);
    }
    return obsolete_files;
  }

 private:
  // entries within (0, _compacted_to) can be compacted
  uint64_t _compacted_to = protocol::log_id::invalid_index;
  std::vector<entry> _entries;
};

class node_index {
 public:
  explicit node_index(group_id id) : _id(id) {}
  void clear() noexcept {
    _snapshot = index::entry{};
    _state = index::entry{};
    _index = index{};
  }

  void update_entry(index::entry e) {
    if (e.first_index != protocol::log_id::invalid_index &&
        e.last_index != protocol::log_id::invalid_index) {
      _index.update(e);
    }
  }

  void update_snapshot(index::entry e) {
    if (e.first_index > _snapshot.first_index) {
      _snapshot = e;
    }
  }

  void update_state(index::entry e) {
    if (!e.empty()) {
      _state = e;
    }
  }

  bool file_in_use(uint64_t filename) {
    return _snapshot.filename == filename ||
           _state.filename == filename ||
           _index.file_in_use(filename);
  }

  std::span<const index::entry> query(
      uint64_t low, uint64_t high) const noexcept {
    return _index.query(low, high);
  }

  index::entry query_state() const noexcept {
    return _state;
  }

  index::entry query_snapshot() const noexcept {
    return _snapshot;
  }

  uint64_t compacted_to() const noexcept {
    return _index.compacted_to();
  }

  void set_compacted_to(uint64_t index) {
    _index.set_compacted_to(index);
  }

  std::vector<uint64_t> compaction() {
    auto max_obsolete = _index.compaction();
    if (!_snapshot.empty()) {
      max_obsolete = std::min(max_obsolete, _snapshot.filename - 1);
    }
    if (!_state.empty()) {
      max_obsolete = std::min(max_obsolete, _state.filename - 1);
    }
    if (max_obsolete == 0 || max_obsolete == UINT64_MAX) {
      return {};
    }
    return _index.remove_obsolete_files(max_obsolete);
  }

 private:
  group_id _id;
  index::entry _snapshot;
  index::entry _state;
  index _index;
};

class index_group {
 public:
  protocol::hard_state get_hard_state(group_id id) {
    assert(id.valid());
    return _states[id];
  }

  index_group& set_hard_state(group_id id, protocol::hard_state state) {
    assert(id.valid());
    _states[id] = state;
    return *this;
  }

  seastar::lw_shared_ptr<node_index> get_node_index(group_id id) {
    assert(id.valid());
    if (_indexes.contains(id)) {
      return _indexes[id];
    }
    _indexes[id] = seastar::make_lw_shared<node_index>(id);
    return _indexes[id];
  }

  std::span<const index::entry> query(
      group_id id, uint64_t low, uint64_t high) {
    assert(id.valid());
    return get_node_index(id)->query(low, high);
  }

  index::entry query_state(group_id id) {
    assert(id.valid());
    return get_node_index(id)->query_state();
  }

  index::entry query_snapshot(group_id id) {
    assert(id.valid());
    return get_node_index(id)->query_snapshot();
  }

  uint64_t compacted_to(group_id id) {
    assert(id.valid());
    return get_node_index(id)->compacted_to();
  }

  void set_compacted_to(group_id id, uint64_t index) {
    assert(id.valid());
    get_node_index(id)->set_compacted_to(index);
  }

  index_group& set_in_use_filename(group_id id, uint64_t filename) {
    assert(id.valid());
    if (!_minimal_in_use_filenames.contains(id)) {
      _minimal_in_use_filenames[id] = UINT64_MAX;
    }
    _minimal_in_use_filenames[id] = filename;
    return *this;
  }

  index_group& clear_in_use_filename(group_id id) {
    assert(id.valid());
    _minimal_in_use_filenames[id] = UINT64_MAX;
    return *this;
  }

  uint64_t minimal_in_use_filename() const noexcept {
    uint64_t filename = UINT64_MAX;
    for (const auto& [id, name] : _minimal_in_use_filenames) {
      assert(id.valid());
      filename = std::min(filename, name);
    }
    return filename;
  }

 private:
  std::unordered_map<
      group_id, seastar::lw_shared_ptr<node_index>, util::pair_hasher> _indexes;
  std::unordered_map<
      group_id, protocol::hard_state, util::pair_hasher> _states;
  std::unordered_map<
      group_id, uint64_t, util::pair_hasher> _minimal_in_use_filenames;
};

}  // namespace rafter::storage
