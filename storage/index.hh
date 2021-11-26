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

class segment;

class index {
 public:
  class entry {
   public:
    enum type {
      normal, state, snapshot, compaction,
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

    bool is_compaction() const noexcept {
      return type == type::compaction;
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

  std::span<const entry> query(protocol::hint range) const noexcept {
    if (range.low > range.high) {
      // TODO: throw
    }
    if (range.low <= _compacted_to) {
      return {};
    }
    if (_entries.empty()) {
      return {};
    }
    auto [start, found] = binary_search(0, _entries.size() - 1, range.low);
    if (!found) {
      return {};
    }
    uint64_t end = start;
    for (; end < _entries.size(); ++end) {
      if (range.high <= _entries[end].first_index) {
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
    uint64_t max_obsolete = UINT64_MAX;
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

   void remove_obsolete_entries(uint64_t max_obsolete_filename) {
     auto it = _entries.begin();
     for (; it != _entries.end(); ++it) {
       if (it->filename > max_obsolete_filename) {
         break;
       }
     }
     _entries.erase(_entries.begin(), it);
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

  bool update_entry(const index::entry& e) {
    if (e.first_index != protocol::log_id::invalid_index &&
        e.last_index != protocol::log_id::invalid_index) {
      _index.update(e);
      return _filenames.insert(e.filename).second;
    }
    return false;
  }

  bool update_snapshot(const index::entry& e) {
    if (e.first_index > _snapshot.first_index) {
      _snapshot = e;
      return _filenames.insert(e.filename).second;
    }
    return false;
  }

  bool update_state(const index::entry& e) {
    if (!e.empty()) {
      _state = e;
      return _filenames.insert(e.filename).second;
    }
    return false;
  }

  bool file_in_use(uint64_t filename) {
    return _snapshot.filename == filename ||
           _state.filename == filename ||
           _index.file_in_use(filename);
  }

  std::span<const index::entry> query(protocol::hint range) const noexcept {
    return _index.query(range);
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
    _index.remove_obsolete_entries(max_obsolete);
    std::vector<uint64_t> obsoletes;
    for (auto it = _filenames.begin(); it != _filenames.end(); ) {
      if (*it <= max_obsolete) {
        obsoletes.emplace_back(*it);
        it = _filenames.erase(it);
      } else {
        ++it;
      }
    }
    return obsoletes;
  }

 private:
  group_id _id;
  index::entry _snapshot;
  index::entry _state;
  index _index;
  std::unordered_set<uint64_t> _filenames;
};

class index_group {
 public:
  index_group() = default;
  protocol::hard_state get_hard_state(group_id id) {
    assert(id.valid());
    return _states[id];
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
      group_id id, protocol::hint range) {
    assert(id.valid());
    return get_node_index(id)->query(range);
  }

  index::entry query_state(group_id id) {
    assert(id.valid());
    return get_node_index(id)->query_state();
  }

  index::entry query_snapshot(group_id id) {
    assert(id.valid());
    return get_node_index(id)->query_snapshot();
  }

  // update the index and return if this filename is newly referenced
  bool update(
      const protocol::update& u,
      uint64_t filename,
      uint64_t offset,
      uint64_t length) {
    assert(u.group_id.valid());
    auto i = get_node_index(u.group_id);
    index::entry e {
        .id = u.group_id,
        .filename = filename,
        .offset = offset,
        .length = length,
    };
    auto compactedTo = u.compactedTo();
    if (compactedTo != protocol::log_id::invalid_index) {
      i->set_compacted_to(compactedTo);
    } else {
      if (!u.entries_to_save.empty()) {
        e.first_index = u.first_index;
        e.last_index = u.last_index;
        e.type = index::entry::type::normal;
        return i->update_entry(e);
      }
      if (!u.state.empty()) {
        e.first_index = u.state.commit;
        e.last_index = protocol::log_id::invalid_index;
        e.type = index::entry::type::state;
        _states[u.group_id] = u.state;
        return i->update_state(e);
      }
      if (u.snapshot) {
        e.first_index = u.snapshot->log_id.index;
        e.last_index = protocol::log_id::invalid_index;
        e.type = index::entry::type::snapshot;
        return i->update_snapshot(e);
      }
    }
    return false;
  }

  uint64_t compacted_to(group_id id) {
    assert(id.valid());
    return get_node_index(id)->compacted_to();
  }

  void set_compacted_to(group_id id, uint64_t index) {
    assert(id.valid());
    auto ni = get_node_index(id);
    ni->set_compacted_to(index);
    // TODO
    get_node_index(id)->set_compacted_to(index);
  }

 private:
  std::unordered_map<
      group_id, seastar::lw_shared_ptr<node_index>, util::pair_hasher> _indexes;
  std::unordered_map<
      group_id, protocol::hard_state, util::pair_hasher> _states;
};

}  // namespace rafter::storage
