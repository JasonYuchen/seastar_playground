//
// Created by jason on 2021/10/13.
//

#include "index.hh"

#include <fmt/format.h>

#include "storage/logger.hh"
#include "util/error.hh"

namespace rafter::storage {

using namespace seastar;
using namespace std;

bool index::entry::empty() const noexcept {
  return filename == 0;
}

bool index::entry::is_normal() const noexcept {
  return type == type::normal;
}

bool index::entry::is_state() const noexcept {
  return type == type::state;
}

bool index::entry::is_snapshot() const noexcept {
  return type == type::snapshot;
}

bool index::entry::is_compaction() const noexcept {
  return type == type::compaction;
}

bool index::entry::try_merge(index::entry& e) noexcept {
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

std::string index::entry::debug_string() const {
  return fmt::format("entry[fi:{}, li:{}, fn:{}, off:{}, len:{}, type:{}]",
      first_index, last_index, filename, offset, length, uint8_t{type});
}

index& index::set_compacted_to(uint64_t compacted_to) noexcept {
  _compacted_to = std::max(_compacted_to, compacted_to);
  return *this;
}

uint64_t index::compacted_to() const noexcept {
  return _compacted_to;
}

size_t index::size() const noexcept {
  return _entries.size();
}

bool index::empty() const noexcept {
  return _entries.empty();
}

index& index::update(index::entry e) {
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
    l.error("{} index::update: first index {} out of range [{}, {}]",
            _gid.to_string(),
            e.first_index,
            _entries.front().first_index,
            _entries.back().last_index);
    throw util::out_of_range_error();
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

pair<uint64_t, bool> index::binary_search(
    uint64_t start, uint64_t end, uint64_t raft_index) const noexcept {
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

span<const index::entry> index::query(protocol::hint range) const noexcept {
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
  uint64_t end = start + 1;
  for (; end < _entries.size(); ++end) {
    if (range.high < _entries[end].first_index) {
      break;
    }
    if (_entries[end - 1].last_index + 1 !=
        _entries[end].first_index) [[unlikely]] {
      break;
    }
  }
  return {_entries.begin() + start, _entries.begin() + end};
}

bool index::file_in_use(uint64_t filename) const noexcept {
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

uint64_t index::compaction() {
  if (empty()) {
    return UINT64_MAX;
  }
  uint64_t prev_filename = _entries.front().filename;
  uint64_t max_obsolete = prev_filename - 1;
  for (size_t i = 1; i < _entries.size(); ++i) {
    const auto& e = _entries[i];
    const auto& prev_e = _entries[i-1];
    if (!e.is_normal()) [[unlikely]] {
      throw util::failed_precondition_error("invalid index type");
    }
    if (e.filename != prev_filename) {
      if (prev_e.last_index <= _compacted_to) {
        max_obsolete = prev_e.filename;
      }
      prev_filename = e.filename;
    }
  }
  return max_obsolete;
}

vector<uint64_t> index::remove_obsolete_entries(uint64_t max_obsolete_filename) {
  vector<uint64_t> obsoletes;
  auto it = _entries.begin();
  for (; it != _entries.end(); ++it) {
    if (it->filename > max_obsolete_filename) {
      break;
    }
    obsoletes.emplace_back(it->filename);
  }
  _entries.erase(_entries.begin(), it);
  return obsoletes;
}

bool index::operator==(const index& rhs) const noexcept {
  return _gid == rhs._gid &&
         _compacted_to == rhs._compacted_to &&
         _entries == rhs._entries;
}

bool index::operator!=(const index& rhs) const noexcept {
  return !(*this == rhs);
}

std::string index::debug_string() const {
  return "NOT IMPLEMENTED";
}

bool node_index::update_entry(const index::entry& e) {
  if (e.first_index != protocol::log_id::invalid_index &&
      e.last_index != protocol::log_id::invalid_index) {
    _index.update(e);
    return _filenames.insert(e.filename).second;
  }
  return false;
}

bool node_index::update_snapshot(const index::entry& e) {
  if (e.first_index > _snapshot.first_index) {
    _snapshot = e;
    return _filenames.insert(e.filename).second;
  }
  return false;
}

bool node_index::update_state(const index::entry& e) {
  if (!e.empty()) {
    _state = e;
    return _filenames.insert(e.filename).second;
  }
  return false;
}

bool node_index::file_in_use(uint64_t filename) const noexcept {
  return _snapshot.filename == filename ||
      _state.filename == filename ||
      _index.file_in_use(filename);
}

bool node_index::file_in_tracking(uint64_t filename) const noexcept {
  return _filenames.contains(filename);
}

span<const index::entry> node_index::query(
    protocol::hint range) const noexcept {
  return _index.query(range);
}

index::entry node_index::query_state() const noexcept {
  return _state;
}

index::entry node_index::query_snapshot() const noexcept {
  return _snapshot;
}

uint64_t node_index::compacted_to() const noexcept {
  return _index.compacted_to();
}

void node_index::set_compacted_to(uint64_t index) noexcept {
  _index.set_compacted_to(index);
}

vector<uint64_t> node_index::compaction() {
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
  vector<uint64_t> obsoletes;
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

bool node_index::operator==(const node_index& rhs) const noexcept {
  return _gid == rhs._gid &&
         _snapshot == rhs._snapshot &&
         _state == rhs._state &&
         _index == rhs._index &&
         _filenames == rhs._filenames;
}

bool node_index::operator!=(const node_index& rhs) const noexcept {
  return !(*this == rhs);
}

protocol::hard_state index_group::get_hard_state(group_id id) {
  assert(id.valid());
  return _states[id];
}

lw_shared_ptr<node_index> index_group::get_node_index(group_id id) {
  assert(id.valid());
  auto it = _indexes.find(id);
  if (it != _indexes.end()) {
    return it->second;
  }
  auto ni = make_lw_shared<node_index>(id);
  _indexes[id] = ni;
  return ni;
}

span<const index::entry> index_group::query(group_id id, protocol::hint range) {
  assert(id.valid());
  if (range.low > range.high) [[unlikely]] {
    throw util::failed_precondition_error("invalid query range");
  }
  return get_node_index(id)->query(range);
}

index::entry index_group::query_state(group_id id) {
  assert(id.valid());
  return get_node_index(id)->query_state();
}

index::entry index_group::query_snapshot(group_id id) {
  assert(id.valid());
  return get_node_index(id)->query_snapshot();
}

bool index_group::update(const protocol::update& u, index::entry e) {
  assert(u.gid.valid());
  auto i = get_node_index(u.gid);
  auto compacted_to = u.compacted_to();
  if (compacted_to != protocol::log_id::invalid_index) {
    i->set_compacted_to(compacted_to);
    return false;
  }
  bool new_tracking = false;
  if (u.first_index != protocol::log_id::invalid_index &&
      u.last_index != protocol::log_id::invalid_index) {
    e.first_index = u.first_index;
    e.last_index = u.last_index;
    e.type = index::entry::type::normal;
    new_tracking = i->update_entry(e);
  }
  if (!u.state.empty()) {
    e.first_index = u.state.commit;
    e.last_index = protocol::log_id::invalid_index;
    e.type = index::entry::type::state;
    _states[u.gid] = u.state;
    new_tracking = new_tracking || i->update_state(e);
  }
  if (u.snapshot_index != protocol::log_id::invalid_index) {
    e.first_index = u.snapshot_index;
    e.last_index = protocol::log_id::invalid_index;
    e.type = index::entry::type::snapshot;
    new_tracking = new_tracking || i->update_snapshot(e);
  }
  return new_tracking;
}

uint64_t index_group::compacted_to(group_id id) {
  assert(id.valid());
  return get_node_index(id)->compacted_to();
}

void index_group::set_compacted_to(group_id id, uint64_t index) {
  assert(id.valid());
  get_node_index(id)->set_compacted_to(index);
}

bool index_group::operator==(const index_group& rhs) const noexcept {
  if (_states != rhs._states) {
    return false;
  }
  if (_indexes.size() != rhs._indexes.size()) {
    return false;
  }
  for (const auto& [gid, idx] : _indexes) {
    auto it = rhs._indexes.find(gid);
    if (it == rhs._indexes.end()) {
      return false;
    }
    if (*idx != *it->second) {
      return false;
    }
  }
  return true;
}

bool index_group::operator!=(const index_group& rhs) const noexcept {
  return !(*this == rhs);
}

}  // namespace rafter::storage
