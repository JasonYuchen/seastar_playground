//
// Created by jason on 2021/12/21.
//

#include "raft_log.hh"

#include "core/logger.hh"
#include "util/error.hh"

namespace rafter::core {

using namespace seastar;

in_memory_log::in_memory_log(uint64_t last_index)
  : _marker(last_index + 1), _saved(last_index) {}

protocol::log_entry_span in_memory_log::query(protocol::hint range) const {
  auto upper = _marker + _entries.size();
  if (range.low > range.high || range.low < _marker || range.high > upper) {
    throw util::out_of_range_error(fmt::format(
        "invalid range:{}, marker:{}, upper:{}", range, _marker, upper));
  }
  auto start = _entries.begin();
  std::advance(start, range.low - _marker);
  auto end = _entries.begin();
  std::advance(end, range.high - _marker);
  return {start, end};
}

protocol::log_entry_span in_memory_log::get_entries_to_save() const {
  if (_saved + 1 - _marker > _entries.size()) {
    return {};
  }
  auto start = _entries.begin();
  std::advance(start, _saved + 1 - _marker);
  return {start, _entries.end()};
}

std::optional<uint64_t> in_memory_log::get_snapshot_index() const {
  if (_snapshot) {
    return _snapshot->log_id.index;
  }
  return std::optional<uint64_t>{};
}

std::optional<uint64_t> in_memory_log::get_last_index() const {
  if (!_entries.empty()) {
    return _entries.back()->lid.index;
  }
  return get_snapshot_index();
}

std::optional<uint64_t> in_memory_log::get_term(uint64_t index) const {
  if (index > 0 && index == _applied.index) {
    return _applied.term;
  }
  if (index < _marker) {
    if (_snapshot && _snapshot->log_id.index == index) {
      return _snapshot->log_id.term;
    }
    return std::optional<uint64_t>{};
  }
  if (!_entries.empty() && index <= _entries.back()->lid.index) {
    return _entries[index - _marker]->lid.term;
  }
  return std::optional<uint64_t>{};
}

void in_memory_log::advance(
    protocol::log_id stable_log, uint64_t stable_snapshot_index) {
  if (stable_log.index != protocol::log_id::INVALID_INDEX) {
    advance_saved_log(stable_log);
  }
  if (stable_snapshot_index != protocol::log_id::INVALID_INDEX) {
    advance_saved_snapshot(stable_snapshot_index);
  }
}

void in_memory_log::advance_saved_log(protocol::log_id saved_log) {
  if (saved_log.index < _marker) {
    return;
  }
  if (_entries.empty()) {
    return;
  }
  if (saved_log.index > _entries.back()->lid.index ||
      saved_log.term != _entries[saved_log.index - _marker]->lid.term) {
    return;
  }
  _saved = saved_log.index;
}

void in_memory_log::advance_applied_log(uint64_t applied_index) {
  if (applied_index < _marker) {
    return;
  }
  if (_entries.empty()) {
    return;
  }
  if (applied_index > _entries.back()->lid.index) {
    return;
  }
  if (_entries[applied_index - _marker]->lid.index != applied_index) {
    throw util::failed_precondition_error("mismatch last applied index");
  }
  _applied = _entries[applied_index - _marker]->lid;
  auto new_marker = applied_index + 1;
  _shrunk = true;
  auto end = _entries.begin();
  std::advance(end, new_marker - _marker);
  _entries.erase(_entries.begin(), end);
  _marker = new_marker;
  assert_marker();
  // TODO(jyc): resize entry slice
  // TODO(jyc): rate limiter
}

void in_memory_log::advance_saved_snapshot(uint64_t saved_snapshot_index) {
  auto index = get_snapshot_index();
  if (!index) {
    return;
  }
  if (*index == saved_snapshot_index) {
    _snapshot.release();
  } else {
    l.warn(
        "in_memory_log::advance_saved_snapshot: index mismatch, expect:{}, "
        "actual:{}",
        saved_snapshot_index,
        *index);
  }
}

void in_memory_log::merge(protocol::log_entry_span entries) {
  if (entries.empty()) {
    return;
  }
  // TODO(jyc): rate limiter
  auto first_index = entries.front()->lid.index;
  if (first_index >= _marker + _entries.size()) {
    protocol::utils::assert_continuous(_entries, entries);
    _entries.insert(_entries.end(), entries.begin(), entries.end());
    assert_marker();
    return;
  }

  if (first_index <= _marker) {
    _marker = first_index;
    _shrunk = false;
    _entries = {entries.begin(), entries.end()};
    _saved = first_index - 1;
    assert_marker();
    return;
  }

  auto start = _entries.begin();
  std::advance(start, first_index - _marker);
  _entries.erase(start, _entries.end());
  protocol::utils::assert_continuous(_entries, entries);
  _entries.insert(_entries.end(), entries.begin(), entries.end());
  _saved = std::min(_saved, first_index - 1);
  _shrunk = false;
  assert_marker();
}

void in_memory_log::restore(protocol::snapshot_ptr snapshot) {
  _snapshot = std::move(snapshot);
  _marker = _snapshot->log_id.index + 1;
  _applied = _snapshot->log_id;
  _shrunk = false;
  _entries.clear();
  _saved = _snapshot->log_id.index;
  // TODO(jyc): reset rate limiter
}

void in_memory_log::assert_marker() const {
  if (!_entries.empty() && _entries.front()->lid.index != _marker) {
    throw util::failed_precondition_error(fmt::format(
        "mismatch marker:{}, first index:{}",
        _marker,
        _entries.front()->lid.index));
  }
}

log_reader::log_reader(protocol::group_id gid, storage::segment_manager& log)
  : _gid(gid), _log(log), _snapshot(make_lw_shared<protocol::snapshot>()) {}

protocol::hard_state log_reader::get_state() const noexcept { return _state; }
void log_reader::set_state(protocol::hard_state state) { _state = state; }

protocol::membership_ptr log_reader::get_membership() const noexcept {
  return _snapshot->membership;
}

future<> log_reader::query(
    protocol::hint range,
    protocol::log_entry_vector& entries,
    size_t max_size) {
  if (range.low < first_index()) [[unlikely]] {
    throw util::compacted_error(range.low, first_index());
  }

  if (range.high > last_index() + 1) [[unlikely]] {
    throw util::unavailable_error(range.high, last_index() + 1);
  }
  co_await _log.query_entries(_gid, range, entries, max_size);
  if (entries.size() == range.high - range.low) {
    co_return;
  }
  if (!entries.empty()) {
    if (range.low < entries.front()->lid.index) {
      throw util::compacted_error(range.low, entries.front()->lid.index);
    }
    if (last_index() < entries.back()->lid.index) {
      throw util::unavailable_error(
          entries.back()->lid.index, last_index() + 1);
    }
    throw util::failed_precondition_error(fmt::format(
        "log hole found in [{}:{}) at {}",
        range.low,
        range.high,
        entries.back()->lid.index + 1));
  }
  throw util::unavailable_error(range.low, 0);
}

future<uint64_t> log_reader::get_term(uint64_t index) {
  if (index == _marker.index) {
    co_return _marker.term;
  }
  protocol::log_entry_vector entry;
  co_await query({.low = index, .high = index + 1}, entry, UINT64_MAX);
  if (entry.empty()) {
    co_return protocol::log_id::INVALID_INDEX;
  }
  co_return entry.front()->lid.term;
}

protocol::hint log_reader::get_range() const noexcept {
  return {first_index(), last_index()};
}

void log_reader::set_range(
    protocol::hint range) {  // range.high = range.low + length
  if (range.low == range.high) {
    return;
  }
  auto first = first_index();
  if (range.high - 1 < first) {
    return;
  }
  range.low = std::max(range.low, first);
  auto offset = range.low - _marker.index;
  if (_length > offset) {
    _length = range.high - _marker.index;
  } else if (_length == offset) {
    _length += offset;
  } else {
    throw util::failed_precondition_error("");
  }
}

protocol::snapshot_ptr log_reader::get_snapshot() const noexcept {
  return _snapshot;
}

void log_reader::apply_snapshot(protocol::snapshot_ptr snapshot) {
  if (_snapshot->log_id.index >= snapshot->log_id.index) {
    throw util::out_of_date_error(
        "snapshot", _snapshot->log_id.index, snapshot->log_id.index);
  }
  _snapshot = snapshot;
}

void log_reader::apply_entries(protocol::log_entry_span entries) {
  if (entries.empty()) {
    return;
  }
  if (entries.front()->lid.index + entries.size() - 1 !=
      entries.back()->lid.index) {
    throw util::failed_precondition_error("log hole found");
  }
  set_range({entries.front()->lid.index, entries.back()->lid.index + 1});
}

future<> log_reader::apply_compaction(uint64_t index) {
  // index == _marker.index is a no-op
  if (index < _marker.index) {
    throw util::compacted_error(index, first_index());
  }
  if (index > last_index()) {
    throw util::unavailable_error(index, last_index());
  }
  auto term = co_await get_term(index);
  auto i = index - _marker.index;
  _length -= i;
  _marker.index = index;
  _marker.term = term;
}

}  // namespace rafter::core
