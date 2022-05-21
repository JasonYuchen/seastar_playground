//
// Created by jason on 2021/12/21.
//

#include "raft_log.hh"

#include <numeric>
#include <seastar/core/coroutine.hh>

#include "core/logger.hh"
#include "util/error.hh"

namespace rafter::core {

in_memory_log::in_memory_log(uint64_t last_index)
  : _marker(last_index + 1), _saved(last_index) {}

size_t in_memory_log::query(
    protocol::hint range,
    protocol::log_entry_vector& entries,
    size_t max_bytes) const {
  auto upper = _marker + _entries.size();
  if (range.low > range.high || range.low < _marker || range.high > upper) {
    throw util::out_of_range_error(fmt::format(
        "invalid range:{}, marker:{}, upper:{}", range, _marker, upper));
  }
  auto it = _entries.begin();
  std::advance(it, range.low - _marker);
  while (true) {
    if (it == _entries.end()) {
      break;
    }
    if ((*it)->lid.index >= range.high) {
      break;
    }
    auto bytes = (*it)->bytes();
    if (bytes > max_bytes) {
      max_bytes = 0;
      break;
    }
    if (!entries.empty() && entries.back()->lid.index + 1 != (*it)->lid.index) {
      throw util::failed_precondition_error(fmt::format(
          "gap found, left:{}, right:{}",
          entries.back()->lid.index,
          (*it)->lid.index));
    }
    entries.emplace_back(*it);
    max_bytes -= bytes;
  }
  return max_bytes;
}

protocol::log_entry_span in_memory_log::get_entries_to_save() const noexcept {
  if (_saved + 1 - _marker > _entries.size()) {
    return {};
  }
  auto start = _entries.begin();
  std::advance(start, _saved + 1 - _marker);
  return {start, _entries.end()};
}

protocol::snapshot_ptr in_memory_log::get_snapshot() const noexcept {
  return _snapshot;
}

std::optional<uint64_t> in_memory_log::get_snapshot_index() const noexcept {
  if (_snapshot) {
    return _snapshot->log_id.index;
  }
  return std::optional<uint64_t>{};
}

std::optional<uint64_t> in_memory_log::get_last_index() const noexcept {
  if (!_entries.empty()) {
    return _entries.back()->lid.index;
  }
  return get_snapshot_index();
}

std::optional<uint64_t> in_memory_log::get_term(uint64_t index) const noexcept {
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
    protocol::log_id stable_log, uint64_t stable_snapshot_index) noexcept {
  if (stable_log.index != protocol::log_id::INVALID_INDEX) {
    advance_saved_log(stable_log);
  }
  if (stable_snapshot_index != protocol::log_id::INVALID_INDEX) {
    advance_saved_snapshot(stable_snapshot_index);
  }
}

void in_memory_log::advance_saved_log(protocol::log_id saved_log) noexcept {
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

void in_memory_log::advance_saved_snapshot(
    uint64_t saved_snapshot_index) noexcept {
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

void in_memory_log::restore(protocol::snapshot_ptr snapshot) noexcept {
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

log_reader::log_reader(protocol::group_id gid, storage::logdb& logdb)
  : _gid(gid), _logdb(logdb), _snapshot(make_lw_shared<protocol::snapshot>()) {}

protocol::hard_state log_reader::get_state() const noexcept { return _state; }
void log_reader::set_state(protocol::hard_state state) noexcept {
  _state = state;
}

protocol::membership_ptr log_reader::get_membership() const noexcept {
  return _snapshot->membership;
}

future<size_t> log_reader::query(
    protocol::hint range,
    protocol::log_entry_vector& entries,
    size_t max_bytes) {
  if (range.low < first_index()) [[unlikely]] {
    co_return coroutine::make_exception(
        util::compacted_error(range.low, first_index()));
  }

  if (range.high > last_index() + 1) [[unlikely]] {
    co_return coroutine::make_exception(
        util::unavailable_error(range.high, last_index() + 1));
  }
  max_bytes = co_await _logdb.query_entries(_gid, range, entries, max_bytes);
  if (entries.size() == range.high - range.low) {
    co_return max_bytes;
  }
  if (!entries.empty()) {
    if (range.low < entries.front()->lid.index) {
      co_return coroutine::make_exception(
          util::compacted_error(range.low, entries.front()->lid.index));
    }
    if (last_index() < entries.back()->lid.index) {
      co_return coroutine::make_exception(
          util::unavailable_error(entries.back()->lid.index, last_index() + 1));
    }
    co_return coroutine::make_exception(
        util::failed_precondition_error(fmt::format(
            "log hole found in [{}:{}) at {}",
            range.low,
            range.high,
            entries.back()->lid.index + 1)));
  }
  co_return coroutine::make_exception(util::unavailable_error(range.low, 0));
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
  _marker = snapshot->log_id;
  _length = 1;
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

raft_log::raft_log(protocol::group_id gid, log_reader& log)
  : _gid(gid), _in_memory(log.get_range().high), _logdb(log) {
  auto [first, _] = log.get_range();
  _committed = first - 1;
  _processed = first - 1;
}

uint64_t raft_log::first_index() const noexcept {
  auto index = _in_memory.get_snapshot_index();
  if (index) {
    return *index;
  }
  auto [first, _] = _logdb.get_range();
  return first;
}

uint64_t raft_log::last_index() const noexcept {
  auto index = _in_memory.get_last_index();
  if (index) {
    return *index;
  }
  auto [_, last] = _logdb.get_range();
  return last;
}

future<uint64_t> raft_log::term(uint64_t index) const {
  auto [first, last] = term_entry_range();
  if (index < first || index > last) {
    co_return protocol::log_id::INVALID_TERM;
  }
  auto t = _in_memory.get_term(index);
  if (t) {
    co_return *t;
  }
  co_return co_await _logdb.get_term(index);
}

future<uint64_t> raft_log::last_term() const { return term(last_index()); }

future<bool> raft_log::term_index_match(protocol::log_id lid) const {
  auto t = co_await term(lid.index);
  co_return lid.term == t;
}

protocol::hint raft_log::term_entry_range() const noexcept {
  return {first_index() - 1, last_index()};
}

protocol::hint raft_log::entry_range() const noexcept {
  if (_in_memory.get_snapshot() && _in_memory.get_entries_size() == 0) {
    return {};
  }
  return {first_index(), last_index()};
}

uint64_t raft_log::first_not_applied_index() const noexcept {
  return std::max(_processed + 1, first_index());
}

uint64_t raft_log::apply_index_limit() const noexcept { return _committed + 1; }

bool raft_log::has_entries_to_apply() const noexcept {
  return apply_index_limit() > first_not_applied_index();
}

bool raft_log::has_more_entries_to_apply(uint64_t applied_to) const noexcept {
  return _committed > applied_to;
}

bool raft_log::has_config_change_to_apply() const noexcept {
  // TODO(jyc): avoid entry vector
  protocol::log_entry_vector entries;
  _in_memory.query(
      {.low = first_not_applied_index(), .high = UINT64_MAX},
      entries,
      UINT64_MAX);
  for (const auto& ent : entries) {
    if (ent->type == protocol::entry_type::config_change) {
      return true;
    }
  }
  return false;
}

bool raft_log::has_entries_to_save() const noexcept {
  return !_in_memory.get_entries_to_save().empty();
}

void raft_log::get_entries_to_save(protocol::log_entry_vector& entries) {
  auto ents = _in_memory.get_entries_to_save();
  entries.insert(entries.end(), ents.begin(), ents.end());
}

future<> raft_log::get_entries_to_apply(protocol::log_entry_vector& entries) {
  if (has_entries_to_apply()) {
    // TODO(jyc): configurable max_bytes for entries to apply
    co_await query(
        {first_not_applied_index(), apply_index_limit()}, entries, UINT64_MAX);
  }
  co_return;
}

future<size_t> raft_log::query(
    uint64_t start,
    protocol::log_entry_vector& entries,
    size_t max_bytes) const {
  if (start > last_index()) {
    co_return max_bytes;
  }
  co_return co_await query(
      {.low = start, .high = last_index() + 1}, entries, max_bytes);
}

future<size_t> raft_log::query(
    protocol::hint range,
    protocol::log_entry_vector& entries,
    size_t max_bytes) const {
  check_range(range);
  if (range.low == range.high) {
    co_return max_bytes;
  }
  entries.reserve(range.count());
  max_bytes = co_await query_logdb(range, entries, max_bytes);
  if (max_bytes > 0) {
    max_bytes = co_await query_memory(range, entries, max_bytes);
  }
  co_return max_bytes;
}

future<size_t> raft_log::query_logdb(
    protocol::hint range,
    protocol::log_entry_vector& entries,
    size_t max_bytes) const noexcept {
  if (range.low >= _in_memory._marker) {
    // all logs in question are in memory, directly return
    co_return max_bytes;
  }
  auto before_query = entries.size();
  auto high = std::min(range.high, _in_memory._marker);
  max_bytes = co_await _logdb.query({range.low, high}, entries, max_bytes);
  if (entries.size() - before_query == high - range.low) {
    // we have enough logs, avoid redundant searching in memory
    max_bytes = 0;
  }
  co_return max_bytes;
}

future<size_t> raft_log::query_memory(
    protocol::hint range,
    protocol::log_entry_vector& entries,
    size_t max_bytes) const noexcept {
  if (range.high <= _in_memory._marker) {
    co_return max_bytes;
  }
  auto low = std::max(range.low, _in_memory._marker);
  co_return _in_memory.query({low, range.high}, entries, max_bytes);
}

protocol::snapshot_ptr raft_log::get_snapshot() const noexcept {
  if (_in_memory.get_snapshot()) {
    return _in_memory.get_snapshot();
  }
  return _logdb.get_snapshot();
}

protocol::snapshot_ptr raft_log::get_memory_snapshot() const noexcept {
  return _in_memory.get_snapshot();
}

future<uint64_t> raft_log::get_conflict_index(
    protocol::log_entry_span entries) const {
  for (const auto& ent : entries) {
    if (!co_await term_index_match(ent->lid)) {
      co_return ent->lid.index;
    }
  }
  co_return protocol::log_id::INVALID_INDEX;
}

future<uint64_t> raft_log::pending_config_change_count() {
  uint64_t count = 0;
  uint64_t start_index = _committed + 1;
  protocol::log_entry_vector entries;
  while (true) {
    // TODO(jyc): refine max_bytes
    entries.clear();
    co_await query(start_index, entries, UINT64_MAX);
    if (entries.empty()) {
      co_return count;
    }
    count = std::accumulate(
        entries.begin(),
        entries.end(),
        count,
        [](uint64_t count, const auto& entry) {
          return count + (entry->type == protocol::entry_type::config_change);
        });
    start_index = entries.back()->lid.index;
  }
}

future<bool> raft_log::try_append(
    uint64_t index, protocol::log_entry_span entries) {
  auto conflict_index = co_await get_conflict_index(entries);
  if (conflict_index != protocol::log_id::INVALID_INDEX) {
    if (conflict_index <= _committed) {
      co_return coroutine::make_exception(
          util::failed_precondition_error(fmt::format(
              "conflict {} <= committed {}", conflict_index, _committed)));
    }
    append(entries.subspan(conflict_index - index - 1));
    co_return true;
  }
  co_return false;
}

void raft_log::append(protocol::log_entry_span entries) {
  if (entries.empty()) {
    return;
  }
  if (entries.front()->lid.index <= _committed) {
    throw util::failed_precondition_error(fmt::format(
        "append first {} <= committed {}",
        entries.front()->lid.index,
        _committed));
  }
  _in_memory.merge(entries);
}

future<bool> raft_log::try_commit(protocol::log_id lid) {
  if (lid.index <= _committed) {
    co_return false;
  }
  auto t = protocol::log_id::INVALID_TERM;
  try {
    t = co_await term(lid.index);
  } catch (util::compacted_error& e) {
  }
  if (lid.index > _committed && lid.term == t) {
    commit(lid.index);
    co_return true;
  }
  co_return false;
}

void raft_log::commit(uint64_t index) {
  if (index == _committed) {
    return;
  }
  if (index < _committed) {
    throw util::failed_precondition_error(
        fmt::format("commit index {} < committed {}", index, _committed));
  }
  if (index > last_index()) {
    throw util::failed_precondition_error(
        fmt::format("commit index {} > last index {}", index, last_index()));
  }
  _committed = index;
}

void raft_log::commit_update(const protocol::update_commit& uc) {
  _in_memory.advance(uc.stable_log_id, uc.stable_snapshot_to);
  if (uc.processed > 0) {
    if (uc.processed < _processed || uc.processed > _committed) {
      throw util::failed_precondition_error(fmt::format(
          "processed {} out of range [{},{}]",
          uc.processed,
          _processed,
          _committed));
    }
    _processed = uc.processed;
  }
  if (uc.last_applied > 0) {
    if (uc.last_applied > _committed) {
      throw util::failed_precondition_error(fmt::format(
          "last_applied {} > committed {}", uc.last_applied, _committed));
    }
    if (uc.last_applied > _processed) {
      throw util::failed_precondition_error(fmt::format(
          "last_applied {} > processed {}", uc.last_applied, _processed));
    }
    _in_memory.advance_applied_log(uc.last_applied);
  }
}

future<bool> raft_log::up_to_date(protocol::log_id lid) {
  auto li = last_index();
  auto lt = co_await term(li);
  co_return lid >= protocol::log_id{.term = lt, .index = li};
}

void raft_log::restore(protocol::snapshot_ptr snapshot) {
  _in_memory.restore(snapshot);
  if (snapshot->log_id.index < _committed) {
    throw util::failed_precondition_error(fmt::format(
        "restore snapshot index {} < committed {}",
        snapshot->log_id.index,
        _committed));
  }
  _committed = snapshot->log_id.index;
  _processed = snapshot->log_id.index;
}

void raft_log::check_range(protocol::hint range) const {
  if (range.low > range.high) {
    throw util::failed_precondition_error(
        fmt::format("invalid range [{},{})", range.low, range.high));
  }
  auto r = entry_range();
  if (r == protocol::hint{} || range.low < r.low) {
    throw util::compacted_error(range.low, r.low);
  }
  if (range.high > r.high + 1) {
    throw util::unavailable_error(range.high, r.high + 1);
  }
}

}  // namespace rafter::core
