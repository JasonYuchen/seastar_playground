//
// Created by jason on 2021/12/21.
//

#pragma once

#include <optional>

#include "protocol/raft.hh"
#include "storage/logdb.hh"
#include "util/seastarx.hh"

namespace rafter::test {

class core_helper;

}  // namespace rafter::test

namespace rafter::core {

class in_memory_log {
 public:
  explicit in_memory_log(uint64_t last_index);
  size_t query(
      protocol::hint range,
      protocol::log_entry_vector& entries,
      size_t max_bytes) const;
  protocol::log_entry_span get_entries_to_save() const noexcept;
  size_t get_entries_size() const noexcept { return _entries.size(); }
  protocol::snapshot_ptr get_snapshot() const noexcept;
  std::optional<uint64_t> get_snapshot_index() const noexcept;
  std::optional<uint64_t> get_last_index() const noexcept;
  std::optional<uint64_t> get_term(uint64_t index) const noexcept;
  void advance(
      protocol::log_id stable_log, uint64_t stable_snapshot_index) noexcept;
  void advance_saved_log(protocol::log_id saved_log) noexcept;
  void advance_saved_snapshot(uint64_t saved_snapshot_index) noexcept;
  void advance_applied_log(uint64_t applied_index);

  void merge(protocol::log_entry_span entries);
  void restore(protocol::snapshot_ptr snapshot) noexcept;

 private:
  friend class raft_log;
  friend class test::core_helper;
  void assert_marker() const;

  bool _shrunk = false;
  protocol::snapshot_ptr _snapshot;
  protocol::log_entry_vector _entries;
  uint64_t _marker;  // equal to the index of the first entry in _entries
  uint64_t _saved;
  protocol::log_id _applied;
};

class log_reader {
 public:
  log_reader(protocol::group_id gid, storage::logdb& logdb);
  protocol::hard_state get_state() const noexcept;
  void set_state(protocol::hard_state state) noexcept;
  protocol::membership_ptr get_membership() const noexcept;
  future<size_t> query(
      protocol::hint range,
      protocol::log_entry_vector& entries,
      size_t max_bytes);
  future<uint64_t> get_term(uint64_t index);
  protocol::hint get_range() const noexcept;
  void set_range(protocol::hint range);
  protocol::snapshot_ptr get_snapshot() const noexcept;
  void apply_snapshot(protocol::snapshot_ptr snapshot);
  void create_snapshot(protocol::snapshot_ptr snapshot);
  void apply_entries(protocol::log_entry_span entries);
  future<> apply_compaction(uint64_t index);

 private:
  friend class raft_log;
  friend class test::core_helper;
  uint64_t first_index() const noexcept { return _marker.index + 1; }
  uint64_t last_index() const noexcept { return _marker.index + _length - 1; }

  protocol::group_id _gid;
  storage::logdb& _logdb;
  protocol::snapshot_ptr _snapshot;
  protocol::hard_state _state;
  protocol::log_id _marker;
  uint64_t _length = 1;  // the _marker itself
};

class raft_log {
 public:
  raft_log(protocol::group_id gid, log_reader& log);
  uint64_t committed() const noexcept { return _committed; }
  void set_committed(uint64_t index) noexcept { _committed = index; }
  uint64_t processed() const noexcept { return _processed; }
  uint64_t first_index() const noexcept;
  uint64_t last_index() const noexcept;
  future<uint64_t> term(uint64_t index) const;
  future<uint64_t> last_term() const;
  future<bool> term_index_match(protocol::log_id lid) const;
  // the available entries' term range + snapshot's term
  protocol::hint term_entry_range() const noexcept;
  // the available entries' range
  protocol::hint entry_range() const noexcept;
  uint64_t first_not_applied_index() const noexcept;
  uint64_t apply_index_limit() const noexcept;
  bool has_entries_to_apply() const noexcept;
  bool has_more_entries_to_apply(uint64_t applied_to) const noexcept;
  bool has_config_change_to_apply() const noexcept;
  bool has_entries_to_save() const noexcept;
  void get_entries_to_save(protocol::log_entry_vector& entries);
  future<> get_entries_to_apply(protocol::log_entry_vector& entries);
  future<size_t> query(
      uint64_t start,
      protocol::log_entry_vector& entries,
      size_t max_bytes) const;
  future<size_t> query(
      protocol::hint range,
      protocol::log_entry_vector& entries,
      size_t max_bytes) const;
  future<size_t> query_logdb(
      protocol::hint range,
      protocol::log_entry_vector& entries,
      size_t max_bytes) const noexcept;
  future<size_t> query_memory(
      protocol::hint range,
      protocol::log_entry_vector& entries,
      size_t max_bytes) const noexcept;
  protocol::snapshot_ptr get_snapshot() const noexcept;
  protocol::snapshot_ptr get_memory_snapshot() const noexcept;
  future<uint64_t> get_conflict_index(protocol::log_entry_span entries) const;
  future<uint64_t> pending_config_change_count();
  future<bool> try_append(uint64_t index, protocol::log_entry_span entries);
  void append(protocol::log_entry_span entries);
  future<bool> try_commit(protocol::log_id lid);
  void commit(uint64_t index);
  void commit_update(const protocol::update_commit& uc);
  future<bool> up_to_date(protocol::log_id lid);
  void restore(protocol::snapshot_ptr snapshot);

 private:
  friend class test::core_helper;
  void check_range(protocol::hint range) const;

  protocol::group_id _gid;
  uint64_t _committed;
  // The last index known to be pushed to rsm for execution,
  // not the last index confirmed to be executed by rsm
  uint64_t _processed;
  in_memory_log _in_memory;
  log_reader& _logdb;
};

}  // namespace rafter::core
