//
// Created by jason on 2021/12/21.
//

#pragma once

#include <optional>

#include "protocol/raft.hh"
#include "storage/segment_manager.hh"

namespace rafter::core {

class in_memory_log {
 public:
  explicit in_memory_log(uint64_t last_index);
  protocol::log_entry_span query(protocol::hint range) const;
  protocol::log_entry_span get_entries_to_save() const;
  std::optional<uint64_t> get_snapshot_index() const;
  std::optional<uint64_t> get_last_index() const;
  std::optional<uint64_t> get_term(uint64_t index) const;
  void advance(protocol::log_id stable_log, uint64_t stable_snapshot_index);
  void advance_saved_log(protocol::log_id saved_log);
  void advance_saved_snapshot(uint64_t saved_snapshot_index);
  void advance_applied_log(uint64_t applied_index);

  void merge(protocol::log_entry_span entries);
  void restore(protocol::snapshot_ptr snapshot);

 private:
  void assert_marker() const;

  bool _shrunk = false;
  protocol::snapshot_ptr _snapshot;
  protocol::log_entry_vector _entries;
  uint64_t _marker;
  uint64_t _saved;
  protocol::log_id _applied;
};

class log_reader {
 public:
  log_reader(protocol::group_id gid, storage::segment_manager& log);
  protocol::hard_state get_state() const noexcept;
  void set_state(protocol::hard_state state);
  protocol::membership_ptr get_membership() const noexcept;
  seastar::future<> query(
      protocol::hint range,
      protocol::log_entry_vector& entries,
      size_t max_size);
  seastar::future<uint64_t> get_term(uint64_t index);
  protocol::hint get_range() const noexcept;
  void set_range(protocol::hint range);
  protocol::snapshot_ptr get_snapshot() const noexcept;
  void apply_snapshot(protocol::snapshot_ptr snapshot);
  void apply_entries(protocol::log_entry_span entries);
  seastar::future<> apply_compaction(uint64_t index);

 private:
  uint64_t first_index() const noexcept { return _marker.index + 1; }
  uint64_t last_index() const noexcept { return _marker.index + _length - 1; }

  protocol::group_id _gid;
  storage::segment_manager& _log;
  protocol::snapshot_ptr _snapshot;
  protocol::hard_state _state;
  protocol::log_id _marker;
  uint64_t _length = 1;  // the _marker itself
};

class raft_log {
 public:
  raft_log(protocol::group_id gid, log_reader& log, uint64_t last_index);

 private:
  void assert_range(protocol::hint range) const;

  protocol::group_id _gid;
  uint64_t _committed;
  uint64_t _processed;
  in_memory_log _in_memory;
  log_reader& _logdb;
};

}  // namespace rafter::core
