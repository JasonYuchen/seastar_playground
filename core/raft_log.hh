//
// Created by jason on 2021/12/21.
//

#pragma once

#include <optional>

#include "protocol/raft.hh"

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
  void assertMarker() const;

  bool _shrunk = false;
  protocol::snapshot_ptr _snapshot;
  protocol::log_entry_vector _entries;
  uint64_t _marker;
  uint64_t _saved;
  protocol::log_id _applied;
};

class raft_log {
 public:
  raft_log();

 private:
  uint64_t _committed;
  uint64_t _processed;
  in_memory_log _in_memory;
};

}  // namespace rafter::core
