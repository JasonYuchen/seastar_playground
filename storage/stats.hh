//
// Created by jason on 2021/12/12.
//

#pragma once

#include <stdint.h>

namespace rafter::storage {

struct stats {
  // # of segment_manager::append called
  uint64_t _append = 0;
  // # of snapshot appended
  uint64_t _append_snap = 0;
  // # of hard state appended
  uint64_t _append_state = 0;
  // # of entries appended
  uint64_t _append_entry = 0;
  // # of segment_manager::remove called
  uint64_t _remove = 0;
  // # of segment_manager::query_snapshot called
  uint64_t _query_snap = 0;
  // # of segment_manager::query_raft_state called
  uint64_t _query_state = 0;
  // # of segment_manager::query_entries called
  uint64_t _query_entry = 0;
  // # of actual fsync on segment called
  uint64_t _sync = 0;
  // # of created segment file
  uint64_t _new_segment = 0;
  // # of deleted segment file
  uint64_t _del_segment = 0;

  void operator+=(const stats& rhs);
};

}  // namespace rafter::storage
