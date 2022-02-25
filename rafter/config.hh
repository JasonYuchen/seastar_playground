//
// Created by jason on 2021/12/8.
//

#pragma once

#include <stdint.h>

#include <string>

#include <seastar/core/units.hh>

#include "protocol/raft.hh"

namespace rafter {

using seastar::GB;
using seastar::KB;
using seastar::MB;

struct raft_config {
  protocol::group_id group_id;

  void validate() const;
};

// config file is shared among all shards
struct config {
  // TODO(jyc): add scheduling group configuration here
  // the absolute path of a directory for data (WAL, snapshot, etc) storage
  std::string data_dir;
  // the rolling threshold size in bytes for segment files
  uint64_t wal_rolling_size = 1UL * GB;
  // the max capacity of the queue holding out-dated segment files
  uint64_t wal_gc_queue_capacity = 100;
  // the listening address for raft messages
  std::string listen_address = "::";
  // the listening port for raft messages
  uint16_t listen_port = 10615;

  // the snapshot chunk size
  uint64_t snapshot_chunk_size = 1UL * MB;

  // the max bytes allowed for storing in memory logs
  uint64_t max_in_memory_log_bytes = UINT64_MAX;

  // the interval between gc in memory logs, defined as the number of ticks
  uint64_t in_memory_gc_timeout = 10;

  void validate() const;
  // TODO(jyc): read from yaml?
};

}  // namespace rafter
