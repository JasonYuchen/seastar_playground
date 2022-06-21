//
// Created by jason on 2021/12/8.
//

#pragma once

#include <stdint.h>

#include <seastar/core/future.hh>
#include <seastar/core/units.hh>
#include <string>

namespace rafter {

using seastar::GB;
using seastar::KB;
using seastar::MB;

struct raft_config {
  // cluster_id is used to uniquely identify a Raft cluster.
  uint64_t cluster_id = 0;

  // node_id is used to uniquely identify a node within a specific Raft cluster.
  uint64_t node_id = 0;

  // election_rtt is the minimum number of RTTs between elections. Message RTT
  // is defined by config.rtt_ms. The actual interval between elections is
  // randomized to be between election_rtt and 2 * election_rtt.
  uint64_t election_rtt = 0;

  // heartbeat_rtt is the number of RTTs between heartbeats. Message RTT is
  // defined by config.rtt_ms. The Raft paper suggest the heartbeat interval to
  // be close to the average RTT between nodes.
  uint64_t heartbeat_rtt = 0;

  // snapshot_interval defines how often the state machine should be snapshotted
  // automatically. It is defined in terms of the number of applied Raft log
  // entries. snapshot_interval can be set to 0 to disable such automatic
  // snapshotting.
  uint64_t snapshot_interval = 0;

  // compaction_overhead defines the number of most recent entries to keep after
  // each Raft log compaction. Raft log compaction is performed automatically
  // every time when a snapshot is created.
  uint64_t compaction_overhead = 0;

  // the max bytes allowed for storing in memory raft logs
  uint64_t max_in_memory_log_bytes = UINT64_MAX;

  // check_quorum specifies whether the leader node should periodically check
  // non-leader node status and step down to become a follower node when it no
  // longer has the quorum.
  bool check_quorum = false;

  bool observer = false;

  bool witness = false;

  bool quiesce = false;

  void validate() const;
};

struct snapshot_option {
  // export_path is the path where the exported snapshot should be stored, it
  // must point to an existing directory for which the current user has write
  // permission.
  std::string export_path;

  // compaction_overhead is the compaction overhead value to use for the
  // requested snapshot operation. This field is ignored when set to 0 or
  // exporting a snapshot. A non-zero value will override the raft_config's
  // compaction_overhead.
  uint64_t compaction_overhead = 0;
};

// config file is shared among all shards
// TODO(jyc): refactor the shard-wide configuration referring to the
//  https://github.com/scylladb/scylla/blob/master/utils/config_file.hh

struct config;

template <typename T>
class named_value {
 public:
 private:
  config* _c;
  std::string_view _name;
  std::string_view _desc;
  T _value;
};

struct config {
  // TODO(jyc): add scheduling group configuration here

  // rtt_ms defines the average RTT in milliseconds between two node_host.  It
  // is internally used as a logical clock tick, Raft heartbeat and election
  // intervals are both defined in terms of logical clock ticks (RTT intervals).
  uint64_t rtt_ms = 0;

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

  // the interval between gc in memory logs, defined as the number of ticks
  uint64_t in_memory_gc_timeout = 10;

  // soft
  // the max bytes of a single entry including entry's metadata.
  uint64_t max_entry_bytes = 8UL * MB;

  // the max bytes of entries that can be included in a single replicate message
  // should be greater than max_entry_bytes.
  uint64_t max_replicate_entry_bytes = 64UL * MB;

  // the max bytes of entries that can be applied at once
  // should be greater than max_entry_bytes.
  uint64_t max_apply_entry_bytes = 64UL * MB;

  // the number of allowed pending proposals for a raft instance.
  uint64_t incoming_proposal_queue_length = 2048;

  // the number of allowed pending reads for a raft instance.
  uint64_t incoming_read_index_queue_length = 4096;

  // the number of allowed tasks in the task queue (e.g. rsm apply queue)
  uint64_t task_queue_capacity = 64;

  // max_send_queue_bytes is the maximum size in bytes of each send queue.
  // When set to 0, it means the send queue size is unlimited.
  uint64_t max_send_queue_bytes = 0;

  // max_receive_queue_bytes is the maximum size in bytes of each receive queue.
  // When set to 0, it means the receiving queue's size is unlimited.
  uint64_t max_receive_queue_bytes = 0;

  // hard
  // lru_max_session_count is the max number of client sessions that can be
  // concurrently held and managed by each raft cluster.
  uint64_t lru_max_session_count = 4096;

  void validate() const;

  // initialize the config on shard 0 with default value
  static void initialize();
  // initialize the config on shard 0 with given value
  static void initialize(const config& init);
  // broadcast the shard 0's config to all shards
  static seastar::future<> broadcast();
  static seastar::future<> read_from(std::string_view file);
  static const config& shard();
  static config& mutable_shard();

 private:
  static inline thread_local std::unique_ptr<config> _config;
};

std::ostream& operator<<(std::ostream& os, config cfg);

}  // namespace rafter
