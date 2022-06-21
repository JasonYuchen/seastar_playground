//
// Created by jason on 2021/12/8.
//

#include "config.hh"

#include <seastar/core/smp.hh>

#include "protocol/raft.hh"
#include "util/error.hh"

namespace rafter {

using namespace protocol;
using namespace seastar;

void raft_config::validate() const {
  if (cluster_id == group_id::INVALID_CLUSTER) {
    throw util::configuration_error("cluster_id", "invalid");
  }
  if (node_id == group_id::INVALID_NODE) {
    throw util::configuration_error("node_id", "invalid");
  }
  if (election_rtt == 0) {
    throw util::configuration_error("election_rtt", "invalid");
  }
  if (heartbeat_rtt == 0) {
    throw util::configuration_error("heartbeat_rtt", "invalid");
  }
  if (election_rtt < 5 * heartbeat_rtt) {
    throw util::configuration_error("election_rtt", "too small");
  }
  if (observer && witness) {
    throw util::configuration_error("witness", "observer cannot be an witness");
  }
}

void config::validate() const {
  if (rtt_ms == 0) {
    throw util::configuration_error("rtt_ms", "invalid");
  }
  if (data_dir.empty()) {
    throw util::configuration_error("data_dir", "empty");
  }
}

void config::initialize() { initialize({}); }

void config::initialize(const config& init) {
  if (this_shard_id() != 0) {
    throw util::configuration_error("config", "must be initialized on shard 0");
  }
  if (_config) {
    throw util::configuration_error("config", "already initialized");
  }
  _config = std::make_unique<config>(init);
}

future<> config::broadcast() {
  if (!_config) {
    throw util::configuration_error("config", "not initialized");
  }
  return smp::invoke_on_all([&cfg = std::as_const(*_config)] {
    if (this_shard_id() != 0) {
      _config = std::make_unique<config>(cfg);
    }
    return make_ready_future<>();
  });
}

future<> config::read_from(std::string_view file) { throw "NOT IMPLEMENTED"; }

const config& config::shard() { return *_config; }

config& config::mutable_shard() { return *_config; }

std::ostream& operator<<(std::ostream& os, config cfg) {
  os << "rtt_ms: " << cfg.rtt_ms << ", "
     << "data_dir: " << cfg.data_dir << ", "
     << "wal_rolling_size: " << cfg.wal_rolling_size << ", "
     << "wal_gc_queue_capacity: " << cfg.wal_gc_queue_capacity << ", "
     << "listen_address: " << cfg.listen_address << ", "
     << "listen_port: " << cfg.listen_port << ", "
     << "snapshot_chunk_size: " << cfg.snapshot_chunk_size << ", "
     << "in_memory_gc_timeout: " << cfg.in_memory_gc_timeout << ", "
     << "max_entry_bytes: " << cfg.max_entry_bytes << ", "
     << "max_replicate_entry_bytes: " << cfg.max_replicate_entry_bytes << ", "
     << "max_apply_entry_bytes: " << cfg.max_apply_entry_bytes << ", "
     << "incoming_proposal_queue_length: " << cfg.incoming_proposal_queue_length
     << ", "
     << "incoming_read_index_queue_length: "
     << cfg.incoming_read_index_queue_length << ", "
     << "task_queue_capacity: " << cfg.task_queue_capacity << ", "
     << "max_send_queue_bytes: " << cfg.max_send_queue_bytes << ", "
     << "max_receive_queue_bytes: " << cfg.max_receive_queue_bytes << ", "
     << "lru_max_session_count: " << cfg.lru_max_session_count << ", ";
  return os;
}

}  // namespace rafter
