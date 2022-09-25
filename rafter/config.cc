//
// Created by jason on 2021/12/8.
//

#include "config.hh"

#include <yaml-cpp/yaml.h>

#include <fstream>
#include <seastar/core/smp.hh>

#include "protocol/raft.hh"
#include "util/error.hh"

namespace YAML {

template <>
struct convert<rafter::config> {
  static bool decode(const Node& node, rafter::config& cfg) {
    if (node["rtt_ms"]) {
      cfg.rtt_ms = node["rtt_ms"].as<uint64_t>();
    }
    if (node["data_dir"]) {
      cfg.data_dir = node["data_dir"].as<std::string>();
    }
    if (node["wal_rolling_size"]) {
      cfg.wal_rolling_size = node["wal_rolling_size"].as<uint64_t>();
    }
    if (node["wal_gc_queue_capacity"]) {
      cfg.wal_gc_queue_capacity = node["wal_gc_queue_capacity"].as<uint64_t>();
    }
    if (node["listen_address"]) {
      cfg.listen_address = node["listen_address"].as<std::string>();
    }
    if (node["listen_port"]) {
      cfg.listen_port = node["listen_port"].as<uint16_t>();
    }
    if (node["snapshot_chunk_size"]) {
      cfg.snapshot_chunk_size = node["snapshot_chunk_size"].as<uint64_t>();
    }
    if (node["in_memory_gc_timeout"]) {
      cfg.in_memory_gc_timeout = node["in_memory_gc_timeout"].as<uint64_t>();
    }
    if (node["max_entry_bytes"]) {
      cfg.max_entry_bytes = node["max_entry_bytes"].as<uint64_t>();
    }
    if (node["max_replicate_entry_bytes"]) {
      cfg.max_replicate_entry_bytes =
          node["max_replicate_entry_bytes"].as<uint64_t>();
    }
    if (node["max_apply_entry_bytes"]) {
      cfg.max_apply_entry_bytes = node["max_apply_entry_bytes"].as<uint64_t>();
    }
    if (node["incoming_proposal_queue_length"]) {
      cfg.incoming_proposal_queue_length =
          node["incoming_proposal_queue_length"].as<uint64_t>();
    }
    if (node["incoming_read_index_queue_length"]) {
      cfg.incoming_read_index_queue_length =
          node["incoming_read_index_queue_length"].as<uint64_t>();
    }
    if (node["task_queue_capacity"]) {
      cfg.task_queue_capacity = node["task_queue_capacity"].as<uint64_t>();
    }
    if (node["max_send_queue_bytes"]) {
      cfg.max_send_queue_bytes = node["max_send_queue_bytes"].as<uint64_t>();
    }
    if (node["max_receive_queue_bytes"]) {
      cfg.max_receive_queue_bytes =
          node["max_receive_queue_bytes"].as<uint64_t>();
    }
    if (node["lru_max_session_count"]) {
      cfg.lru_max_session_count = node["lru_max_session_count"].as<uint64_t>();
    }
    return true;
  }
};

template <>
struct convert<rafter::raft_config> {
  static Node encode(const rafter::raft_config& cfg) {
    Node node;
    node["cluster_id"] = cfg.cluster_id;
    node["node_id"] = cfg.node_id;
    node["election_rtt"] = cfg.election_rtt;
    node["heartbeat_rtt"] = cfg.heartbeat_rtt;
    node["snapshot_interval"] = cfg.snapshot_interval;
    node["compaction_overhead"] = cfg.compaction_overhead;
    node["max_in_memory_log_bytes"] = cfg.max_in_memory_log_bytes;
    node["check_quorum"] = cfg.check_quorum;
    node["observer"] = cfg.observer;
    node["witness"] = cfg.witness;
    node["quiesce"] = cfg.quiesce;
    return node;
  }
  static bool decode(const Node& node, rafter::raft_config& cfg) {
    if (node["cluster_id"]) {
      cfg.cluster_id = node["cluster_id"].as<uint64_t>();
    }
    if (node["node_id"]) {
      cfg.node_id = node["node_id"].as<uint64_t>();
    }
    if (node["election_rtt"]) {
      cfg.election_rtt = node["election_rtt"].as<uint64_t>();
    }
    if (node["heartbeat_rtt"]) {
      cfg.heartbeat_rtt = node["heartbeat_rtt"].as<uint64_t>();
    }
    if (node["snapshot_interval"]) {
      cfg.snapshot_interval = node["snapshot_interval"].as<uint64_t>();
    }
    if (node["compaction_overhead"]) {
      cfg.compaction_overhead = node["compaction_overhead"].as<uint64_t>();
    }
    if (node["max_in_memory_log_bytes"]) {
      cfg.max_in_memory_log_bytes =
          node["max_in_memory_log_bytes"].as<uint64_t>();
    }
    if (node["check_quorum"]) {
      cfg.check_quorum = node["check_quorum"].as<bool>();
    }
    if (node["observer"]) {
      cfg.observer = node["observer"].as<bool>();
    }
    if (node["witness"]) {
      cfg.witness = node["witness"].as<bool>();
    }
    if (node["quiesce"]) {
      cfg.quiesce = node["quiesce"].as<bool>();
    }
    return true;
  }
};

}  // namespace YAML

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

raft_config raft_config::read_from(std::istream& input) {
  return YAML::Load(input).as<raft_config>();
}

void raft_config::write_to(std::ostream& output) {
  YAML::Node node;
  node = *this;
  output << YAML::Dump(node);
}

std::ostream& operator<<(std::ostream& os, const raft_config& cfg) {
  os << "cluster_id: " << cfg.cluster_id << ", "
     << "node_id: " << cfg.node_id << ", "
     << "election_rtt: " << cfg.election_rtt << ", "
     << "heartbeat_rtt: " << cfg.heartbeat_rtt << ", "
     << "snapshot_interval: " << cfg.snapshot_interval << ", "
     << "compaction_overhead: " << cfg.compaction_overhead << ", "
     << "max_in_memory_log_bytes: " << cfg.max_in_memory_log_bytes << ", "
     << "check_quorum: " << cfg.check_quorum << ", "
     << "observer: " << cfg.observer << ", "
     << "witness: " << cfg.witness << ", "
     << "quiesce: " << cfg.quiesce;
  return os;
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

config config::read_from(std::istream& input) {
  return YAML::Load(input).as<config>();
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

const config& config::shard() { return *_config; }

config& config::mutable_shard() { return *_config; }

std::ostream& operator<<(std::ostream& os, const config& cfg) {
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
