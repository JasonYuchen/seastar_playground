//
// Created by jason on 2021/9/21.
//

#pragma once

#include <stdint.h>

#include <seastar/core/shared_ptr.hh>
#include <string>

namespace rafter::protocol {

struct group_id {
  inline static constexpr uint64_t invalid_cluster = 0;
  inline static constexpr uint64_t invalid_node = 0;
  uint64_t cluster = invalid_cluster;
  uint64_t node = invalid_node;
};

struct log_id {
  inline static constexpr uint64_t invalid_term = 0;
  inline static constexpr uint64_t invalid_index = 0;
  uint64_t term = invalid_term;
  uint64_t index = invalid_index;
  std::strong_ordering operator<=>(const log_id &) const = default;
};

enum class message_type : uint8_t {
  noop,
  local_tick,
  election,
  leader_heartbeat,
  config_change,
  propose,
  replicate,
  replicate_resp,
  read_index,
  read_index_resp,
  heartbeat,
  heartbeat_resp,
  request_vote,
  request_vote_resp,
  request_prevote,
  request_prevote_resp,
  install_snapshot,
  snapshot_status,
  snapshot_received,
  leader_transfer,
  timeout_now,
  unreachable,
  quiesce,
  check_quorum,
  num_of_type,
};

const char *name(enum message_type type);

enum class entry_type : uint8_t {
  application,
  config_change,
  encoded,
  metadata,
  num_of_type,
};

const char *name(enum entry_type type);

enum class config_change_type : uint8_t {
  add_node,
  remove_node,
  add_observer,
  add_witness,
  num_of_type,
};

const char *name(enum config_change_type type);

enum class state_machine_type : uint8_t {
  regular,
  num_of_type,
};

const char *name(enum state_machine_type type);

enum class compression_type : uint8_t {
  no_compression,
  lz4,
  snappy,
  num_of_type,
};

const char *name(enum compression_type type);

enum class checksum_type : uint8_t {
  no_checksum,
  crc32,
  highway,
  num_of_type,
};

const char *name(enum checksum_type type);

struct bootstrap {
  std::unordered_map<uint64_t, std::string> addresses;
  bool join = false;
  state_machine_type smtype = state_machine_type::regular;
};

using bootstrap_ptr = seastar::lw_shared_ptr<bootstrap>;

struct membership {
  uint64_t config_change_id = 0;
  std::unordered_map<uint64_t, std::string> addresses;
  std::unordered_map<uint64_t, std::string> observers;
  std::unordered_map<uint64_t, std::string> witnesses;
  std::unordered_map<uint64_t, bool> removed;
};

using membership_ptr = seastar::lw_shared_ptr<membership>;

struct log_entry {
  log_id id;
  entry_type type = entry_type::application;
  uint64_t key;
  uint64_t client_id;
  uint64_t series_id;
  uint64_t responded_to;
  std::string payload;

  bool is_proposal() const noexcept;
  bool is_config_change() const noexcept;
  bool is_session_managed() const noexcept;
  bool is_new_session_request() const noexcept;
  bool is_end_session_request() const noexcept;
  bool is_noop_session() const noexcept;
  bool is_empty() const noexcept;
  bool is_regular() const noexcept;
};

using log_entry_ptr = seastar::lw_shared_ptr<log_entry>;
using log_entry_vector = std::vector<log_entry_ptr>;

struct hard_state {
  uint64_t term = log_id::invalid_term;
  uint64_t vote = group_id::invalid_node;
  uint64_t commit = log_id::invalid_index;

  bool is_empty() const noexcept;
};

struct snapshot_file {
  uint64_t file_id = 0;
  uint64_t file_size = 0;
  std::string file_path;
  std::string metadata;
};

using snapshot_file_ptr = seastar::lw_shared_ptr<snapshot_file>;

struct snapshot {
  struct group_id group_id;
  struct log_id log_id;
  std::string file_path;
  uint64_t file_size = 0;
  membership_ptr membership;
  std::vector<snapshot_file_ptr> files;
  state_machine_type smtype = state_machine_type::regular;
  bool imported = false;
  bool witness = false;
  bool dummy = false;
  uint64_t on_disk_index = 0;
};

using snapshot_ptr = seastar::lw_shared_ptr<snapshot>;

struct hint {
  uint64_t low = 0;
  uint64_t high = 0;
};

struct message {
  message_type type = message_type::noop;
  uint64_t cluster = group_id::invalid_cluster;
  uint64_t from = group_id::invalid_node;
  uint64_t to = group_id::invalid_node;
  uint64_t term = log_id::invalid_term;
  uint64_t log_term = log_id::invalid_term;
  uint64_t log_index = log_id::invalid_index;
  uint64_t commit = log_id::invalid_index;
  // replicate messages sent to witness will only include the Entry.Index and
  // the Entry.Term with Entry.Type=Metadata, other fields will be ignored
  // (except for the ConfigChange entries)
  bool witness = false;
  bool reject = false;
  struct hint hint;
  std::vector<log_entry_ptr> entries;
  snapshot_ptr snapshot;

  bool is_request() const noexcept;
  bool is_response() const noexcept;
  bool is_leader_message() const noexcept;
  bool is_local_message() const noexcept;
};

using message_ptr = seastar::lw_shared_ptr<message>;

struct message_batch {
  uint64_t deployment_id = 0;
  std::string source_address;
  std::vector<message_ptr> messages;
};

struct config_change {
  uint64_t config_change_id = 0;
  config_change_type type = config_change_type::add_node;
  uint64_t node = 0;
  std::string address;
  bool initialize = false;
};

struct snapshot_header {
  uint64_t session_size = 0;
  uint64_t datastore_size = 0;
  uint32_t header_checksum = 0;
  uint32_t payload_checksum = 0;
  enum checksum_type checksum_type = checksum_type::crc32;
  enum compression_type compression_type = compression_type::no_compression;
};

struct snapshot_chunk {
  uint64_t deployment_id = 0;
  struct group_id group_id;
  struct log_id log_id;
  uint64_t from = group_id::invalid_node;
  uint64_t id = 0;
  uint64_t size = 0;
  uint64_t count = 0;
  std::string data;
  membership_ptr membership;
  std::string file_path;
  uint64_t file_size = 0;
  uint64_t file_chunk_id = 0;
  uint64_t file_chunk_count = 0;
  snapshot_file_ptr file_info;
  uint64_t on_disk_index = log_id::invalid_index;
  bool witness= false;
};

struct ready_to_read {
  uint64_t index = log_id::invalid_index;
  struct hint context;
};

// UpdateCommit is used to describe how to commit the Update instance to
// progress the state of Raft.
struct update_commit {
  // The last index known to be pushed to rsm for execution.
  uint64_t processed = log_id::invalid_index;
  // The last index confirmed to be executed by rsm.
  uint64_t last_applied = log_id::invalid_index;
  struct log_id stable_log_id;
  uint64_t stable_snapshot_to = log_id::invalid_index;
  uint64_t ready_to_read = log_id::invalid_index;
};

// Update is a collection of state, entries and messages that are expected to be
// processed by Raft's upper layer to progress the Raft node modelled as a state
// machine.
struct update {
  struct group_id group_id;
  // The current persistent state of a Raft node. It must be stored onto
  // persistent storage before any non-replication can be sent to other nodes.
  struct hard_state state;
  // Whether CommittedEntries can be applied without waiting for the Update
  // to be persisted to disk.
  bool fast_apply = false;

};

}  // namespace rafter::protocol
