//
// Created by jason on 2021/9/21.
//

#pragma once

#include <fmt/ostream.h>

#include <any>
#include <optional>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <span>
#include <string>
#include <vector>

namespace seastar {

// FIXME(jyc)
inline std::strong_ordering operator<=>(
    const seastar::temporary_buffer<char> &lhs,
    const seastar::temporary_buffer<char> &rhs) {
  return std::string_view{lhs.get(), lhs.size()} <=>
         std::string_view{rhs.get(), rhs.size()};
}

}  // namespace seastar

namespace rafter::protocol {

// TODO(jyc): separate data structures into different files

enum class raft_role : uint8_t {
  follower,
  pre_candidate,
  candidate,
  leader,
  observer,
  witness,
  num_of_role,
};

std::string_view name(enum raft_role role);
inline std::ostream &operator<<(std::ostream &os, raft_role role) {
  return os << name(role);
}

struct group_id {
  inline static constexpr uint64_t INVALID_CLUSTER = 0;
  inline static constexpr uint64_t INVALID_NODE = 0;

  uint64_t cluster = INVALID_CLUSTER;
  uint64_t node = INVALID_NODE;

  bool valid() const noexcept;

  static constexpr uint64_t bytes() noexcept { return 16; }

  std::strong_ordering operator<=>(const group_id &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const group_id &id);
};

struct log_id {
  inline static constexpr uint64_t INVALID_TERM = 0;
  inline static constexpr uint64_t INVALID_INDEX = 0;

  uint64_t term = INVALID_TERM;
  uint64_t index = INVALID_INDEX;

  static constexpr uint64_t bytes() noexcept { return 16; }

  std::strong_ordering operator<=>(const log_id &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const log_id &id);
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
  rate_limit,
  num_of_type,
};

inline bool is_prevote(message_type type) {
  return type == message_type::request_prevote ||
         type == message_type::request_prevote_resp;
}

inline bool is_request_vote(message_type type) {
  return type == message_type::request_vote ||
         type == message_type::request_prevote;
}

inline bool is_request(message_type type) {
  return type == message_type::propose || type == message_type::read_index ||
         type == message_type::leader_transfer;
}

inline bool is_leader(message_type type) {
  return type == message_type::replicate ||
         type == message_type::install_snapshot ||
         type == message_type::heartbeat || type == message_type::timeout_now ||
         type == message_type::read_index_resp;
}

inline bool is_local(message_type type) {
  return type == message_type::election ||
         type == message_type::leader_heartbeat ||
         type == message_type::unreachable ||
         type == message_type::snapshot_status ||
         type == message_type::check_quorum || type == message_type::local_tick;
}

inline bool is_response(message_type type) {
  return type == message_type::replicate_resp ||
         type == message_type::request_vote_resp ||
         type == message_type::request_prevote_resp ||
         type == message_type::heartbeat_resp ||
         type == message_type::read_index_resp ||
         type == message_type::unreachable ||
         type == message_type::snapshot_status ||
         type == message_type::leader_transfer;
}

std::string_view name(enum message_type type);
inline std::ostream &operator<<(std::ostream &os, message_type type) {
  return os << name(type);
}

enum class entry_type : uint8_t {
  application,
  config_change,
  encoded,
  metadata,
  num_of_type,
};

std::string_view name(enum entry_type type);
inline std::ostream &operator<<(std::ostream &os, entry_type type) {
  return os << name(type);
}

enum class config_change_type : uint8_t {
  add_node,
  remove_node,
  add_observer,
  add_witness,
  num_of_type,
};

std::string_view name(enum config_change_type type);
inline std::ostream &operator<<(std::ostream &os, config_change_type type) {
  return os << name(type);
}

enum class state_machine_type : uint8_t {
  regular,
  num_of_type,
};

std::string_view name(enum state_machine_type type);
inline std::ostream &operator<<(std::ostream &os, state_machine_type type) {
  return os << name(type);
}

enum class compression_type : uint8_t {
  no_compression,
  lz4,
  snappy,
  num_of_type,
};

std::string_view name(enum compression_type type);
inline std::ostream &operator<<(std::ostream &os, compression_type type) {
  return os << name(type);
}

enum class checksum_type : uint8_t {
  no_checksum,
  crc32,
  highway,
  num_of_type,
};

std::string_view name(enum checksum_type type);
inline std::ostream &operator<<(std::ostream &os, checksum_type type) {
  return os << name(type);
}

using member_map = std::unordered_map<uint64_t, std::string>;

struct bootstrap {
  member_map addresses;
  bool join = false;
  state_machine_type smtype = state_machine_type::regular;

  uint64_t bytes() const noexcept;

  bool operator==(const bootstrap &rhs) const noexcept;
  bool operator!=(const bootstrap &rhs) const noexcept;
};

using bootstrap_ptr = seastar::lw_shared_ptr<bootstrap>;

struct membership {
  uint64_t config_change_id = 0;
  member_map addresses;
  member_map observers;
  member_map witnesses;
  std::unordered_map<uint64_t, bool> removed;

  uint64_t bytes() const noexcept;

  bool operator==(const membership &rhs) const noexcept;
  bool operator!=(const membership &rhs) const noexcept;
};

using membership_ptr = seastar::lw_shared_ptr<membership>;

struct log_entry {
  log_entry() = default;
  explicit log_entry(log_id lid) : lid(lid) {}
  log_entry(uint64_t term, uint64_t index) : lid({term, index}) {}
  log_entry(const log_entry &e) = delete;
  log_entry &operator=(const log_entry &e) = delete;
  log_entry(log_entry &&e) = default;
  log_entry &operator=(log_entry &&e) = default;

  // return a new log entry with cloned payload
  log_entry clone();
  // return a new log entry with shared payload
  log_entry share();
  void copy_of(std::string_view data) {
    payload = seastar::temporary_buffer<char>::copy_of(data);
  }

  log_id lid;
  entry_type type = entry_type::application;
  uint64_t key = 0;
  uint64_t client_id = 0;
  uint64_t series_id = 0;
  uint64_t responded_to = 0;
  seastar::temporary_buffer<char> payload;

  uint64_t bytes() const noexcept;
  uint64_t in_memory_bytes() const noexcept;
  bool is_proposal() const noexcept;
  bool is_config_change() const noexcept;
  bool is_noop() const noexcept;
  bool is_session_managed() const noexcept;
  bool is_new_session_request() const noexcept;
  bool is_end_session_request() const noexcept;
  bool is_noop_session() const noexcept;
  bool is_empty() const noexcept;
  bool is_regular() const noexcept;

  static std::vector<log_entry> share(std::span<log_entry> entries);
  static uint64_t in_memory_bytes(std::span<log_entry> entries) noexcept;

  std::strong_ordering operator<=>(const log_entry &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const log_entry &entry);
};

using log_entry_vector = std::vector<log_entry>;
using log_entry_span = std::span<log_entry>;

struct hard_state {
  uint64_t term = log_id::INVALID_TERM;
  uint64_t vote = group_id::INVALID_NODE;
  uint64_t commit = log_id::INVALID_INDEX;

  static constexpr uint64_t bytes() noexcept { return 24; }
  bool empty() const noexcept;

  std::strong_ordering operator<=>(const hard_state &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const hard_state &state);
};

struct snapshot_file {
  uint64_t file_id = 0;
  uint64_t file_size = 0;
  std::string file_path;
  std::string metadata;

  uint64_t bytes() const noexcept;
  std::string filename();

  std::strong_ordering operator<=>(const snapshot_file &) const = default;
};

using snapshot_file_ptr = seastar::lw_shared_ptr<snapshot_file>;
using snapshot_files = std::vector<snapshot_file_ptr>;

struct snapshot {
  struct group_id group_id;
  struct log_id log_id;
  std::string file_path;
  uint64_t file_size = 0;
  membership_ptr membership;
  snapshot_files files;
  state_machine_type smtype = state_machine_type::regular;
  bool imported = false;
  bool witness = false;
  bool dummy = false;
  uint64_t on_disk_index = 0;

  uint64_t bytes() const noexcept;
  bool empty() const noexcept;
};

using snapshot_ptr = seastar::lw_shared_ptr<snapshot>;

struct hint {
  uint64_t low = 0;   // inclusive
  uint64_t high = 0;  // exclusive
  uint64_t count() const noexcept { return high - low; }

  static constexpr uint64_t bytes() noexcept { return 16; }

  std::strong_ordering operator<=>(const hint &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const hint &h);
};

using hint_vector = std::vector<hint>;

struct message {
  message_type type = message_type::noop;
  uint64_t cluster = group_id::INVALID_CLUSTER;
  uint64_t from = group_id::INVALID_NODE;
  uint64_t to = group_id::INVALID_NODE;
  // term is the term of the raft node.
  uint64_t term = log_id::INVALID_TERM;
  // lid{term, index} are the previous log term/index.
  struct log_id lid;
  // commit is the log committed value (leader commit).
  uint64_t commit = log_id::INVALID_INDEX;
  // replicate messages sent to witness will only include the Entry.Index and
  // the Entry.Term with Entry.Type=Metadata, other fields will be ignored
  // (except for the ConfigChange entries)
  bool witness = false;
  bool reject = false;
  struct hint hint;
  log_entry_vector entries;
  snapshot_ptr snapshot;

  bool is_request() const noexcept;
  bool is_response() const noexcept;
  bool is_leader_message() const noexcept;
  bool is_local_message() const noexcept;
  uint64_t bytes() const noexcept;

  message share();
  static std::vector<message> share(std::span<message> messages);
};

using message_ptr = seastar::lw_shared_ptr<message>;
using message_vector = std::vector<message>;

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

  uint64_t bytes() const noexcept;
};

struct snapshot_header {
  uint64_t session_size = 0;
  uint64_t datastore_size = 0;
  uint32_t header_checksum = 0;
  uint32_t payload_checksum = 0;
  enum checksum_type checksum_type = checksum_type::crc32;
  enum compression_type compression_type = compression_type::no_compression;

  static constexpr uint64_t bytes() noexcept { return 26; }
};

struct snapshot_chunk {
  uint64_t deployment_id = 0;
  struct group_id group_id;
  struct log_id log_id;
  uint64_t from = group_id::INVALID_NODE;
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
  uint64_t on_disk_index = log_id::INVALID_INDEX;
  bool witness = false;

  uint64_t bytes() const noexcept;
  bool is_last_chunk() const noexcept { return id + 1 == count; }
  bool is_last_file_chunk() const noexcept {
    return file_chunk_id + 1 == file_chunk_count;
  }
};

using snapshot_chunk_ptr = seastar::lw_shared_ptr<snapshot_chunk>;

struct ready_to_read {
  uint64_t index = log_id::INVALID_INDEX;
  struct hint context;

  static constexpr uint64_t bytes() noexcept { return 8 + hint::bytes(); }
};

using ready_to_read_vector = std::vector<ready_to_read>;

// update_commit is used to describe how to commit the update instance to
// progress the state of Raft.
struct update_commit {
  // The last index known to be pushed to rsm for execution.
  uint64_t processed = log_id::INVALID_INDEX;
  // The last index confirmed to be executed by rsm.
  uint64_t last_applied = log_id::INVALID_INDEX;
  struct log_id stable_log_id;
  uint64_t stable_snapshot_to = log_id::INVALID_INDEX;
  uint64_t ready_to_read = log_id::INVALID_INDEX;
};

// update is a collection of state, entries and messages that are expected to be
// processed by Raft's upper layer to progress the Raft node modelled as a state
// machine.
struct update {
  struct group_id gid;
  // The current persistent state of a Raft node. It must be stored onto
  // persistent storage before any non-replication can be sent to other nodes.
  struct hard_state state;
  // entries_to_save are entries waiting to be stored onto persistent storage.
  // first_index is the first index of the entries_to_save
  // last_index is the last index of the entries_to_save
  uint64_t first_index = log_id::INVALID_INDEX;
  uint64_t last_index = log_id::INVALID_INDEX;
  uint64_t snapshot_index = log_id::INVALID_INDEX;
  log_entry_vector entries_to_save;
  // snapshot is the metadata of the snapshot ready to be applied.
  snapshot_ptr snapshot;
  // Whether CommittedEntries can be applied without waiting for the Update
  // to be persisted to disk.
  bool fast_apply = false;
  // committed_entries are entries already committed in Raft and ready to be
  // applied by rsm.
  log_entry_vector committed_entries;
  // dropped_entries is a list of entries dropped when no leader is available.
  log_entry_vector dropped_entries;
  // Whether there are more committed entries ready to be applied.
  bool has_more_committed_entries = false;
  // ready_to_reads provides a list of ReadIndex requests ready for local read.
  ready_to_read_vector ready_to_reads;
  // messages is a collection of outgoing messages to be sent to remote nodes.
  // As stated above, replication messages can be immediately sent, all other
  // messages must be sent after the persistent state and entries are saved
  // onto persistent storage.
  message_vector messages;
  // last_applied is the actual last applied index reported by the rsm.
  uint64_t last_applied = log_id::INVALID_INDEX;
  // update_commit contains info on how the Update instance can be committed
  // to actually progress the state of Raft.
  struct update_commit update_commit;
  // dropped_read_indexes is a list of read index requests dropped when no
  // leader is available.
  hint_vector dropped_read_indexes;

  // if this update is a compaction update, return compactedTo, otherwise return
  // log_id::invalid_index
  void fill_meta() noexcept;
  uint64_t compacted_to() const noexcept;
  uint64_t bytes() const noexcept;
  bool has_update() const noexcept;
  void validate() const;
  void set_fast_apply() noexcept;
  void set_update_commit() noexcept;

  // total size + gid + state + fi + li + snapshot index
  static constexpr uint64_t meta_bytes() noexcept { return 72; }
};

using update_ptr = seastar::lw_shared_ptr<update>;

struct rsm_result {
  uint64_t value = 0;
  std::string data;
};

using snapshot_dir_func = std::function<std::string(group_id)>;

struct snapshot_request {
  enum class type : uint8_t {
    periodic,
    user_triggered,
    exported,
    streaming,
  };
  enum type type = type::periodic;
  std::string path;
  uint64_t compaction_overhead = 0;
  uint64_t key = 0;

  bool exported() const { return type == type::exported; }
};

struct snapshot_metadata {
  uint64_t from = group_id::INVALID_NODE;
  log_id lid;
  snapshot_request request;
  membership_ptr membership;
  state_machine_type smtype;
  compression_type comptype;
  std::any ctx;
};

// TODO(jyc): more elegant task type
struct rsm_task {
  uint64_t index = log_id::INVALID_INDEX;
  log_entry_vector entries;
  snapshot_request ss_request;
  bool save = false;
  bool stream = false;
  bool periodic_sync = false;
  bool new_node = false;
  bool recover = false;
  bool initial = false;
};

struct snapshot_state {
  uint64_t snapshot_index = protocol::log_id::INVALID_INDEX;
  uint64_t request_snapshot_index = protocol::log_id::INVALID_INDEX;
  uint64_t compact_log_to = protocol::log_id::INVALID_INDEX;
  uint64_t compacted_to = protocol::log_id::INVALID_INDEX;
  // TODO(jyc): have to use these independent fields?
  bool saving = false;
  bool recovering = false;
  bool streaming = false;
  std::optional<rsm_task> recover_ready;
  std::optional<rsm_task> save_ready;
  std::optional<rsm_task> stream_ready;
  std::optional<rsm_task> recover_completed;
  std::optional<rsm_task> save_completed;
  std::optional<rsm_task> stream_completed;
};

class utils {
 public:
  static void assert_continuous(log_entry_span left, log_entry_span right);
  static void fill_metadata_entries(log_entry_vector &entries);
  static log_entry_vector entries_to_apply(
      log_entry_vector &entries, uint64_t applied);
};

}  // namespace rafter::protocol
