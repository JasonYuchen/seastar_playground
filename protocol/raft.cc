//
// Created by jason on 2021/9/26.
//

#include "raft.hh"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "protocol/serializer.hh"

namespace rafter::protocol {

using namespace std;

const char* name(enum raft_role role) {
  static const char* types[] = {
      "follower",
      "pre_candidate",
      "candidate",
      "leader",
      "observer",
      "witness",
  };
  assert(
      static_cast<uint8_t>(role) <
      static_cast<uint8_t>(raft_role::num_of_role));
  return types[static_cast<uint8_t>(role)];
}

const char* name(enum message_type type) {
  static const char* types[] = {
      "noop",
      "local_tick",
      "election",
      "leader_heartbeat",
      "config_change",
      "propose",
      "replicate",
      "replicate_resp",
      "read_index",
      "read_index_resp",
      "heartbeat",
      "heartbeat_resp",
      "request_vote",
      "request_vote_resp",
      "request_prevote",
      "request_prevote_resp",
      "install_snapshot",
      "snapshot_status",
      "snapshot_received",
      "leader_transfer",
      "timeout_now",
      "unreachable",
      "quiesce",
      "check_quorum",
  };
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(message_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum entry_type type) {
  static const char* types[] = {
      "application",
      "config_change",
      "encoded",
      "metadata",
  };
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(entry_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum config_change_type type) {
  static const char* types[] = {
      "add_node",
      "remove_node",
      "add_observer",
      "add_witness",
  };
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(config_change_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum state_machine_type type) {
  static const char* types[] = {
      "regular",
  };
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(state_machine_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum compression_type type) {
  static const char* types[] = {
      "no_compression",
      "lz4",
      "snappy",
  };
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(compression_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum checksum_type type) {
  static const char* types[] = {
      "no_checksum",
      "crc32",
      "highway",
  };
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(checksum_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

bool group_id::valid() const noexcept {
  return cluster != invalid_cluster && node != invalid_node;
}

string group_id::to_string() const {
  return fmt::format("gid[{:05d},{:05d}]", cluster, node);
}

std::ostream& operator<<(std::ostream& os, const group_id& id) {
  return os << "gid[" << std::setfill('0') << std::setw(5) << id.cluster << ","
            << std::setfill('0') << std::setw(5) << id.node << "]";
}

string log_id::to_string() const {
  return fmt::format("lid[{},{}]", term, index);
}

std::ostream& operator<<(std::ostream& os, const log_id& id) {
  return os << "lid[" << std::setfill('0') << std::setw(5) << id.term << ","
            << std::setfill('0') << std::setw(5) << id.index << "]";
}

uint64_t bootstrap::bytes() const noexcept { return sizer(*this); }

uint64_t membership::bytes() const noexcept { return sizer(*this); }

bool membership::operator==(const membership& rhs) const noexcept {
  return config_change_id == rhs.config_change_id &&
         addresses == rhs.addresses && observers == rhs.observers &&
         witnesses == rhs.witnesses && removed == rhs.removed;
}

bool membership::operator!=(const membership& rhs) const noexcept {
  return !(*this == rhs);
}

uint64_t log_entry::bytes() const noexcept { return sizer(*this); }

bool log_entry::is_proposal() const noexcept {
  return type != entry_type::config_change;
}

bool log_entry::is_config_change() const noexcept {
  return type == entry_type::config_change;
}

bool hard_state::empty() const noexcept {
  return term == log_id::invalid_term && vote == group_id::invalid_node &&
         commit == log_id::invalid_index;
}

uint64_t snapshot_file::bytes() const noexcept { return sizer(*this); }

uint64_t snapshot::bytes() const noexcept { return sizer(*this); }

uint64_t message::bytes() const noexcept { return sizer(*this); }

uint64_t config_change::bytes() const noexcept { return sizer(*this); }

void update::fill_meta() noexcept {
  if (!entries_to_save.empty()) {
    first_index = entries_to_save.front()->lid.index;
    last_index = entries_to_save.back()->lid.index;
  }
  if (snapshot) {
    snapshot_index = snapshot->log_id.index;
  }
}

uint64_t update::compacted_to() const noexcept {
  if (first_index == log_id::invalid_index &&
      last_index == log_id::invalid_index &&
      snapshot_index == log_id::invalid_index &&
      state.vote == group_id::invalid_node &&
      state.term == log_id::invalid_term &&
      state.commit != log_id::invalid_index) {
    return state.commit;
  }
  return log_id::invalid_index;
}

bool update::has_update() const noexcept {
  return snapshot || !state.empty() || !entries_to_save.empty() ||
         !committed_entries.empty() || !messages.empty() ||
         !ready_to_reads.empty() || !dropped_entries.empty();
}

uint64_t update::bytes() const noexcept { return sizer(*this); }

}  // namespace rafter::protocol