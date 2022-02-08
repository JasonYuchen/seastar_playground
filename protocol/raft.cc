//
// Created by jason on 2021/9/26.
//

#include "raft.hh"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "protocol/serializer.hh"
#include "util/error.hh"

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
  return cluster != INVALID_CLUSTER && node != INVALID_NODE;
}

std::ostream& operator<<(std::ostream& os, const group_id& id) {
  return os << "gid[" << std::setfill('0') << std::setw(5) << id.cluster << ","
            << std::setfill('0') << std::setw(5) << id.node << "]";
}

std::ostream& operator<<(std::ostream& os, const log_id& id) {
  return os << "lid[" << std::setfill('0') << std::setw(5) << id.term << ","
            << std::setfill('0') << std::setw(5) << id.index << "]";
}

std::ostream& operator<<(std::ostream& os, const hint& h) {
  return os << "[" << h.low << "," << h.high << "]";
}

std::ostream& operator<<(std::ostream& os, const hard_state& state) {
  return os << "[" << state.term << "," << state.vote << "," << state.commit
            << "]";
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
  return term == log_id::INVALID_TERM && vote == group_id::INVALID_NODE &&
         commit == log_id::INVALID_INDEX;
}

uint64_t snapshot_file::bytes() const noexcept { return sizer(*this); }

uint64_t snapshot::bytes() const noexcept { return sizer(*this); }

uint64_t message::bytes() const noexcept { return sizer(*this); }

uint64_t config_change::bytes() const noexcept { return sizer(*this); }

uint64_t snapshot_chunk::bytes() const noexcept { return sizer(*this); }

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
  if (first_index == log_id::INVALID_INDEX &&
      last_index == log_id::INVALID_INDEX &&
      snapshot_index == log_id::INVALID_INDEX &&
      state.vote == group_id::INVALID_NODE &&
      state.term == log_id::INVALID_TERM &&
      state.commit != log_id::INVALID_INDEX) {
    return state.commit;
  }
  return log_id::INVALID_INDEX;
}

bool update::has_update() const noexcept {
  return snapshot || !state.empty() || !entries_to_save.empty() ||
         !committed_entries.empty() || !messages.empty() ||
         !ready_to_reads.empty() || !dropped_entries.empty();
}

uint64_t update::bytes() const noexcept { return sizer(*this); }

void utils::assert_continuous(log_entry_span left, log_entry_span right) {
  if (left.empty() || right.empty()) {
    return;
  }
  if (left.back()->lid.index + 1 != right.front()->lid.index) {
    throw util::failed_precondition_error(fmt::format(
        "gap found, left:{}, right:{}",
        left.back()->lid.index,
        right.front()->lid.index));
  }
  if (left.back()->lid.term > right.front()->lid.term) {
    throw util::failed_precondition_error(fmt::format(
        "decreasing term, left:{}, right:{}",
        left.back()->lid.term,
        right.front()->lid.term));
  }
}

void utils::fill_metadata_entries(log_entry_vector& entries) {
  for (auto& entry : entries) {
    if (entry->type != entry_type::config_change) {
      log_id lid = entry->lid;
      entry = seastar::make_lw_shared<log_entry>();
      entry->type = entry_type::metadata;
      entry->lid = lid;
    }
  }
}

}  // namespace rafter::protocol