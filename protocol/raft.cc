//
// Created by jason on 2021/9/26.
//

#include "raft.hh"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "protocol/client.hh"
#include "protocol/serializer.hh"
#include "util/error.hh"

namespace rafter::protocol {

using namespace std;

std::string_view name(enum raft_role role) {
  static std::string_view types[] = {
      "follower",
      "pre_candidate",
      "candidate",
      "leader",
      "observer",
      "witness",
  };
  static_assert(std::size(types) == static_cast<int>(raft_role::num_of_role));
  assert(
      static_cast<uint8_t>(role) <
      static_cast<uint8_t>(raft_role::num_of_role));
  return types[static_cast<uint8_t>(role)];
}

std::string_view name(enum message_type type) {
  static std::string_view types[] = {
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
      "rate_limit",
  };
  static_assert(
      std::size(types) == static_cast<int>(message_type::num_of_type));
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(message_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

std::string_view name(enum entry_type type) {
  static std::string_view types[] = {
      "application",
      "config_change",
      "encoded",
      "metadata",
  };
  static_assert(std::size(types) == static_cast<int>(entry_type::num_of_type));
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(entry_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

std::string_view name(enum config_change_type type) {
  static std::string_view types[] = {
      "add_node",
      "remove_node",
      "add_observer",
      "add_witness",
  };
  static_assert(
      std::size(types) == static_cast<int>(config_change_type::num_of_type));
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(config_change_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

std::string_view name(enum state_machine_type type) {
  static std::string_view types[] = {
      "regular",
  };
  static_assert(
      std::size(types) == static_cast<int>(state_machine_type::num_of_type));
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(state_machine_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

std::string_view name(enum compression_type type) {
  static std::string_view types[] = {
      "no_compression",
      "lz4",
      "snappy",
  };
  static_assert(
      std::size(types) == static_cast<int>(compression_type::num_of_type));
  assert(
      static_cast<uint8_t>(type) <
      static_cast<uint8_t>(compression_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

std::string_view name(enum checksum_type type) {
  static std::string_view types[] = {
      "no_checksum",
      "crc32",
      "highway",
  };
  static_assert(
      std::size(types) == static_cast<int>(checksum_type::num_of_type));
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
  return os << "lid[" << id.term << "," << id.index << "]";
}

std::ostream& operator<<(std::ostream& os, const hint& h) {
  return os << "[" << h.low << "," << h.high << "]";
}

std::ostream& operator<<(std::ostream& os, const log_entry& entry) {
  return os << fmt::format(
             "log_entry[term:{},index:{},type:{},key:{},client_id:{},series_id:"
             "{},responded_to:{},payload:{}]",
             entry.lid.term,
             entry.lid.index,
             entry.type,
             entry.key,
             entry.client_id,
             entry.series_id,
             entry.responded_to,
             entry.payload.size() > 10
                 ? fmt::format(
                       "{}...<{} bytes>",
                       std::string_view(entry.payload.get(), 10),
                       entry.payload.size())
                 : std::string_view(entry.payload.get(), entry.payload.size()));
}

std::ostream& operator<<(std::ostream& os, const hard_state& state) {
  return os << "[" << state.term << "," << state.vote << "," << state.commit
            << "]";
}

uint64_t bootstrap::bytes() const noexcept { return sizer(*this); }

bool bootstrap::operator==(const bootstrap& rhs) const noexcept {
  return addresses == rhs.addresses && join == rhs.join && smtype == rhs.smtype;
}

bool bootstrap::operator!=(const bootstrap& rhs) const noexcept {
  return !(*this == rhs);
}

uint64_t membership::bytes() const noexcept { return sizer(*this); }

bool membership::operator==(const membership& rhs) const noexcept {
  return config_change_id == rhs.config_change_id &&
         addresses == rhs.addresses && observers == rhs.observers &&
         witnesses == rhs.witnesses && removed == rhs.removed;
}

bool membership::operator!=(const membership& rhs) const noexcept {
  return !(*this == rhs);
}

log_entry log_entry::clone() {
  log_entry e;
  e.lid = lid;
  e.type = type;
  e.key = key;
  e.client_id = client_id;
  e.series_id = series_id;
  e.responded_to = responded_to;
  e.payload = payload.clone();
  return e;
}

log_entry log_entry::share() {
  log_entry e;
  e.lid = lid;
  e.type = type;
  e.key = key;
  e.client_id = client_id;
  e.series_id = series_id;
  e.responded_to = responded_to;
  e.payload = payload.share();
  return e;
}

uint64_t log_entry::bytes() const noexcept { return sizer(*this); }

uint64_t log_entry::in_memory_bytes() const noexcept {
  return sizeof(log_entry) + payload.size();
}

bool log_entry::is_proposal() const noexcept {
  return type != entry_type::config_change;
}

bool log_entry::is_config_change() const noexcept {
  return type == entry_type::config_change;
}

bool log_entry::is_noop() const noexcept {
  return !is_config_change() && !is_session_managed() && payload.empty();
}

bool log_entry::is_session_managed() const noexcept {
  if (is_config_change()) {
    return false;
  }
  if (payload.empty() && client_id == session::NOT_MANAGED_ID &&
      series_id == session::NOOP_SERIES_ID) {
    return false;
  }
  return true;
}

bool log_entry::is_new_session_request() const noexcept {
  return !is_config_change() && payload.empty() &&
         client_id != session::NOT_MANAGED_ID &&
         series_id == session::REGISTRATION_SERIES_ID;
}

bool log_entry::is_end_session_request() const noexcept {
  return !is_config_change() && payload.empty() &&
         client_id != session::NOT_MANAGED_ID &&
         series_id == session::UNREGISTRATION_SERIES_ID;
}

bool log_entry::is_noop_session() const noexcept {
  // TODO(jyc)
  return series_id == session::NOOP_SERIES_ID;
}

log_entry_vector log_entry::share(log_entry_span entries) {
  log_entry_vector shared;
  shared.reserve(entries.size());
  for (auto& e : entries) {
    shared.emplace_back(e.share());
  }
  return shared;
}

uint64_t log_entry::in_memory_bytes(log_entry_span entries) noexcept {
  uint64_t s = entries.size() * sizeof(log_entry);
  for (const auto& e : entries) {
    s += e.payload.size();
  }
  return s;
}

bool hard_state::empty() const noexcept {
  return term == log_id::INVALID_TERM && vote == group_id::INVALID_NODE &&
         commit == log_id::INVALID_INDEX;
}

bool snapshot::empty() const noexcept {
  return log_id.index == log_id::INVALID_INDEX;
}

uint64_t snapshot_file::bytes() const noexcept { return sizer(*this); }

std::string snapshot_file::filename() {
  return fmt::format("external-file-{}", file_id);
}

uint64_t snapshot::bytes() const noexcept { return sizer(*this); }

uint64_t message::bytes() const noexcept { return sizer(*this); }

message message::share() {
  message m;
  m.type = type;
  m.cluster = cluster;
  m.from = from;
  m.to = to;
  m.term = term;
  m.lid = lid;
  m.commit = commit;
  m.witness = witness;
  m.reject = reject;
  m.hint = hint;
  m.entries = log_entry::share(entries);
  m.snapshot = snapshot;
  return m;
}

std::vector<message> message::share(std::span<message> messages) {
  std::vector<message> msgs;
  msgs.reserve(messages.size());
  for (auto& msg : messages) {
    msgs.emplace_back(msg.share());
  }
  return msgs;
}

uint64_t config_change::bytes() const noexcept { return sizer(*this); }

uint64_t snapshot_chunk::bytes() const noexcept { return sizer(*this); }

void update::fill_meta() noexcept {
  if (!entries_to_save.empty()) {
    first_index = entries_to_save.front().lid.index;
    last_index = entries_to_save.back().lid.index;
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

void update::validate() const {
  if (state.commit != log_id::INVALID_INDEX && !committed_entries.empty()) {
    auto last = committed_entries.back().lid.index;
    if (last > state.commit) {
      throw util::failed_precondition_error(/*FIXME*/);
    }
  }
  if (!committed_entries.empty() && !entries_to_save.empty()) {
    auto last_apply = committed_entries.back().lid.index;
    auto last_save = entries_to_save.back().lid.index;
    if (last_apply > last_save) {
      throw util::failed_precondition_error(/*FIXME*/);
    }
  }
}

void update::set_fast_apply() noexcept {
  fast_apply = true;
  if (snapshot && !snapshot->empty()) {
    fast_apply = false;
  }
  if (fast_apply && !committed_entries.empty() && !entries_to_save.empty()) {
    auto last_apply = committed_entries.back().lid.index;
    auto last_save = entries_to_save.back().lid.index;
    auto first_save = entries_to_save.front().lid.index;
    // we cannot fast apply if some entries are not persistent yet
    if (last_apply >= first_save && last_apply <= last_save) {
      fast_apply = false;
    }
  }
}

void update::set_update_commit() noexcept {
  update_commit.ready_to_read = ready_to_reads.size();
  update_commit.last_applied = last_applied;
  if (!committed_entries.empty()) {
    update_commit.processed = committed_entries.back().lid.index;
  }
  if (!entries_to_save.empty()) {
    update_commit.stable_log_id = entries_to_save.back().lid;
  }
  if (snapshot && !snapshot->empty()) {
    update_commit.stable_snapshot_to = snapshot->log_id.index;
    update_commit.processed =
        max(update_commit.processed, update_commit.stable_snapshot_to);
  }
}

uint64_t update::bytes() const noexcept { return sizer(*this); }

void utils::assert_continuous(log_entry_span left, log_entry_span right) {
  if (left.empty() || right.empty()) {
    return;
  }
  if (left.back().lid.index + 1 != right.front().lid.index) {
    util::panic::panic_with_backtrace(fmt::format(
        "gap found, left:{}, right:{}",
        left.back().lid.index,
        right.front().lid.index));
  }
  if (left.back().lid.term > right.front().lid.term) {
    util::panic::panic_with_backtrace(fmt::format(
        "decreasing term, left:{}, right:{}",
        left.back().lid.term,
        right.front().lid.term));
  }
}

void utils::fill_metadata_entries(log_entry_vector& entries) {
  for (auto& entry : entries) {
    if (entry.type != entry_type::config_change) {
      entry.type = entry_type::metadata;
    }
  }
}

log_entry_vector utils::entries_to_apply(
    log_entry_vector& entries, uint64_t applied) {
  log_entry_vector to_apply;
  // TODO(jyc): binary search
  if (entries.empty() || entries.back().lid.index <= applied) {
    return to_apply;
  }
  for (auto& ent : entries) {
    if (ent.lid.index <= applied) {
      continue;
    }
    to_apply.emplace_back(ent.share());
  }
  return to_apply;
}

}  // namespace rafter::protocol