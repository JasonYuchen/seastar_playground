//
// Created by jason on 2021/9/26.
//

#include "raft.hh"

namespace rafter::protocol {

using namespace std;

// T is unordered_map<uint64_t, integral> or unordered_map<uint64_t, string>
template<typename T>
uint64_t sizer(T&& m) {
  uint64_t total = sizeof(m.size()); // for m.size()
  for (auto&& [_, s] : m) {
    if constexpr (is_integral_v<remove_reference_t<decltype(s)>>) {
      total += sizeof(_) + sizeof(s);
    } else {
      total += sizeof(_) + sizeof(s.size()) + s.size();
    }
  }
  return total;
}

const char* name(enum message_type type) {
  static const char *types[] = {
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
      "check_quorum"
  };
  assert(static_cast<uint8_t>(type)
             < static_cast<uint8_t>(message_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum entry_type type) {
  static const char *types[] = {
      "application",
      "config_change",
      "encoded",
      "metadata"
  };
  assert(static_cast<uint8_t>(type)
             < static_cast<uint8_t>(entry_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum config_change_type type) {
  static const char *types[] = {
      "add_node",
      "remove_node",
      "add_observer",
      "add_witness"
  };
  assert(static_cast<uint8_t>(type)
             < static_cast<uint8_t>(config_change_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum state_machine_type type) {
  static const char *types[] = {
      "regular",
  };
  assert(static_cast<uint8_t>(type)
             < static_cast<uint8_t>(state_machine_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum compression_type type) {
  static const char *types[] = {
      "no_compression",
      "lz4",
      "snappy"
  };
  assert(static_cast<uint8_t>(type)
             < static_cast<uint8_t>(compression_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

const char* name(enum checksum_type type) {
  static const char *types[] = {
      "no_checksum",
      "crc32",
      "highway"
  };
  assert(static_cast<uint8_t>(type)
             < static_cast<uint8_t>(checksum_type::num_of_type));
  return types[static_cast<uint8_t>(type)];
}

uint64_t bootstrap::bytes() const noexcept {
  return sizer(addresses) + sizeof(join) + sizeof(smtype) + 8;
}

uint64_t membership::bytes() const noexcept {
  return sizeof(config_change_id) +
         sizer(addresses) +
         sizer(observers) +
         sizer(witnesses) +
         sizer(removed) + 8;
}

uint64_t log_entry::bytes() const noexcept {
  return id.bytes() +
         sizeof(type) +
         sizeof(key) +
         sizeof(client_id) +
         sizeof(series_id) +
         sizeof(responded_to) +
         sizeof(payload.size()) +
         payload.size() + 8;
}

bool log_entry::is_proposal() const noexcept {
  return type != entry_type::config_change;
}

bool log_entry::is_config_change() const noexcept {
  return type == entry_type::config_change;
}

uint64_t snapshot_file::bytes() const noexcept {
  return sizeof(file_id) +
         sizeof(file_size) +
         sizeof(file_path.size()) +
         file_path.size() +
         sizeof(metadata.size()) +
         metadata.size() + 8;
}

uint64_t snapshot::bytes() const noexcept {
  uint64_t size = this->group_id.bytes() +
                  this->log_id.bytes() +
                  sizeof(file_path.size()) +
                  file_path.size() +
                  sizeof(file_size) +
                  sizeof(bool) +
                  sizeof(files.size()) +
                  sizeof(smtype) +
                  sizeof(imported) +
                  sizeof(witness) +
                  sizeof(dummy) +
                  sizeof(on_disk_index) + 8;
  if (membership) {
    size += membership->bytes();
  }
  for (const auto& f : files) {
    size += f->bytes();
  }
  return size;
}

uint64_t config_change::bytes() const noexcept {
  return sizeof(config_change_id) +
         sizeof(type) +
         sizeof(node) +
         sizeof(address.size()) +
         address.size() +
         sizeof(initialize) + 8;
}

uint64_t update::compactedTo() const noexcept {
  if (entries_to_save.empty() &&
      !snapshot &&
      state.vote == group_id::invalid_node &&
      state.term == log_id::invalid_term &&
      state.commit != log_id::invalid_index) {
    return state.commit;
  }
  return log_id::invalid_index;
}

bool update::has_update() const noexcept {
  return snapshot ||
         !state.empty() ||
         !entries_to_save.empty() ||
         !committed_entries.empty() ||
         !messages.empty() ||
         !ready_to_reads.empty() ||
         !dropped_entries.empty();
}

uint64_t update::bytes() const noexcept {
  uint64_t size = this->group_id.bytes() +
                  this->state.bytes() +
                  sizeof(first_index) +
                  sizeof(last_index) +
                  sizeof(entries_to_save.size()) +
                  sizeof(bool) + 8;
  if (snapshot) {
    size += snapshot->bytes();
  }
  for (const auto& e : entries_to_save) {
    size += e->bytes();
  }
  return size;
}

uint64_t update::meta_bytes() const noexcept {
  return this->group_id.bytes() +
         this->state.bytes() +
         sizeof(first_index) +
         sizeof(last_index) +
         sizeof(bool) + 8;
}

}  // namespace rafter::protocol