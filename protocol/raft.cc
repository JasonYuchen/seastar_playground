//
// Created by jason on 2021/9/26.
//

#include "raft.hh"

namespace rafter::protocol {

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
             < static_cast<uint8_t>(MessageType::num_of_type));
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

bool log_entry::is_proposal() const noexcept {
  return type != entry_type::config_change;
}

bool log_entry::is_config_change() const noexcept {
  return type == entry_type::config_change;
}

}  // namespace rafter::protocol