//
// Created by jason on 2021/10/14.
//

#pragma once

#include <optional>
#include <seastar/core/future.hh>

#include "protocol/raft.hh"
#include "util/seastarx.hh"

namespace rafter::storage {

struct raft_state {
  protocol::hard_state hard_state;
  uint64_t first_index = protocol::log_id::INVALID_INDEX;
  uint64_t entry_count = 0;

  bool empty() const {
    return hard_state.empty() &&
           first_index == protocol::log_id::INVALID_INDEX && entry_count == 0;
  }
};

class logdb {
 public:
  virtual ~logdb() = default;
  virtual std::string name() const noexcept = 0;
  virtual future<std::vector<protocol::group_id>> list_nodes() = 0;
  virtual future<> save_bootstrap(
      protocol::group_id id, const protocol::bootstrap& info) = 0;
  virtual future<std::optional<protocol::bootstrap>> load_bootstrap(
      protocol::group_id id) = 0;
  virtual future<> save(std::span<protocol::update> updates) = 0;
  virtual future<size_t> query_entries(
      protocol::group_id id,
      protocol::hint range,
      protocol::log_entry_vector& entries,
      uint64_t max_bytes) = 0;
  virtual future<raft_state> query_raft_state(
      protocol::group_id id, uint64_t last_index) = 0;
  virtual future<protocol::snapshot_ptr> query_snapshot(
      protocol::group_id id) = 0;
  virtual future<> remove(protocol::group_id id, uint64_t index) = 0;
  virtual future<> remove_node(protocol::group_id id) = 0;
  virtual future<> import_snapshot(protocol::snapshot_ptr snapshot) = 0;
};

}  // namespace rafter::storage
