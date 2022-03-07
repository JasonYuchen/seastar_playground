//
// Created by jason on 2021/10/14.
//

#pragma once

#include <seastar/core/future.hh>

#include "protocol/raft.hh"

namespace rafter::storage {

struct raft_state {
  protocol::hard_state hard_state;
  uint64_t first_index = protocol::log_id::INVALID_INDEX;
  uint64_t entry_count = 0;
};

class logdb {
 public:
  virtual std::string name() const noexcept = 0;
  virtual seastar::future<> save_bootstrap_info(
      protocol::bootstrap_ptr info) = 0;
  virtual seastar::future<protocol::bootstrap_ptr> load_bootstrap_info() = 0;
  virtual seastar::future<> save(std::span<protocol::update> updates) = 0;
  virtual seastar::future<size_t> query_entries(
      protocol::group_id id,
      protocol::hint range,
      protocol::log_entry_vector& entries,
      uint64_t max_bytes) = 0;
  virtual seastar::future<raft_state> query_raft_state(
      protocol::group_id id, uint64_t last_index) = 0;
  virtual seastar::future<protocol::snapshot_ptr> query_snapshot(
      protocol::group_id id) = 0;
  virtual seastar::future<> remove(protocol::group_id id, uint64_t index) = 0;
  virtual seastar::future<> remove_node(protocol::group_id id) = 0;
  virtual seastar::future<> import_snapshot(
      protocol::snapshot_ptr snapshot) = 0;
};

}  // namespace rafter::storage
