//
// Created by jason on 2022/5/29.
//

#pragma once

#include <seastar/core/coroutine.hh>

#include "storage/logdb.hh"
#include "util/error.hh"
#include "util/util.hh"

namespace rafter::test {

// test_logdb is a naive in-memory logdb
class test_logdb final : public storage::logdb {
 public:
  ~test_logdb() override = default;

  std::string name() const noexcept override { return "test_logdb"; }

  future<std::vector<protocol::group_id>> list_nodes() override;

  future<> save_bootstrap(
      protocol::group_id id, const protocol::bootstrap& info) override;

  future<std::optional<protocol::bootstrap>> load_bootstrap(
      protocol::group_id id) override;

  future<> save(std::span<storage::update_pack> updates) override;

  future<size_t> query_entries(
      protocol::group_id id,
      protocol::hint range,
      protocol::log_entry_vector& entries,
      uint64_t max_bytes) override;

  future<storage::raft_state> query_raft_state(
      protocol::group_id id, uint64_t last_index) override;

  future<protocol::snapshot_ptr> query_snapshot(protocol::group_id id) override;

  future<> remove(protocol::group_id id, uint64_t index) override;

  future<> remove_node(protocol::group_id id) override;

  future<> import_snapshot(protocol::snapshot_ptr snapshot) override;

 private:
  struct data {
    std::optional<protocol::bootstrap> _boot;
    protocol::hard_state _state;
    protocol::snapshot_ptr _snap;
    protocol::log_entry_vector _entries;
  };
  std::unordered_map<protocol::group_id, data, util::pair_hasher> _clusters;
};

}  // namespace rafter::test
