//
// Created by jason on 2022/5/29.
//

#pragma once

#include <seastar/core/coroutine.hh>

#include "storage/logdb.hh"
#include "util/error.hh"

namespace rafter::test {

class test_logdb final : public storage::logdb {
 public:
  ~test_logdb() override = default;

  std::string name() const noexcept override { return "test_logdb"; }

  future<std::vector<protocol::group_id>> list_nodes() override {
    return not_implemented<std::vector<protocol::group_id>>();
  }

  future<> save_bootstrap(
      protocol::group_id, const protocol::bootstrap&) override {
    return not_implemented<>();
  }

  future<std::optional<protocol::bootstrap>> load_bootstrap(
      protocol::group_id id) override {
    return not_implemented<std::optional<protocol::bootstrap>>();
  }

  future<> save(std::span<storage::update_pack> updates) override {
    for (auto& up : updates) {
      for (const auto& ent : up.update.entries_to_save) {
        if (!_entries.empty() &&
            ent->lid.index != _entries.back()->lid.index + 1) {
          return make_exception_future<>(
              rafter::util::panic("inconsistent entry"));
        }
        _entries.push_back(ent);
      }
      up.done.set_value();
    }
    return make_ready_future<>();
  }

  future<size_t> query_entries(
      protocol::group_id,
      protocol::hint range,
      protocol::log_entry_vector& entries,
      uint64_t max_bytes) override {
    uint64_t next = range.low;
    for (auto& ent : _entries) {
      if (next == range.high) {
        break;
      }
      if (max_bytes < ent->bytes()) {
        break;
      }
      if (ent->lid.index != next) {
        continue;
      }
      if (!entries.empty() && entries.back()->lid.index + 1 != ent->lid.index) {
        return make_exception_future<size_t>(
            rafter::util::panic("inconsistent entry"));
      }
      entries.push_back(ent);
      max_bytes -= ent->bytes();
      next++;
    }
    return make_ready_future<size_t>(max_bytes);
  }

  future<storage::raft_state> query_raft_state(
      protocol::group_id, uint64_t last_index) override {
    return make_ready_future<storage::raft_state>(storage::raft_state{
        .hard_state = _state,
        .first_index = _entries.empty() ? 0 : _entries.front()->lid.index,
        .entry_count = _entries.size()});
  }

  future<protocol::snapshot_ptr> query_snapshot(protocol::group_id) override {
    return make_ready_future<protocol::snapshot_ptr>(_snap);
  }

  future<> remove(protocol::group_id, uint64_t) override {
    return not_implemented<>();
  }

  future<> remove_node(protocol::group_id) override {
    return not_implemented<>();
  }

  future<> import_snapshot(protocol::snapshot_ptr snapshot) override {
    if (_snap && _snap->log_id.index >= snapshot->log_id.index) {
      throw rafter::util::snapshot_out_of_date();
    }
    _snap = snapshot;
    return make_ready_future<>();
  }

 private:
  protocol::hard_state _state;
  protocol::snapshot_ptr _snap;
  protocol::log_entry_vector _entries;
};

}  // namespace rafter::test
