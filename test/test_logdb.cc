//
// Created by jason on 2022/6/12.
//

#include "test_logdb.hh"

namespace rafter::test {

using namespace protocol;

future<std::vector<group_id>> test_logdb::list_nodes() {
  std::vector<group_id> groups;
  groups.reserve(_clusters.size());
  for (auto& [gid, _] : _clusters) {
    groups.emplace_back(gid);
  }
  return make_ready_future<std::vector<group_id>>(std::move(groups));
}

future<> test_logdb::save_bootstrap(group_id id, const bootstrap& info) {
  _clusters[id]._boot = info;
  return make_ready_future<>();
}

future<std::optional<bootstrap>> test_logdb::load_bootstrap(group_id id) {
  if (auto it = _clusters.find(id); it != _clusters.end()) {
    return make_ready_future<std::optional<bootstrap>>(it->second._boot);
  }
  return make_ready_future<std::optional<bootstrap>>(std::nullopt);
}

future<> test_logdb::save(std::span<storage::update_pack> updates) {
  for (auto& up : updates) {
    auto& n = _clusters[up.update.gid];
    for (const auto& ent : up.update.entries_to_save) {
      if (!n._entries.empty() &&
          ent->lid.index != n._entries.back()->lid.index + 1) {
        return make_exception_future<>(
            rafter::util::panic("inconsistent entry"));
      }
      n._entries.push_back(ent);
    }
    up.done.set_value();
  }
  return make_ready_future<>();
}

future<size_t> test_logdb::query_entries(
    group_id id, hint range, log_entry_vector& entries, uint64_t max_bytes) {
  auto it = _clusters.find(id);
  if (it == _clusters.end()) {
    return make_ready_future<size_t>(max_bytes);
  }
  auto& n = it->second;
  uint64_t next = range.low;
  for (auto& ent : n._entries) {
    if (next == range.high) {
      break;
    }
    if (max_bytes < ent->bytes()) {
      max_bytes = 0;
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

future<storage::raft_state> test_logdb::query_raft_state(
    group_id id, uint64_t /*last_index*/) {
  auto it = _clusters.find(id);
  if (it == _clusters.end()) {
    return make_ready_future<storage::raft_state>();
  }
  auto& n = it->second;
  return make_ready_future<storage::raft_state>(storage::raft_state{
      .hard_state = n._state,
      .first_index = n._entries.empty() ? 0 : n._entries.front()->lid.index,
      .entry_count = n._entries.size()});
}

future<snapshot_ptr> test_logdb::query_snapshot(group_id id) {
  auto it = _clusters.find(id);
  if (it == _clusters.end()) {
    return make_ready_future<snapshot_ptr>();
  }
  return make_ready_future<snapshot_ptr>(it->second._snap);
}

future<> test_logdb::remove(group_id id, uint64_t index) {
  // TODO(jyc)
  return not_implemented<>();
}

future<> test_logdb::remove_node(group_id id) {
  _clusters.erase(id);
  return make_ready_future<>();
}

future<> test_logdb::import_snapshot(snapshot_ptr snapshot) {
  auto& n = _clusters[snapshot->group_id];
  if (n._snap && n._snap->log_id.index >= snapshot->log_id.index) {
    throw rafter::util::snapshot_out_of_date();
  }
  n._snap = snapshot;
  n._boot = bootstrap{.join = true, .smtype = snapshot->smtype};
  n._entries.clear();
  return make_ready_future<>();
}

}  // namespace rafter::test
