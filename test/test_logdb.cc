//
// Created by jason on 2022/6/12.
//

#include "test_logdb.hh"

#include "test/base.hh"

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
  if (!id.valid()) {
    l.error("test_logdb::save: invalid {}", id);
    rafter::util::panic::panic_with_backtrace("invalid gid");
  }
  _clusters[id]._boot = info;
  return make_ready_future<>();
}

future<std::optional<bootstrap>> test_logdb::load_bootstrap(group_id id) {
  if (auto it = _clusters.find(id); it != _clusters.end()) {
    l.warn("test_logdb::load_bootstrap: {} not found", id);
    return make_ready_future<std::optional<bootstrap>>(it->second._boot);
  }

  return make_ready_future<std::optional<bootstrap>>(std::nullopt);
}

future<> test_logdb::save(std::span<storage::update_pack> updates) {
  for (auto& up : updates) {
    if (!up.update.gid.valid()) {
      l.error("test_logdb::save: invalid {}", up.update.gid);
      rafter::util::panic::panic_with_backtrace("invalid gid");
    }
    auto& n = _clusters[up.update.gid];
    // TODO(jyc): binary search ?
    if (!up.update.entries_to_save.empty()) {
      auto it = n._entries.begin();
      for (; it != n._entries.end(); ++it) {
        if (it->lid.index >= up.update.entries_to_save.front().lid.index) {
          break;
        }
      }
      n._entries.erase(it, n._entries.end());
    }
    for (const auto& ent : up.update.entries_to_save) {
      if (!n._entries.empty() &&
          ent.lid.index != n._entries.back().lid.index + 1) {
        rafter::util::panic::panic_with_backtrace("inconsistent entry");
      }
      // to avoid redundant copy, have to make it mutable and call share()
      // which is safe here since share() is just a ref counting operation
      n._entries.push_back(const_cast<log_entry&>(ent).share());
    }
    if (up.update.snapshot) {
      if (!n._snap ||
          up.update.snapshot->log_id.index > n._snap->log_id.index) {
        n._snap = up.update.snapshot;
      }
    }
    if (!up.update.state.empty()) {
      n._state = up.update.state;
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
    if (max_bytes < ent.in_memory_bytes()) {
      max_bytes = 0;
      break;
    }
    if (ent.lid.index != next) {
      continue;
    }
    if (!entries.empty() && entries.back().lid.index + 1 != ent.lid.index) {
      rafter::util::panic::panic_with_backtrace("inconsistent entry");
    }
    entries.push_back(ent.share());
    max_bytes -= ent.in_memory_bytes();
    next++;
  }
  return make_ready_future<size_t>(max_bytes);
}

future<storage::raft_state> test_logdb::query_raft_state(
    group_id id, uint64_t last_index) {
  auto it = _clusters.find(id);
  if (it == _clusters.end()) {
    return make_ready_future<storage::raft_state>();
  }
  auto& n = it->second;
  uint64_t first_index = 0;
  uint64_t entry_count = 0;
  for (auto& e : n._entries) {
    if (last_index + 1 == e.lid.index) {
      first_index = e.lid.index;
      entry_count = n._entries.back().lid.index + 1 - e.lid.index;
      break;
    }
  }

  return make_ready_future<storage::raft_state>(storage::raft_state{
      .hard_state = n._state,
      .first_index = first_index,
      .entry_count = entry_count});
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
    return make_exception_future<>(rafter::util::snapshot_out_of_date());
  }
  n._snap = snapshot;
  n._boot = bootstrap{.join = true, .smtype = snapshot->smtype};
  n._entries.clear();
  return make_ready_future<>();
}

}  // namespace rafter::test
