//
// Created by jason on 2021/12/15.
//

#include "peer.hh"

#include <seastar/core/coroutine.hh>

#include "core/logger.hh"
#include "protocol/serializer.hh"
#include "util/error.hh"

namespace rafter::core {

using namespace protocol;

peer::peer(
    const raft_config& c,
    log_reader& lr,
    const member_map& addresses,
    bool initial,
    bool new_node)
  : _raft(c, lr) {
  l.info(
      "{} created, initial:{}, new_node:{}",
      group_id{c.cluster_id, c.node_id},
      initial,
      new_node);
  _prev_state = _raft.state();
  if (initial && new_node) {
    _raft.become_follower(1, group_id::INVALID_NODE, true);
    bootstrap(addresses);
  }
}

future<> peer::tick() {
  // TODO(jyc): directly call tick()
  return _raft.handle(
      message{.type = message_type::local_tick, .reject = false});
}

future<> peer::quiesced_tick() {
  // TODO(jyc): directly call quiesce_tick()
  return _raft.handle(
      message{.type = message_type::local_tick, .reject = true});
}

future<> peer::request_leader_transfer(uint64_t target) {
  return _raft.handle(message{
      .type = message_type::leader_transfer,
      .to = _raft._gid.node,
      .hint = {target}});
}

future<> peer::read_index(hint ctx) {
  return _raft.handle(message{.type = message_type::read_index, .hint = ctx});
}

future<> peer::propose_entries(log_entry_vector entries) {
  return _raft.handle(message{
      .type = message_type::propose,
      .from = _raft._gid.node,
      .entries = std::move(entries)});
}

future<> peer::propose_config_change(
    const config_change& change, uint64_t key) {
  log_entry_vector e;
  e.emplace_back();
  e.back().type = entry_type::config_change;
  e.back().payload = write_to_tmpbuf(change);
  e.back().key = key;
  return _raft.handle(
      message{.type = message_type::propose, .entries = std::move(e)});
}

future<> peer::apply_config_change(const config_change& change) {
  if (change.node == group_id::INVALID_NODE) {
    _raft._pending_config_change = false;
    return make_ready_future<>();
  }
  return _raft.handle(message{
      .type = message_type::config_change,
      .reject = false,
      .hint = {change.node, static_cast<uint8_t>(change.type)}});
}

future<> peer::reject_config_change() {
  return _raft.handle(
      message{.type = message_type::config_change, .reject = true});
}

future<> peer::restore_remotes(snapshot_ptr snapshot) {
  return _raft.handle(message{
      .type = message_type::snapshot_received,
      .snapshot = std::move(snapshot)});
}

future<> peer::report_unreachable(uint64_t node) {
  return _raft.handle(message{.type = message_type::unreachable, .from = node});
}

future<> peer::report_snapshot_status(uint64_t node, bool reject) {
  return _raft.handle(message{
      .type = message_type::snapshot_status, .from = node, .reject = reject});
}

future<> peer::handle(message m) {
  if (is_local(m.type)) [[unlikely]] {
    return make_exception_future<>(
        util::failed_precondition_error("peer received local message"));
  }
  if (_raft._remotes.contains(m.from) || _raft._observers.contains(m.from) ||
      _raft._witnesses.contains(m.from) || !is_response(m.type)) {
    return _raft.handle(m);
  }
  return make_ready_future<>();
}

bool peer::has_entry_to_apply() { return _raft._log.has_entries_to_apply(); }

bool peer::has_update(bool more_to_apply) {
  if (_raft._log.has_entries_to_save()) {
    return true;
  }
  if (!_raft._messages.empty()) {
    return true;
  }
  if (more_to_apply && _raft._log.has_entries_to_apply()) {
    return true;
  }
  if (auto s = _raft.state(); !s.empty() && s != _prev_state) {
    return true;
  }
  if (auto s = _raft._log.get_memory_snapshot(); s && !s->empty()) {
    return true;
  }
  if (!_raft._ready_to_reads.empty()) {
    return true;
  }
  if (!_raft._dropped_entries.empty()) {
    return true;
  }
  if (!_raft._dropped_read_indexes.empty()) {
    return true;
  }
  return false;
}

future<update> peer::get_update(bool more_to_apply, uint64_t last_applied) {
  auto up = update{
      .gid = _raft._gid,
      .fast_apply = true,
      .messages = message::share(_raft._messages),
      .last_applied = last_applied,
  };
  _raft._log.get_entries_to_save(up.entries_to_save);
  for (auto& m : up.messages) {
    m.cluster = _raft._gid.cluster;
  }
  if (more_to_apply) {
    co_await _raft._log.get_entries_to_apply(up.committed_entries);
  }
  if (!up.committed_entries.empty()) {
    auto last = up.committed_entries.back().lid.index;
    up.has_more_committed_entries = _raft._log.has_more_entries_to_apply(last);
  }
  if (auto s = _raft.state(); s != _prev_state) {
    up.state = s;
  }
  if (auto s = _raft._log.get_memory_snapshot(); s) {
    up.snapshot = s;
  }
  if (!_raft._ready_to_reads.empty()) {
    up.ready_to_reads = _raft._ready_to_reads;
  }
  up.validate();
  up.set_fast_apply();
  up.set_update_commit();
  up.fill_meta();
  co_return std::move(up);
}

void peer::commit(const update& up) {
  _raft._messages.clear();
  if (!up.state.empty()) {
    _prev_state = up.state;
  }
  if (up.update_commit.ready_to_read > 0) {
    _raft._ready_to_reads.clear();
  }
  _raft._log.commit_update(up.update_commit);
}

void peer::notify_last_applied(uint64_t last_applied) noexcept {
  _raft._applied = last_applied;
}

void peer::bootstrap(const member_map& addresses) {
  log_entry_vector entries;
  entries.reserve(addresses.size());
  for (const auto& [id, address] : addresses) {
    l.info("{}: added bootstrap node {} {}", _raft, id, address);
    auto& e = entries.emplace_back();
    e.lid = {.term = 1, .index = entries.size()};
    e.type = entry_type::config_change;
    e.payload = write_to_tmpbuf(config_change{
        .type = config_change_type::add_node,
        .node = id,
        .address = address,
        .initialize = true});
  }
  _raft._log.append(entries);
  _raft._log.set_committed(entries.size());
  for (const auto& [id, address] : addresses) {
    _raft.add_node(id);
  }
}

}  // namespace rafter::core
