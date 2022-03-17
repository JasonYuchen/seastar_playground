//
// Created by jason on 2021/12/15.
//

#include "peer.hh"

#include "protocol/serializer.hh"
#include "util/error.hh"

namespace rafter::core {

using namespace protocol;
using namespace seastar;

peer::peer(
    const config& c,
    log_reader& lr,
    std::map<uint64_t, std::string> addresses,
    bool initial,
    bool new_node)
  : _raft(c, lr) {
  // TODO(jyc)
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

future<> peer::read_index(protocol::hint ctx) {
  return _raft.handle(message{.type = message_type::read_index, .hint = ctx});
}

future<> peer::propose_entries(protocol::log_entry_vector entries) {
  return _raft.handle(message{
      .type = message_type::propose,
      .from = _raft._gid.node,
      .entries = std::move(entries)});
}

future<> peer::propose_config_change(
    protocol::config_change change, uint64_t key) {
  log_entry_vector e;
  e.emplace_back(make_lw_shared<log_entry>());
  e.back()->type = entry_type::config_change;
  e.back()->payload = write_to_string(change);
  e.back()->key = key;
  return _raft.handle(
      message{.type = message_type::propose, .entries = std::move(e)});
}

future<> peer::apply_config_change(protocol::config_change change) {
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

future<> peer::restore_remotes(protocol::snapshot_ptr snapshot) {
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

future<> peer::handle(protocol::message m) {
  if (is_local(m.type)) [[unlikely]] {
    throw util::failed_precondition_error("peer received local message");
  }
  if (_raft._remotes.contains(m.from) || _raft._observers.contains(m.from) ||
      _raft._witnesses.contains(m.from) || !is_response(m.type)) {
    return _raft.handle(m);
  }
  return make_ready_future<>();
}

}  // namespace rafter::core
