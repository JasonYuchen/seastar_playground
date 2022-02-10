//
// Created by jason on 2021/10/9.
//

#include "raft.hh"

#include "core/logger.hh"

namespace rafter::core {

using namespace protocol;
using namespace seastar;

// TODO(jyc): add raft event listener

std::ostream& operator<<(std::ostream& os, const raft& r) {
  return os << "raft[" << r._gid << ","
            << "r:" << r._role << ","
            << "fi:" << r._log.first_index() << ","
            << "li:" << r._log.last_index() << ","
            << "t:" << r._term << "]";
}

void raft::initialize_handlers() {
  for (auto& roles : _handlers) {
    for (auto& handler : roles) {
      handler = nullptr;
    }
  }

#define D_(role, type, handler)                                                \
  _handlers[static_cast<uint8_t>(raft_role::role)]                             \
           [static_cast<uint8_t>(message_type::type)] = &raft::handler

  D_(leader, election, node_election);
  D_(leader, request_vote, node_request_vote);
  D_(leader, config_change, node_config_change);
  D_(leader, local_tick, node_local_tick);
  D_(leader, snapshot_received, node_restore_remote);
  D_(leader, leader_heartbeat, leader_heartbeat);
  D_(leader, heartbeat_resp, leader_heartbeat_resp);
  D_(leader, check_quorum, leader_check_quorum);
  D_(leader, propose, leader_propose);
  D_(leader, read_index, leader_read_index);
  D_(leader, replicate_resp, leader_replicate_resp);
  D_(leader, snapshot_status, leader_snapshot_status);
  D_(leader, unreachable, leader_unreachable);
  D_(leader, leader_transfer, leader_leader_transfer);
  D_(leader, rate_limit, leader_rate_limit);

  D_(candidate, election, node_election);
  D_(candidate, request_vote, node_request_vote);
  D_(candidate, request_prevote, node_request_prevote);
  D_(candidate, config_change, node_config_change);
  D_(candidate, local_tick, node_local_tick);
  D_(candidate, snapshot_received, node_restore_remote);
  D_(candidate, heartbeat, candidate_heartbeat);
  D_(candidate, propose, candidate_propose);
  D_(candidate, read_index, candidate_read_index);
  D_(candidate, replicate, candidate_replicate);
  D_(candidate, install_snapshot, candidate_install_snapshot);
  D_(candidate, request_vote_resp, candidate_request_vote_resp);

  D_(pre_candidate, election, node_election);
  D_(pre_candidate, request_vote, node_request_vote);
  D_(pre_candidate, request_prevote, node_request_prevote);
  D_(pre_candidate, config_change, node_config_change);
  D_(pre_candidate, local_tick, node_local_tick);
  D_(pre_candidate, snapshot_received, node_restore_remote);
  D_(pre_candidate, heartbeat, candidate_heartbeat);
  D_(pre_candidate, propose, candidate_propose);
  D_(pre_candidate, read_index, candidate_read_index);
  D_(pre_candidate, replicate, candidate_replicate);
  D_(pre_candidate, install_snapshot, candidate_install_snapshot);
  D_(pre_candidate, request_vote_resp, candidate_request_vote_resp);
  D_(pre_candidate, request_prevote_resp, pre_candidate_request_prevote_resp);

  D_(follower, election, node_election);
  D_(follower, request_vote, node_request_vote);
  D_(follower, request_prevote, node_request_prevote);
  D_(follower, config_change, node_config_change);
  D_(follower, local_tick, node_local_tick);
  D_(follower, snapshot_received, node_restore_remote);
  D_(follower, heartbeat, follower_heartbeat);
  D_(follower, propose, follower_propose);
  D_(follower, read_index, follower_read_index);
  D_(follower, read_index_resp, follower_read_index_resp);
  D_(follower, replicate, follower_replicate);
  D_(follower, leader_transfer, follower_leader_transfer);
  D_(follower, install_snapshot, follower_install_snapshot);
  D_(follower, timeout_now, follower_timeout_now);

  D_(observer, election, node_election);
  D_(observer, request_vote, node_request_vote);
  D_(observer, request_prevote, node_request_prevote);
  D_(observer, config_change, node_config_change);
  D_(observer, local_tick, node_local_tick);
  D_(observer, snapshot_received, node_restore_remote);
  D_(observer, heartbeat, observer_heartbeat);
  D_(observer, propose, observer_propose);
  D_(observer, read_index, observer_read_index);
  D_(observer, read_index_resp, observer_read_index_resp);
  D_(observer, replicate, observer_replicate);
  D_(observer, install_snapshot, observer_install_snapshot);

  D_(witness, election, node_election);
  D_(witness, request_vote, node_request_vote);
  D_(witness, request_prevote, node_request_prevote);
  D_(witness, config_change, node_config_change);
  D_(witness, local_tick, node_local_tick);
  D_(witness, snapshot_received, node_restore_remote);
  D_(witness, heartbeat, witness_heartbeat);
  D_(witness, replicate, witness_replicate);
  D_(witness, install_snapshot, witness_install_snapshot);

#undef D_

  assert_handlers();
}

void raft::assert_handlers() {
  using enum raft_role;
  using enum message_type;
  std::pair<raft_role, message_type> null_handlers[] = {
      {leader, heartbeat},
      {leader, replicate},
      {leader, install_snapshot},
      {leader, read_index_resp},
      {leader, request_prevote_resp},
      {follower, replicate_resp},
      {follower, heartbeat_resp},
      {follower, snapshot_status},
      {follower, unreachable},
      {follower, request_prevote_resp},
      {candidate, replicate_resp},
      {candidate, heartbeat_resp},
      {candidate, snapshot_status},
      {candidate, unreachable},
      {candidate, request_prevote_resp},
      {pre_candidate, replicate_resp},
      {pre_candidate, heartbeat_resp},
      {pre_candidate, snapshot_status},
      {pre_candidate, unreachable},
      {observer, election},
      {observer, replicate_resp},
      {observer, heartbeat_resp},
      {observer, request_vote_resp},
      {observer, request_prevote_resp},
      {witness, election},
      {witness, propose},
      {witness, read_index},
      {witness, read_index_resp},
      {witness, replicate_resp},
      {witness, heartbeat_resp},
      {witness, request_vote_resp},
      {witness, request_prevote_resp},
  };
  bool fatal = false;
  for (auto [role, type] : null_handlers) {
    if (_handlers[static_cast<uint8_t>(role)][static_cast<uint8_t>(type)] !=
        nullptr) {
      l.error(
          "raft::assert_handlers: unexpected handler {} {}",
          name(role),
          name(type));
      fatal = true;
    }
  }
  if (fatal) {
    std::terminate();
  }
}

void raft::must_be(raft_role role) const {
  if (_role != role) [[unlikely]] {
    l.error("{}: unexpected role {}, should be {}", *this, _role, role);
    throw util::invalid_raft_state();
  }
}

void raft::must_not_be(raft_role role) const {
  if (_role == role) [[unlikely]] {
    l.error("{}: unexpected role {}");
    throw util::invalid_raft_state();
  }
}

void raft::report_dropped_config_change(log_entry_ptr e) {
  _dropped_entries.emplace_back(std::move(e));
}

void raft::report_dropped_proposal(message& m) {
  _dropped_entries.insert(
      _dropped_entries.end(), m.entries.begin(), m.entries.end());
}

void raft::report_dropped_read_index(message& m) {
  _dropped_read_indexes.emplace_back(m.hint);
}

void raft::finalize_message(message& m) {
  if (m.term == log_id::INVALID_TERM && m.type == message_type::request_vote)
      [[unlikely]] {
    l.error("{}: sending request_vote with invalid term", *this);
    throw util::invalid_raft_state();
  }
  if (m.term != log_id::INVALID_TERM && !is_request_vote(m.type) &&
      m.type != message_type::request_prevote_resp) [[unlikely]] {
    l.error("{}: term {} should not be set for {}", *this, m.term, m.type);
    throw util::invalid_raft_state();
  }
  if (!is_request(m.type) && !is_request_vote(m.type) &&
      m.type != message_type::request_prevote_resp) {
    m.term = _term;
  }
}

future<message> raft::make_replicate(
    uint64_t to, uint64_t next, uint64_t max_bytes) {
  auto m = message{.type = message_type::replicate, .to = to};
  auto term = co_await _log.term(next - 1);
  co_await _log.query(next, m.entries, max_bytes);
  if (!m.entries.empty() &&
      m.entries.back()->lid.index != next - 1 + m.entries.size()) [[unlikely]] {
    l.error(
        "{}: potential log hole in entries, expect:{}, actual:{}",
        *this,
        next - 1 + m.entries.size(),
        m.entries.back()->lid.index);
    throw util::invalid_raft_state();
  }
  if (_witnesses.contains(to)) {
    utils::fill_metadata_entries(m.entries);
  }
  m.lid = {.term = term, .index = next - 1};
  m.commit = _log.committed();
  co_return std::move(m);
}

message raft::make_install_snapshot(uint64_t to) {
  message m{.type = message_type::install_snapshot, .to = to};
  auto ss = _log.get_snapshot();
  if (!ss || ss->log_id.index == log_id::INVALID_INDEX) {
    l.error("{}: empty snapshot", *this);
    throw util::invalid_raft_state();
  }
  if (_witnesses.contains(to)) {
    ss = make_lw_shared<snapshot>();
    ss->witness = true;
  }
  m.snapshot = std::move(ss);
  return m;
}

void raft::send(message&& m) {
  m.from = _gid.node;
  finalize_message(m);
  _messages.emplace_back(std::move(m));
}

void raft::send_timeout_now(uint64_t to) {
  send(message{.type = message_type::timeout_now, .to = to});
}

void raft::send_heartbeat(uint64_t to, hint ctx, uint64_t match_index) {
  send(message{
      .type = message_type::heartbeat,
      .to = to,
      .commit = std::min(match_index, _log.committed()),
      .hint = ctx});
}

future<> raft::send_replicate(uint64_t to, remote& r) {
  if (r.is_paused()) {
    co_return;
  }
  // TODO(jyc): refine max bytes
  try {
    auto m = co_await make_replicate(to, r.next, UINT64_MAX);
    if (!m.entries.empty()) {
      r.optimistic_update(m.entries.back()->lid.index);
    }
    send(std::move(m));
  } catch (util::compacted_error& e) {
    if (!r.active) {
      l.warn("{}: peer:{} is not active, skip snapshot", *this, to);
      co_return;
    }
    auto m = make_install_snapshot(to);
    auto index = m.snapshot->log_id.index;
    l.info("{}: sending snapshot(i:{}) to peer:{}, {}", *this, index, to, r);
    r.become_snapshot(index);
    send(std::move(m));
  }
}

void raft::broadcast_heartbeat() {
  must_be(role::leader);
  hint ctx;
  if (_read_index.has_pending_request()) {
    ctx = _read_index.peep();
  }
  auto sender = [this, ctx](auto& peer) {
    if (is_self(peer.first)) {
      return;
    }
    send_heartbeat(peer.first, ctx, peer.second.match);
  };
  std::for_each(_remotes.begin(), _remotes.end(), sender);
  std::for_each(_witnesses.begin(), _witnesses.end(), sender);
  if (ctx == hint{}) {
    std::for_each(_observers.begin(), _observers.end(), sender);
  }
}

future<> raft::broadcast_replicate() {
  must_be(role::leader);
  auto sender = [this](auto& peer) -> future<> {
    if (is_self(peer.first)) {
      co_return;
    }
    co_await send_replicate(peer.first, peer.second);
  };
  co_await do_for_each(_remotes, sender);
  co_await do_for_each(_observers, sender);
  co_await do_for_each(_witnesses, sender);
  co_return;
}

void raft::add_node(uint64_t id) {
  _pending_config_change = false;
  if (is_self(id) && is_witness()) {
    l.error("{}: self is a witness", *this);
    throw util::invalid_raft_state();
  }
  if (_remotes.contains(id)) {
    return;
  }
  if (auto oit = _observers.find(id); oit != _observers.end()) {
    _remotes.insert(_observers.extract(oit));
    if (is_self(id)) {
      become_follower(_term, _leader_id);
    }
  } else if (auto wit = _witnesses.find(id); wit != _witnesses.end()) {
    l.error("{}: cannot promote a witness:{}", *this, id);
    throw util::invalid_raft_state();
  } else {
    _remotes[id] = remote{.match = 0, .next = _log.last_index() + 1};
  }
}

void raft::add_observer(uint64_t id) {
  _pending_config_change = false;
  if (is_self(id) && !is_observer()) {
    l.error("{}: self is not an observer", *this);
    throw util::invalid_raft_state();
  }
  if (_observers.contains(id)) {
    return;
  }
  _observers[id] = remote{.match = 0, .next = _log.last_index() + 1};
}

void raft::add_witness(uint64_t id) {
  _pending_config_change = false;
  if (is_self(id) && !is_witness()) {
    l.error("{}: self is not a witness", *this);
    throw util::invalid_raft_state();
  }
  if (_witnesses.contains(id)) {
    return;
  }
  _witnesses[id] = remote{.match = 0, .next = _log.last_index() + 1};
}

seastar::future<> raft::remove_node(uint64_t id) {
  _remotes.erase(id);
  _observers.erase(id);
  _witnesses.erase(id);
  _pending_config_change = false;
  if (is_self(id) && is_leader()) {
    become_follower(_term, group_id::INVALID_NODE);
  }
  if (is_leader_transferring() && _leader_transfer_target == id) {
    abort_leader_transfer();
  }
  if (is_leader() && _remotes.size() + _witnesses.size() > 0) {
    if (try_commit()) {
      co_await broadcast_replicate();
    }
  }
}

bool raft::is_self_removed() const {
  if (is_observer()) {
    return !_observers.contains(_gid.node);
  }
  if (is_witness()) {
    return !_witnesses.contains(_gid.node);
  }
  return !_remotes.contains(_gid.node);
}

uint64_t raft::quorum() const noexcept { return voting_members_size() / 2 + 1; }

uint64_t raft::voting_members_size() const noexcept {
  return _remotes.size() + _witnesses.size();
}

void raft::set_leader(uint64_t leader_id) {
  _leader_id = leader_id;
  // TODO(jyc): event
}

future<> raft::pre_campaign() {
  become_pre_candidate();
  handle_vote_resp(_gid.node, false, true);
  if (quorum() == 1) {
    co_return co_await campaign();
  }
  auto li = _log.last_index();
  auto lt = co_await _log.last_term();
  auto sender = [this, li, lt](auto& peer) {
    if (is_self(peer.first)) {
      return;
    }
    send(message{
        .type = message_type::request_prevote,
        .to = peer.first,
        .term = _term + 1,
        .lid = {.term = lt, .index = li}});
    l.info("{}: sent request_prevote to {}", *this, peer.first);
  };
  std::for_each(_remotes.begin(), _remotes.end(), sender);
  std::for_each(_witnesses.begin(), _witnesses.end(), sender);
}

}  // namespace rafter::core
