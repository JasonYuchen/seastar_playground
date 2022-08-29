//
// Created by jason on 2021/10/9.
//

#include "raft.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

#include "core/logger.hh"
#include "util/error.hh"

namespace rafter::core {

using namespace protocol;

// TODO(jyc): add raft event listener

template <typename... Args>
void throw_with_log(logger::format_info fmt, Args&&... args) {
  l.error(fmt, std::forward<Args>(args)...);
  throw_with_backtrace<util::invalid_raft_state>();
}

raft::raft(const raft_config& cfg, log_reader& lr)
  : _config(cfg)
  , _gid{cfg.cluster_id, cfg.node_id}
  , _role(role::follower)
  , _leader_id(group_id::INVALID_NODE)
  , _leader_transfer_target(group_id::INVALID_NODE)
  , _term(log_id::INVALID_TERM)
  , _vote(group_id::INVALID_NODE)
  , _applied(log_id::INVALID_INDEX)
  , _is_leader_transfer_target(false)
  , _pending_config_change(false)
  , _quiesce(cfg.quiesce)
  , _check_quorum(cfg.check_quorum)
  , _snapshotting(false)
  , _log(_gid, lr)
  , _limiter(_config.max_in_memory_log_bytes)
  , _random_engine(std::chrono::system_clock::now().time_since_epoch().count())
  , _tick(0)
  , _election_tick(0)
  , _heartbeat_tick(0)
  , _election_timeout(cfg.election_rtt)
  , _heartbeat_timeout(cfg.heartbeat_rtt)
  , _randomized_election_timeout(0) {
  auto st = lr.get_state();
  auto member = lr.get_membership();
  if (member) {
    for (const auto& [id, address] : member->addresses) {
      _remotes[id].next = 1;
    }
    for (const auto& [id, address] : member->observers) {
      _observers[id].next = 1;
    }
    for (const auto& [id, address] : member->witnesses) {
      _witnesses[id].next = 1;
    }
  }
  reset_matched();
  set_state(st);
  if (cfg.observer) {
    _role = role::observer;
    become_observer(_term, group_id::INVALID_NODE);
  } else if (cfg.witness) {
    _role = role::witness;
    become_witness(_term, group_id::INVALID_NODE);
  } else {
    become_follower(_term, group_id::INVALID_NODE, true);
  }
  initialize_handlers();
}

std::ostream& operator<<(std::ostream& os, const raft& r) {
  return os << "raft[" << r._gid << ","
            << "r:" << r._role << ","
            << "fi:" << r._log.first_index() << ","
            << "li:" << r._log.last_index() << ","
            << "t:" << r._term << "]";
}

future<> raft::handle(protocol::message& m) {
  if (!term_not_matched(m)) {
    if (!is_prevote(m.type) && m.term != log_id::INVALID_TERM &&
        m.term != _term) {
      throw_with_log("{}: mismatched term", *this);
    }
    auto role = static_cast<uint8_t>(_role);
    assert(role < static_cast<uint8_t>(role::num_of_role));
    auto type = static_cast<uint8_t>(m.type);
    assert(type < static_cast<uint8_t>(message_type::num_of_type));
    if (_handlers[role][type] != nullptr) {
      return (this->*_handlers[role][type])(m);
    }
    return make_ready_future<>();
  }
  l.info("{}: dropped {} with term:{} from:{}", *this, m.type, m.term, m.from);
  return make_ready_future<>();
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
  D_(leader, request_prevote, node_request_prevote);
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

  D_(observer, request_vote, node_request_vote);
  D_(observer, request_prevote, node_request_prevote);
  D_(observer, config_change, node_config_change);
  D_(observer, local_tick, node_local_tick);
  D_(observer, snapshot_received, node_restore_remote);
  D_(observer, heartbeat, follower_heartbeat);
  D_(observer, propose, follower_propose);
  D_(observer, read_index, follower_read_index);
  D_(observer, read_index_resp, follower_read_index_resp);
  D_(observer, replicate, follower_replicate);
  D_(observer, install_snapshot, follower_install_snapshot);

  D_(witness, request_vote, node_request_vote);
  D_(witness, request_prevote, node_request_prevote);
  D_(witness, config_change, node_config_change);
  D_(witness, local_tick, node_local_tick);
  D_(witness, snapshot_received, node_restore_remote);
  D_(witness, heartbeat, follower_heartbeat);
  D_(witness, replicate, follower_replicate);
  D_(witness, install_snapshot, follower_install_snapshot);

#undef D_

  assert_handlers();
}

void raft::assert_handlers() {
  std::pair<raft_role, message_type> null_handlers[] = {
      {raft_role::leader, message_type::heartbeat},
      {raft_role::leader, message_type::replicate},
      {raft_role::leader, message_type::install_snapshot},
      {raft_role::leader, message_type::read_index_resp},
      {raft_role::leader, message_type::request_prevote_resp},
      {raft_role::follower, message_type::replicate_resp},
      {raft_role::follower, message_type::heartbeat_resp},
      {raft_role::follower, message_type::snapshot_status},
      {raft_role::follower, message_type::unreachable},
      {raft_role::follower, message_type::request_prevote_resp},
      {raft_role::candidate, message_type::replicate_resp},
      {raft_role::candidate, message_type::heartbeat_resp},
      {raft_role::candidate, message_type::snapshot_status},
      {raft_role::candidate, message_type::unreachable},
      {raft_role::candidate, message_type::request_prevote_resp},
      {raft_role::pre_candidate, message_type::replicate_resp},
      {raft_role::pre_candidate, message_type::heartbeat_resp},
      {raft_role::pre_candidate, message_type::snapshot_status},
      {raft_role::pre_candidate, message_type::unreachable},
      {raft_role::observer, message_type::election},
      {raft_role::observer, message_type::replicate_resp},
      {raft_role::observer, message_type::heartbeat_resp},
      {raft_role::observer, message_type::request_vote_resp},
      {raft_role::observer, message_type::request_prevote_resp},
      {raft_role::witness, message_type::election},
      {raft_role::witness, message_type::propose},
      {raft_role::witness, message_type::read_index},
      {raft_role::witness, message_type::read_index_resp},
      {raft_role::witness, message_type::replicate_resp},
      {raft_role::witness, message_type::heartbeat_resp},
      {raft_role::witness, message_type::request_vote_resp},
      {raft_role::witness, message_type::request_prevote_resp},
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

protocol::hard_state raft::state() const noexcept {
  return {.term = _term, .vote = _vote, .commit = _log.committed()};
}

void raft::set_state(hard_state s) {
  if (s.empty()) {
    return;
  }
  if (s.commit < _log.committed() || s.commit > _log.last_index()) {
    throw_with_log(
        "{}: got out of range hard state, commit:{} not in [{},{}]",
        *this,
        s.commit,
        _log.committed(),
        _log.last_index());
  }
  _term = s.term;
  _vote = s.vote;
  _log.set_committed(s.commit);
}

void raft::must_be(raft_role role) const {
  if (_role != role) [[unlikely]] {
    throw_with_log("{}: unexpected role {}, should be {}", *this, _role, role);
  }
}

void raft::must_not_be(raft_role role) const {
  if (_role == role) [[unlikely]] {
    throw_with_log("{}: unexpected role {}", *this, _role);
  }
}

void raft::report_dropped_config_change(log_entry e) {
  _dropped_entries.emplace_back(std::move(e));
}

void raft::report_dropped_proposal(message& m) {
  for (auto& e : m.entries) {
    _dropped_entries.emplace_back(e.share());
  }
}

void raft::report_dropped_read_index(message& m) {
  _dropped_read_indexes.emplace_back(m.hint);
}

void raft::finalize_message(message& m) {
  if (m.term == log_id::INVALID_TERM && m.type == message_type::request_vote)
      [[unlikely]] {
    throw_with_log("{}: sending request_vote with invalid term", *this);
  }
  if (m.term != log_id::INVALID_TERM && !is_request_vote(m.type) &&
      m.type != message_type::request_prevote_resp) [[unlikely]] {
    throw_with_log(
        "{}: term {} should not be set for {}", *this, m.term, m.type);
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
      m.entries.back().lid.index != next - 1 + m.entries.size()) [[unlikely]] {
    throw_with_log(
        "{}: potential log hole in entries, expect:{}, actual:{}",
        *this,
        next - 1 + m.entries.size(),
        m.entries.back().lid.index);
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
    throw_with_log("{}: empty snapshot", *this);
  }
  if (_witnesses.contains(to)) {
    ss = make_lw_shared<snapshot>();
    ss->witness = true;
  }
  m.snapshot = std::move(ss);
  return m;
}

bool raft::term_not_matched(message& m) {
  if (m.term == log_id::INVALID_TERM || m.term == _term) {
    return false;
  }
  if (drop_request_vote(m)) {
    l.warn(
        "{}: dropped request vote at term:{} from:{}, leader:{} available",
        *this,
        _term,
        m.from,
        _leader_id);
    return true;
  }
  if (m.term > _term) {
    bool expected_prevote =
        (m.type == message_type::request_prevote) ||
        (m.type == message_type::request_prevote_resp && !m.reject);
    if (!expected_prevote) {
      l.warn(
          "{}: received {} with higher term:{} from:{}",
          *this,
          m.type,
          m.term,
          m.from);
      auto leader =
          protocol::is_leader(m.type) ? m.from : group_id::INVALID_NODE;
      if (is_observer()) {
        become_observer(m.term, leader);
      } else if (is_witness()) {
        become_witness(m.term, leader);
      } else {
        become_follower(m.term, leader, m.type != message_type::request_vote);
      }
    }
  } else {
    // m.term < _term
    if (m.type == message_type::request_prevote ||
        protocol::is_leader(m.type)) {
      send(message{.type = message_type::noop, .to = m.from});
    } else {
      l.info(
          "{}: ignored {} with lower term:{} from:{}",
          *this,
          m.type,
          m.term,
          m.from);
    }
    return true;
  }
  return false;
}

bool raft::drop_request_vote(message& m) {
  if (!is_request_vote(m.type) || !_check_quorum || m.term <= _term) {
    return false;
  }
  if (m.hint.low == m.from) {
    l.info(
        "{}: received {} with leader transfer hint:{} from:{}",
        *this,
        m.type,
        m.hint.low,
        m.from);
    return false;
  }
  if (is_leader() && !_quiesce && _election_tick >= _election_timeout) {
    throw_with_log(
        "{}: leader has election tick:{} > timeout:{}",
        *this,
        _election_tick,
        _election_timeout);
  }
  return _leader_id != group_id::INVALID_NODE &&
         _election_tick < _election_timeout;
}

void raft::add_ready_to_read(uint64_t index, hint ctx) {
  _ready_to_reads.push_back(ready_to_read{index, ctx});
}

remote* raft::get_peer(uint64_t node_id) noexcept {
  if (auto rit = _remotes.find(node_id); rit != _remotes.end()) {
    return &rit->second;
  }
  if (auto oit = _observers.find(node_id); oit != _observers.end()) {
    return &oit->second;
  }
  if (auto wit = _witnesses.find(node_id); wit != _witnesses.end()) {
    return &wit->second;
  }
  l.warn("{}: failed to find peer {}", *this, node_id);
  return nullptr;
}

void raft::send(message&& m) {
  m.from = _gid.node;
  finalize_message(m);
  _messages.emplace_back(std::move(m));
}

void raft::send_timeout_now(uint64_t to) {
  send(message{.type = message_type::timeout_now, .to = to});
}

void raft::send_rate_limit() {
  must_not_be(role::leader);
  if (_leader_id == group_id::INVALID_NODE) {
    l.info("{}: skip rate limit since no leader available", *this);
    return;
  }
  if (_limiter.enabled()) {
    return;
  }
  // TODO(jyc): prepare rate limit message
  send(message{
      .type = message_type::rate_limit, .to = _leader_id, .hint = {.low = 0}});
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
  try {
    auto m = co_await make_replicate(
        to, r.next, config::shard().max_replicate_entry_bytes);
    if (!m.entries.empty()) {
      r.optimistic_update(m.entries.back().lid.index);
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
    throw_with_log("{}: self is a witness", *this);
  }
  if (_remotes.contains(id)) {
    return;
  }
  if (auto oit = _observers.find(id); oit != _observers.end()) {
    _remotes.insert(_observers.extract(oit));
    if (is_self(id)) {
      become_follower(_term, _leader_id, true);
    }
  } else if (auto wit = _witnesses.find(id); wit != _witnesses.end()) {
    throw_with_log("{}: cannot promote a witness:{}", *this, id);
  } else {
    _remotes[id] = remote{.match = 0, .next = _log.last_index() + 1};
  }
}

void raft::add_observer(uint64_t id) {
  _pending_config_change = false;
  if (is_self(id) && !is_observer()) {
    throw_with_log("{}: self is not an observer", *this);
  }
  if (_observers.contains(id)) {
    return;
  }
  _observers[id] = remote{.match = 0, .next = _log.last_index() + 1};
}

void raft::add_witness(uint64_t id) {
  _pending_config_change = false;
  if (is_self(id) && !is_witness()) {
    throw_with_log("{}: self is not a witness", *this);
  }
  if (_witnesses.contains(id)) {
    return;
  }
  _witnesses[id] = remote{.match = 0, .next = _log.last_index() + 1};
}

future<> raft::remove_node(uint64_t id) {
  _remotes.erase(id);
  _observers.erase(id);
  _witnesses.erase(id);
  _pending_config_change = false;
  if (is_self(id) && is_leader()) {
    become_follower(_term, group_id::INVALID_NODE, true);
  }
  if (is_leader_transferring() && _leader_transfer_target == id) {
    abort_leader_transfer();
  }
  if (is_leader() && _remotes.size() + _witnesses.size() > 0) {
    if (co_await try_commit()) {
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

bool raft::has_quorum() {
  uint64_t cnt = 0;
  for (auto& [id, r] : _remotes) {
    if (id == _gid.node || r.active) {
      cnt++;
      r.active = false;
    }
  }
  for (auto& [id, r] : _witnesses) {
    if (id == _gid.node || r.active) {
      cnt++;
      r.active = false;
    }
  }
  return cnt >= quorum();
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

future<> raft::campaign() {
  become_candidate();
  handle_vote_resp(_gid.node, false, false);
  if (quorum() == 1) {
    co_return co_await become_leader();
  }
  uint64_t hint = group_id::INVALID_NODE;
  if (_is_leader_transfer_target) {
    hint = _gid.node;
    _is_leader_transfer_target = false;
  }
  auto li = _log.last_index();
  auto lt = co_await _log.last_term();
  auto sender = [this, li, lt, hint](auto& peer) {
    if (is_self(peer.first)) {
      return;
    }
    send(message{
        .type = message_type::request_vote,
        .to = peer.first,
        .term = _term,
        .lid = {.term = lt, .index = li},
        .hint = {.low = hint}});
    l.info("{}: sent request_vote to {}", *this, peer.first);
  };
  std::for_each(_remotes.begin(), _remotes.end(), sender);
  std::for_each(_witnesses.begin(), _witnesses.end(), sender);
}

uint64_t raft::handle_vote_resp(uint64_t from, bool rejected, bool prevote) {
  const auto* type = prevote ? "request_prevote" : "request_vote";
  if (rejected) {
    l.warn("{}: received {} rejection from {}", *this, type, from);
  } else {
    l.warn("{}: received {} from {}", *this, type, from);
  }
  if (!_votes.contains(from)) {
    _votes[from] = !rejected;
  }
  return std::accumulate(
      _votes.begin(), _votes.end(), uint64_t{0}, [](uint64_t votes, auto peer) {
        return votes + peer.second;
      });
}

bool raft::can_grant_vote(uint64_t peer_id, uint64_t peer_term) const noexcept {
  return _vote == group_id::INVALID_NODE || _vote == peer_id ||
         peer_term > _term;
}

future<> raft::become_leader() {
  if (!is_leader() && !is_candidate()) {
    l.error("{}: unexpected transitioning to leader", *this);
    co_await coroutine::return_exception(util::invalid_raft_state());
  }
  _role = role::leader;
  reset(_term, true);
  set_leader(_gid.node);
  auto count = co_await _log.pending_config_change_count();
  if (count > 1) {
    throw_with_log(
        "{}: become leader with {} pending config change", *this, count);
  }
  if (count == 1) {
    _pending_config_change = true;
  }
  l.info("{}: become leader with {} pending config change", *this, count);
  co_await append_noop_entry();
}

void raft::become_candidate() {
  must_not_be(role::leader);
  must_not_be(role::observer);
  must_not_be(role::witness);
  _role = role::candidate;
  reset(_term + 1, true);
  set_leader(group_id::INVALID_NODE);
  _vote = _gid.node;
  l.info("{}: become candidate", *this);
}

void raft::become_pre_candidate() {
  must_not_be(role::leader);
  must_not_be(role::observer);
  must_not_be(role::witness);
  _role = role::pre_candidate;
  reset(_term, true);
  set_leader(group_id::INVALID_NODE);
  l.info("{}: become pre_candidate", *this);
}

void raft::become_follower(
    uint64_t term, uint64_t leader_id, bool reset_election) {
  must_not_be(role::witness);
  _role = role::follower;
  reset(term, reset_election);
  set_leader(leader_id);
  l.info("{}: become follower", *this);
}

void raft::become_observer(uint64_t term, uint64_t leader_id) {
  must_be(role::observer);
  reset(term, true);
  set_leader(leader_id);
  l.info("{}: become observer", *this);
}

void raft::become_witness(uint64_t term, uint64_t leader_id) {
  must_be(role::witness);
  reset(term, true);
  set_leader(leader_id);
  l.info("{}: become witness", *this);
}

bool raft::is_leader_transferring() const noexcept {
  return _leader_transfer_target != group_id::INVALID_NODE && is_leader();
}

void raft::abort_leader_transfer() {
  _leader_transfer_target = group_id::INVALID_NODE;
}

void raft::reset(uint64_t term, bool reset_election_timeout) {
  if (_term != term) {
    _term = term;
    _vote = group_id::INVALID_NODE;
  }
  if (_limiter.enabled()) {
    _limiter.reset();
  }
  if (reset_election_timeout) {
    _election_tick = 0;
    set_randomized_election_timeout();
  }
  _votes.clear();
  _heartbeat_tick = 0;
  _read_index.clear();
  _pending_config_change = false;
  abort_leader_transfer();
  reset_matched();
  auto resetter = [this](auto& peer) {
    peer.second = remote{.next = _log.last_index() + 1};
    if (is_self(peer.first)) {
      peer.second.match = _log.last_index();
    }
  };
  std::for_each(_remotes.begin(), _remotes.end(), resetter);
  std::for_each(_observers.begin(), _observers.end(), resetter);
  std::for_each(_witnesses.begin(), _witnesses.end(), resetter);
}

void raft::reset_matched() {
  _matched.resize(voting_members_size());
  std::fill(_matched.begin(), _matched.end(), 0);
}

future<bool> raft::restore(snapshot_ptr ss) {
  if (ss->log_id.index <= _log.committed()) {
    l.warn(
        "{}: snapshot ignored, index:{} <= committed:{}",
        *this,
        ss->log_id.index,
        _log.committed());
    co_return false;
  }
  if (!is_observer()) {
    if (ss->membership->observers.contains(_gid.node)) {
      throw_with_log(
          "{}: converting to observer, index:{}, committed:{}",
          *this,
          ss->log_id.index,
          _log.committed());
    }
  }
  if (!is_witness()) {
    if (ss->membership->witnesses.contains(_gid.node)) {
      throw_with_log(
          "{}: converting to witness, index:{}, committed:{}",
          *this,
          ss->log_id.index,
          _log.committed());
    }
  }
  auto match = co_await _log.term_index_match(ss->log_id);
  if (match) {
    _log.commit(ss->log_id.index);
    co_return false;
  }
  l.info("{}: start to restore snapshot, {}", *this, ss->log_id);
  _log.restore(std::move(ss));
  co_return true;
}

future<bool> raft::try_commit() {
  must_be(role::leader);
  if (_matched.size() != voting_members_size()) {
    reset_matched();
  }
  size_t idx = 0;
  for (const auto& [_, r] : _remotes) {
    _matched[idx++] = r.match;
  }
  for (const auto& [_, r] : _witnesses) {
    _matched[idx++] = r.match;
  }
  std::sort(_matched.begin(), _matched.end());
  auto m = _matched[voting_members_size() - quorum()];
  return _log.try_commit({.term = _term, .index = m});
}

future<> raft::append_entries(log_entry_vector& entries) {
  auto li = _log.last_index();
  for (size_t i = 0; i < entries.size(); ++i) {
    entries[i].lid = {.term = _term, .index = li + i + 1};
  }
  _log.append(entries);
  _remotes[_gid.node].try_update(_log.last_index());
  if (quorum() == 1) {
    co_await try_commit();
  }
  co_return;
}

future<> raft::append_noop_entry() {
  log_entry noop{_term, _log.last_index() + 1};
  noop.type = entry_type::application;
  _log.append({&noop, 1});
  _remotes[_gid.node].try_update(_log.last_index());
  if (quorum() == 1) {
    co_await try_commit();
  }
  co_return;
}

future<bool> raft::has_committed_entry() {
  if (_term == log_id::INVALID_TERM) {
    throw_with_log("{}: invalid term", *this);
  }
  try {
    co_return _term == co_await _log.term(_log.committed());
  } catch (util::compacted_error& e) {
    l.warn("{}: log compacted", *this);
  }
  co_return false;
}

future<> raft::tick() {
  _quiesce = false;
  _tick++;
  // TODO(jyc): in memory gc
  if (is_leader()) {
    return leader_tick();
  }
  return nonleader_tick();
}

future<> raft::leader_tick() {
  must_be(role::leader);
  _election_tick++;
  if (time_to_check_rate_limit()) {
    if (_limiter.enabled()) {
      _limiter.tick();
    }
  }
  auto abort = time_to_abort_leader_transfer();
  if (time_to_check_quorum()) {
    _election_tick = 0;
    if (_check_quorum) {
      co_await handle(
          message{.type = message_type::check_quorum, .from = _gid.node});
    }
  }
  if (abort) {
    abort_leader_transfer();
  }
  _heartbeat_tick++;
  if (time_to_heartbeat()) {
    _heartbeat_tick = 0;
    co_await handle(
        message{.type = message_type::leader_heartbeat, .from = _gid.node});
  }
  // TODO(jyc): check pending snapshot ack
  co_return;
}

future<> raft::nonleader_tick() {
  must_not_be(role::leader);
  _election_tick++;
  if (time_to_check_rate_limit()) {
    if (_limiter.enabled()) {
      _limiter.tick();
      send_rate_limit();
    }
  }
  if (is_observer() || is_witness()) {
    co_return;
  }
  if (!is_self_removed() && time_to_elect()) {
    _election_tick = 0;
    co_await handle(message{.type = message_type::election, .from = _gid.node});
  }
  co_return;
}

void raft::quiesced_tick() {
  if (!_quiesce) {
    _quiesce = true;
    // resize in memory log
  }
  _election_tick++;
}

void raft::leader_is_available(uint64_t leader_id) noexcept {
  _election_tick = 0;
  _leader_id = leader_id;
}

bool raft::time_to_elect() const noexcept {
  return _election_tick >= _randomized_election_timeout;
}

bool raft::time_to_heartbeat() const noexcept {
  return _heartbeat_tick >= _heartbeat_timeout;
}

bool raft::time_to_check_quorum() const noexcept {
  return _election_tick >= _election_timeout;
}

bool raft::time_to_abort_leader_transfer() const noexcept {
  return is_leader_transferring() && _election_tick >= _election_timeout;
}

bool raft::time_to_gc() const noexcept {
  return _tick % config::shard().in_memory_gc_timeout == 0;
}

bool raft::time_to_check_rate_limit() const noexcept {
  return _tick % _election_timeout == 0;
}

void raft::set_randomized_election_timeout() {
  _randomized_election_timeout =
      _election_timeout + _random_engine() % _election_timeout;
}

future<> raft::node_election(message& m) {
  if (!is_leader()) {
    // there can be multiple pending membership change entries committed but not
    // applied on this node. say with a cluster of X, Y and Z, there are two
    // such entries for adding node A and B are committed but not applied
    // available on X. If X is allowed to start a new election, it can become a
    // leader with a vote from any one of the node Y or Z. Further proposals
    // made by the new leader X in the next term will require a quorum of 2
    // which can have no overlap with the committed quorum of 3. this violates
    // the safety requirement of raft. ignore the Election message when there is
    // membership configure change committed but not applied
    if (_log.has_config_change_to_apply()) {
      l.warn("{}: election skipped due to not applied config change", *this);
      co_return;
    }
    co_return co_await pre_campaign();
  }
  l.info("{}: leader ignored election", *this);
  co_return;
}

future<> raft::node_request_vote(message& m) {
  message resp{.type = message_type::request_vote_resp, .to = m.from};
  bool can_grant = can_grant_vote(m.from, m.term);
  bool up_to_date = co_await _log.up_to_date(m.lid);
  if (can_grant && up_to_date) {
    l.info(
        "{}: cast vote from {} with term:{}, {}", *this, m.from, m.term, m.lid);
    _election_tick = 0;
    _vote = m.from;
  } else {
    l.warn(
        "{}: rejected vote {} with term:{}, {}, can_grant:{}, up_to_date:{}",
        *this,
        m.from,
        m.term,
        m.lid,
        can_grant,
        up_to_date);
    resp.reject = true;
  }
  send(std::move(resp));
  co_return;
}

future<> raft::node_request_prevote(message& m) {
  message resp{.type = message_type::request_prevote_resp, .to = m.from};
  bool up_to_date = co_await _log.up_to_date(m.lid);
  if (m.term < _term) {
    throw_with_log("m.term < r.term");
  }
  if (m.term > _term && up_to_date) {
    l.info(
        "{}: cast prevote from {} with term:{}, {}",
        *this,
        m.from,
        m.term,
        m.lid);
    resp.term = m.term;
  } else {
    l.warn(
        "{}: rejected prevote {} with term:{}, {}, up_to_date:{}",
        *this,
        m.from,
        m.term,
        m.lid,
        up_to_date);
    resp.term = _term;
    resp.reject = true;
  }
  send(std::move(resp));
  co_return;
}

future<> raft::node_config_change(message& m) {
  if (m.reject) {
    _pending_config_change = false;
    co_return;
  }
  auto type = static_cast<config_change_type>(m.hint.high);
  auto id = m.hint.low;
  switch (type) {
    case config_change_type::add_node:
      add_node(id);
      break;
    case config_change_type::remove_node:
      co_await remove_node(id);
      break;
    case config_change_type::add_observer:
      add_observer(id);
      break;
    case config_change_type::add_witness:
      add_witness(id);
      break;
    default:
      throw_with_log("unexpected config_change_type");
  }
  co_return;
}

future<> raft::node_local_tick(message& m) {
  if (m.reject) {
    quiesced_tick();
    co_return;
  }
  co_await tick();
}

future<> raft::node_restore_remote(message& m) {
  auto next = _log.last_index() + 1;
  _remotes.clear();
  for (const auto& [id, addr] : m.snapshot->membership->addresses) {
    if (id == _gid.node && is_observer()) {
      become_follower(_term, _leader_id, true);
    }
    if (_witnesses.contains(id)) {
      throw_with_log("cannot promote a witness to a full member");
    }
    auto match = id == _gid.node ? next - 1 : 0;
    _remotes[id] = remote{.match = match, .next = next};
    l.debug(
        "{}: restored remote {} to match:{}, next:{}", *this, id, match, next);
  }
  if (is_self_removed() && is_leader()) {
    become_follower(_term, group_id::INVALID_NODE, true);
  }
  _observers.clear();
  for (const auto& [id, addr] : m.snapshot->membership->observers) {
    auto match = id == _gid.node ? next - 1 : 0;
    _observers[id] = remote{.match = match, .next = next};
    l.debug(
        "{}: restored observer {} to match:{}, next:{}",
        *this,
        id,
        match,
        next);
  }
  _witnesses.clear();
  for (const auto& [id, addr] : m.snapshot->membership->witnesses) {
    auto match = id == _gid.node ? next - 1 : 0;
    _witnesses[id] = remote{.match = match, .next = next};
    l.debug(
        "{}: restored witness {} to match:{}, next:{}", *this, id, match, next);
  }
  reset_matched();
  co_return;
}

future<> raft::node_heartbeat(message& m) {
  _log.commit(m.commit);
  send(message{
      .type = message_type::heartbeat_resp, .to = m.from, .hint = m.hint});
  co_return;
}

future<> raft::node_replicate(message& m) {
  auto resp = message{.type = message_type::replicate_resp, .to = m.from};
  if (m.lid.index < _log.committed()) {
    resp.lid.index = _log.committed();
    send(std::move(resp));
    co_return;
  }
  bool match = co_await _log.term_index_match(m.lid);
  if (match) {
    co_await _log.try_append(m.lid.index, m.entries);
    auto last_index = m.lid.index + m.entries.size();
    _log.commit(std::min(last_index, m.commit));
    resp.lid.index = last_index;
  } else {
    l.debug(
        "{}: rejected replicate from {} term:{} with {}",
        *this,
        m.from,
        m.term,
        m.lid);
    resp.reject = true;
    resp.lid.index = m.lid.index;
    resp.hint.low = _log.last_index();
  }
  send(std::move(resp));
  co_return;
}

future<> raft::node_install_snapshot(message& m) {
  l.debug("{}: install snapshot from {}", *this, m.from);
  auto resp = message{.type = message_type::replicate_resp, .to = m.from};
  bool restored = co_await restore(m.snapshot);
  if (restored) {
    l.debug("{}: restored snapshot with {}", *this, m.snapshot->log_id);
    resp.lid.index = _log.last_index();
  } else {
    l.debug("{}: rejected snapshot with {}", *this, m.snapshot->log_id);
    resp.lid.index = _log.committed();
  }
  send(std::move(resp));
  co_return;
}

future<> raft::leader_heartbeat(message& m) {
  broadcast_heartbeat();
  co_return;
}

future<> raft::leader_heartbeat_resp(message& m) {
  must_be(role::leader);
  remote* r = get_peer(m.from);
  if (r == nullptr) {
    co_return;
  }
  r->active = true;
  r->wait_to_retry();
  if (r->match < _log.last_index()) {
    co_await send_replicate(m.from, *r);
  }
  // heartbeat response contains leadership confirmation requested as part of
  // the read index protocol.
  if (m.hint.low != log_id::INVALID_INDEX) {
    _read_index.confirm(
        m.hint,
        m.from,
        quorum(),
        [this, hint = m.hint](const read_index::status& s) {
          if (s.from == group_id::INVALID_NODE || s.from == _gid.node) {
            add_ready_to_read(s.index, s.ctx);
          } else {
            send(message{
                .type = message_type::read_index_resp,
                .to = s.from,
                .lid = {.index = s.index},
                .hint = hint});
          }
        });
  }
  co_return;
}

future<> raft::leader_check_quorum(message& m) {
  must_be(role::leader);
  if (!has_quorum()) {
    l.warn("{}: lost quorum, step down", *this);
    become_follower(_term, group_id::INVALID_NODE, true);
  }
  co_return;
}

future<> raft::leader_propose(message& m) {
  must_be(role::leader);
  if (is_leader_transferring()) {
    l.warn("{}: dropped proposal due to leader transferring", *this);
    report_dropped_proposal(m);
    co_return;
  }
  for (auto& e : m.entries) {
    if (e.type == entry_type::config_change) {
      if (_pending_config_change) {
        l.warn("{}: dropped config change due to pending change", *this);
        auto lid = e.lid;
        report_dropped_config_change(std::move(e));
        e = log_entry{lid};
        e.type = entry_type::application;
      }
      _pending_config_change = true;
    }
  }
  co_await append_entries(m.entries);
  co_await broadcast_replicate();
}

future<> raft::leader_read_index(message& m) {
  must_be(role::leader);
  if (_witnesses.contains(m.from)) {
    l.error("{}: dropped witness read index from {}", *this, m.from);
    co_return;
  }
  if (quorum() == 1) {
    add_ready_to_read(_log.committed(), m.hint);
    if (m.from != _gid.node && _observers.contains(m.from)) {
      // single node quorum leader received request from observers
      send(message{
          .type = message_type::read_index_resp,
          .to = m.from,
          .lid = {.index = _log.committed()},
          .commit = m.commit,
          .hint = m.hint});
    }
    co_return;
  }
  if (!co_await has_committed_entry()) {
    // leader doesn't know the commit value of the cluster
    // see raft thesis section 6.4, this is the first step of the read index
    // protocol.
    l.warn("{}: dropped read index due to leader not ready", *this);
    report_dropped_read_index(m);
    co_return;
  }
  _read_index.add_request(_log.committed(), m.hint, m.from);
  broadcast_heartbeat();
}

future<> raft::leader_replicate_resp(message& m) {
  must_be(role::leader);
  remote* r = get_peer(m.from);
  if (r == nullptr) {
    co_return;
  }
  r->active = true;
  if (m.reject) {
    // the replication flow control code is derived from etcd raft, it resets
    // next to match + 1. it is thus even more conservative than the raft
    // thesis's approach of next = next - 1 mentioned on the p21 of
    // the thesis.
    if (r->try_decrease(m.lid.index, m.hint.low)) {
      r->replicate_to_retry();
      co_await send_replicate(m.from, *r);
    }
    co_return;
  }
  bool paused = r->is_paused();
  if (r->try_update(m.lid.index)) {
    r->responded_to();
    if (co_await try_commit()) {
      co_await broadcast_replicate();
    } else if (paused) {
      co_await send_replicate(m.from, *r);
    }
    // according to the leadership transfer protocol listed on the p29 of the
    // raft thesis
    if (is_leader_transferring() && m.from == _leader_transfer_target &&
        _log.last_index() == r->match) {
      send_timeout_now(m.from);
    }
  }
}

future<> raft::leader_snapshot_status(message& m) {
  must_be(role::leader);
  remote* r = get_peer(m.from);
  if (r == nullptr) {
    co_return;
  }
  if (r->state != remote::state::snapshot) {
    co_return;
  }
  if (m.hint.low != 0) {
    r->set_snapshot_ack(m.hint.low, m.reject);
    _snapshotting = true;
    co_return;
  }
  if (m.reject) {
    r->clear_pending_snapshot();
    l.warn("{}: snapshot failed, peer {} in wait state", *this, m.from);
  } else {
    l.debug(
        "{}: snapshot succeeded, peer {} in wait state, next:{}",
        *this,
        m.from,
        r->next);
  }
  r->become_wait();
}

future<> raft::leader_unreachable(message& m) {
  must_be(role::leader);
  l.debug("{}: {} unreachable", *this, m.from);
  remote* r = get_peer(m.from);
  if (r == nullptr) {
    co_return;
  }
  r->replicate_to_retry();
  co_return;
}

future<> raft::leader_leader_transfer(message& m) {
  must_be(role::leader);
  auto target = m.hint.low;
  if (target == group_id::INVALID_NODE) {
    throw_with_log("leader transfer target not set");
  }
  if (is_leader_transferring()) {
    l.warn("{}: ignored leader transfer due to an ongoing one", *this);
    co_return;
  }
  if (target == _gid.node) {
    l.warn("{}: ignored leader transfer due to transferring to self", *this);
    co_return;
  }
  auto it = _remotes.find(target);
  if (it == _remotes.end()) {
    l.warn("{}: ignored leader transfer due to unknown target", *this);
    co_return;
  }
  _leader_transfer_target = target;
  _election_tick = 0;
  // fast path below
  // or wait for the target node to catch up, see p29 of the raft thesis
  if (it->second.match == _log.last_index()) {
    send_timeout_now(target);
  }
}

future<> raft::leader_rate_limit(message& m) {
  if (_limiter.enabled()) {
    _limiter.set_peer(m.from, m.hint.low);
  } else {
    l.warn("{}: ignored rate limit due to disabled limiter", *this);
  }
  co_return;
}

future<> raft::candidate_heartbeat(message& m) {
  become_follower(_term, m.from, true);
  return node_heartbeat(m);
}

future<> raft::candidate_propose(message& m) {
  l.warn("{}: dropped proposal due to no leader", *this);
  report_dropped_proposal(m);
  co_return;
}

future<> raft::candidate_read_index(message& m) {
  l.warn("{}: dropped read index due to no leader", *this);
  report_dropped_read_index(m);
  co_return;
}

future<> raft::candidate_replicate(message& m) {
  become_follower(_term, m.from, true);
  return node_replicate(m);
}

future<> raft::candidate_install_snapshot(message& m) {
  become_follower(_term, m.from, true);
  return node_install_snapshot(m);
}

future<> raft::candidate_request_vote_resp(message& m) {
  if (_observers.contains(m.from)) {
    l.warn("{}: dropped request vote resp from observer {}", *this, m.from);
    co_return;
  }
  auto count = handle_vote_resp(m.from, m.reject, false);
  l.info(
      "{}: received {} votes and {} rejections, quorum is {}",
      *this,
      count,
      _votes.size() - count,
      quorum());
  if (count == quorum()) {
    co_await become_leader();
    co_await broadcast_replicate();
  } else if (_votes.size() - count == quorum()) {
    // if rejected by majority, directly become follower, etcd raft does this
    become_follower(_term, group_id::INVALID_NODE, true);
  }
}

future<> raft::pre_candidate_request_prevote_resp(message& m) {
  if (_observers.contains(m.from)) {
    l.warn("{}: dropped request prevote resp from observer {}", *this, m.from);
    co_return;
  }
  auto count = handle_vote_resp(m.from, m.reject, true);
  l.info(
      "{}: received {} prevotes and {} rejections, quorum is {}",
      *this,
      count,
      _votes.size() - count,
      quorum());
  if (count == quorum()) {
    co_await campaign();
  } else if (_votes.size() - count == quorum()) {
    // if rejected by majority, directly become follower, etcd raft does this
    become_follower(_term, group_id::INVALID_NODE, true);
  }
}

future<> raft::follower_heartbeat(message& m) {
  leader_is_available(m.from);
  return node_heartbeat(m);
}

future<> raft::follower_propose(message& m) {
  if (_leader_id == group_id::INVALID_NODE) {
    l.warn("{}: dropped propose due to no leader", *this);
    report_dropped_proposal(m);
    co_return;
  }
  m.to = _leader_id;
  send(std::move(m));
}

future<> raft::follower_read_index(message& m) {
  if (_leader_id == group_id::INVALID_NODE) {
    l.warn("{}: dropped read index due to no leader", *this);
    report_dropped_read_index(m);
    co_return;
  }
  m.to = _leader_id;
  send(std::move(m));
}

future<> raft::follower_read_index_resp(message& m) {
  leader_is_available(m.from);
  add_ready_to_read(m.lid.index, m.hint);
  co_return;
}

future<> raft::follower_replicate(message& m) {
  leader_is_available(m.from);
  return node_replicate(m);
}

future<> raft::follower_leader_transfer(message& m) {
  if (_leader_id == group_id::INVALID_NODE) {
    l.warn("{}: dropped leader transfer due to no leader", *this);
    co_return;
  }
  m.to = _leader_id;
  send(std::move(m));
}

future<> raft::follower_install_snapshot(message& m) {
  leader_is_available(m.from);
  return node_install_snapshot(m);
}

future<> raft::follower_timeout_now(message& m) {
  // the last paragraph, p29 of the raft thesis mentions that this is nothing
  // different from the clock moving forward quickly
  l.info("{}: timeout now", *this);
  _election_tick = _randomized_election_timeout;
  _is_leader_transfer_target = true;
  co_await tick();
  _is_leader_transfer_target = false;
}

}  // namespace rafter::core
