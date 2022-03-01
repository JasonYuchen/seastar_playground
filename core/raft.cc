//
// Created by jason on 2021/10/9.
//

#include "raft.hh"

#include "core/logger.hh"

namespace rafter::core {

using namespace protocol;
using namespace seastar;

// TODO(jyc): add raft event listener

template <typename... Args>
void throw_with_log(logger::format_info fmt, Args&&... args) {
  l.error(fmt, std::forward<Args>(args)...);
  throw util::invalid_raft_state();
}

std::ostream& operator<<(std::ostream& os, const raft& r) {
  return os << "raft[" << r._gid << ","
            << "r:" << r._role << ","
            << "fi:" << r._log.first_index() << ","
            << "li:" << r._log.last_index() << ","
            << "t:" << r._term << "]";
}

seastar::future<> raft::handle(protocol::message& m) {
  if (!term_not_matched(m)) {
    if (!is_prevote(m.type) && m.term != log_id::INVALID_TERM &&
        m.term != _term) {
      throw_with_log("{}: mismatched term", *this);
    }
    auto role = static_cast<uint8_t>(_role);
    assert(role < static_cast<uint8_t>(role::num_of_role));
    auto type = static_cast<uint8_t>(m.type);
    assert(type < static_cast<uint8_t>(message_type::num_of_type));
    return (this->*_handlers[role][type])(m);
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
    throw_with_log("{}: unexpected role {}, should be {}", *this, _role, role);
  }
}

void raft::must_not_be(raft_role role) const {
  if (_role == role) [[unlikely]] {
    throw_with_log("{}: unexpected role {}");
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
      m.entries.back()->lid.index != next - 1 + m.entries.size()) [[unlikely]] {
    throw_with_log(
        "{}: potential log hole in entries, expect:{}, actual:{}",
        *this,
        next - 1 + m.entries.size(),
        m.entries.back()->lid.index);
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
  if (_leader_id != group_id::INVALID_NODE &&
      _election_tick < _election_timeout) {
    return true;
  }
  return false;
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

seastar::future<> raft::remove_node(uint64_t id) {
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
  auto type = prevote ? "request_prevote" : "request_vote";
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
    throw util::invalid_raft_state();
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
    entries[i]->lid = {.term = _term, .index = li + i + 1};
  }
  _log.append(entries);
  _remotes[_gid.node].try_update(_log.last_index());
  if (quorum() == 1) {
    co_await try_commit();
  }
  co_return;
}

future<> raft::append_noop_entry() {
  auto noop = make_lw_shared<log_entry>();
  noop->type = entry_type::application;
  noop->lid = {.term = _term, .index = _log.last_index() + 1};
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
  return _tick % _config.in_memory_gc_timeout == 0;
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

}  // namespace rafter::core
