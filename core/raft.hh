//
// Created by jason on 2021/10/9.
//

#pragma once

#include <random>

#include "core/raft_log.hh"
#include "core/rate_limiter.hh"
#include "core/read_index.hh"
#include "core/remote.hh"
#include "protocol/raft.hh"
#include "rafter/config.hh"
#include "util/seastarx.hh"
#include "util/types.hh"

namespace rafter::test {

class helper;

}  // namespace rafter::test

namespace rafter::core {

class raft {
 public:
  struct status {
    protocol::group_id gid;
    uint64_t applied = protocol::log_id::INVALID_INDEX;
    uint64_t leader_id = protocol::group_id::INVALID_NODE;
    protocol::raft_role role;
    protocol::hard_state state;
  };

  raft(const raft_config& cfg, log_reader& lr);

  future<> handle(protocol::message& m);
  future<> handle(protocol::message&& m) { return handle(m); }

  auto& messages() { return _messages; }
  auto& ready_to_reads() { return _ready_to_reads; }
  auto& dropped_entries() { return _dropped_entries; }
  auto& dropped_read_indexes() { return _dropped_read_indexes; }

 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(raft);

  friend class peer;
  friend class test::helper;
  friend std::ostream& operator<<(std::ostream& os, const raft& r);
  using role = protocol::raft_role;

  // misc
  void initialize_handlers();
  void assert_handlers();
  protocol::hard_state state() const noexcept;
  void set_state(protocol::hard_state s);
  bool is_self(uint64_t id) const noexcept { return _gid.node == id; }
  bool is_leader() const noexcept { return _role == role::leader; }
  bool is_candidate() const noexcept { return _role == role::candidate; }
  bool is_follower() const noexcept { return _role == role::follower; }
  bool is_observer() const noexcept { return _role == role::observer; }
  bool is_witness() const noexcept { return _role == role::witness; }
  void must_be(protocol::raft_role role) const;
  void must_not_be(protocol::raft_role role) const;
  void report_dropped_config_change(protocol::log_entry_ptr e);
  void report_dropped_proposal(protocol::message& m);
  void report_dropped_read_index(protocol::message& m);
  void finalize_message(protocol::message& m);
  future<protocol::message> make_replicate(
      uint64_t to, uint64_t next, uint64_t max_bytes);
  protocol::message make_install_snapshot(uint64_t to);
  bool term_not_matched(protocol::message& m);
  bool drop_request_vote(protocol::message& m);
  void add_ready_to_read(uint64_t index, protocol::hint ctx);
  remote* get_peer(uint64_t node_id) noexcept;

  // send
  void send(protocol::message&& m);
  void send_timeout_now(uint64_t to);
  void send_rate_limit();
  void send_heartbeat(uint64_t to, protocol::hint ctx, uint64_t match_index);
  future<> send_replicate(uint64_t to, remote& r);
  void broadcast_heartbeat();
  future<> broadcast_replicate();

  // membership
  void add_node(uint64_t id);
  void add_observer(uint64_t id);
  void add_witness(uint64_t id);
  future<> remove_node(uint64_t id);
  bool is_self_removed() const;
  uint64_t quorum() const noexcept;
  uint64_t voting_members_size() const noexcept;
  bool has_quorum();

  // state transition
  void set_leader(uint64_t leader_id);
  future<> pre_campaign();
  future<> campaign();
  uint64_t handle_vote_resp(uint64_t from, bool rejected, bool prevote);
  bool can_grant_vote(uint64_t peer_id, uint64_t peer_term) const noexcept;
  future<> become_leader();
  void become_candidate();
  void become_pre_candidate();
  void become_follower(uint64_t term, uint64_t leader_id, bool reset_election);
  void become_observer(uint64_t term, uint64_t leader_id);
  void become_witness(uint64_t term, uint64_t leader_id);
  bool is_leader_transferring() const noexcept;
  void abort_leader_transfer();
  void reset(uint64_t term, bool reset_election_timeout);
  void reset_matched();
  future<bool> restore(protocol::snapshot_ptr ss);

  // log
  future<bool> try_commit();
  future<> append_entries(protocol::log_entry_vector& entries);
  future<> append_noop_entry();
  future<bool> has_committed_entry();

  // clock
  future<> tick();
  future<> leader_tick();
  future<> nonleader_tick();
  void quiesced_tick();
  void leader_is_available(uint64_t leader_id) noexcept;
  bool time_to_elect() const noexcept;
  bool time_to_heartbeat() const noexcept;
  bool time_to_check_quorum() const noexcept;
  bool time_to_abort_leader_transfer() const noexcept;
  bool time_to_gc() const noexcept;
  bool time_to_check_rate_limit() const noexcept;
  void set_randomized_election_timeout();

  // common
  future<> node_election(protocol::message& m);
  future<> node_request_vote(protocol::message& m);
  future<> node_request_prevote(protocol::message& m);
  future<> node_config_change(protocol::message& m);
  future<> node_local_tick(protocol::message& m);
  future<> node_restore_remote(protocol::message& m);

  future<> node_heartbeat(protocol::message& m);
  future<> node_replicate(protocol::message& m);
  future<> node_install_snapshot(protocol::message& m);

  // leader
  //  election          -> common
  //  request_vote      -> common
  //  request_prevote   -> common
  //  config_change     -> common
  //  local_tick        -> common
  //  snapshot_received -> common
  future<> leader_heartbeat(protocol::message& m);
  future<> leader_heartbeat_resp(protocol::message& m);
  future<> leader_check_quorum(protocol::message& m);
  future<> leader_propose(protocol::message& m);
  future<> leader_read_index(protocol::message& m);
  future<> leader_replicate_resp(protocol::message& m);
  future<> leader_snapshot_status(protocol::message& m);
  future<> leader_unreachable(protocol::message& m);
  future<> leader_leader_transfer(protocol::message& m);
  future<> leader_rate_limit(protocol::message& m);

  // candidate
  //  election          -> common
  //  request_vote      -> common
  //  request_prevote   -> common
  //  config_change     -> common
  //  local_tick        -> common
  //  snapshot_received -> common
  future<> candidate_heartbeat(protocol::message& m);
  future<> candidate_propose(protocol::message& m);
  future<> candidate_read_index(protocol::message& m);
  future<> candidate_replicate(protocol::message& m);
  future<> candidate_install_snapshot(protocol::message& m);
  future<> candidate_request_vote_resp(protocol::message& m);

  // pre-candidate
  //  election          -> common
  //  request_vote      -> common
  //  request_prevote   -> common
  //  config_change     -> common
  //  local_tick        -> common
  //  snapshot_received -> common
  //  heartbeat         -> candidate
  //  propose           -> candidate
  //  read_index        -> candidate
  //  replicate         -> candidate
  //  install_snapshot  -> candidate
  future<> pre_candidate_request_prevote_resp(protocol::message& m);

  // follower
  //  election          -> common
  //  request_vote      -> common
  //  request_prevote   -> common
  //  config_change     -> common
  //  local_tick        -> common
  //  snapshot_received -> common
  future<> follower_heartbeat(protocol::message& m);
  future<> follower_propose(protocol::message& m);
  future<> follower_read_index(protocol::message& m);
  future<> follower_read_index_resp(protocol::message& m);
  future<> follower_replicate(protocol::message& m);
  future<> follower_leader_transfer(protocol::message& m);
  future<> follower_install_snapshot(protocol::message& m);
  future<> follower_timeout_now(protocol::message& m);

  // observer
  //  request_vote      -> common
  //  request_prevote   -> common
  //  config_change     -> common
  //  local_tick        -> common
  //  snapshot_received -> common
  //  heartbeat         -> follower
  //  propose           -> follower
  //  read_index        -> follower
  //  read_index_resp   -> follower
  //  replicate         -> follower
  //  install_snapshot  -> follower

  // witness
  //  request_vote      -> common
  //  request_prevote   -> common
  //  config_change     -> common
  //  local_tick        -> common
  //  snapshot_received -> common
  //  heartbeat         -> follower
  //  replicate         -> follower
  //  install_snapshot  -> follower

  const raft_config& _config;
  protocol::group_id _gid;
  protocol::raft_role _role;
  uint64_t _leader_id;
  uint64_t _leader_transfer_target;
  uint64_t _term;
  uint64_t _vote;
  uint64_t _applied;
  bool _is_leader_transfer_target;
  bool _pending_config_change;
  bool _quiesce;
  bool _check_quorum;
  bool _snapshotting;
  raft_log _log;
  rate_limiter _limiter;

  std::unordered_map<uint64_t, bool> _votes;
  std::unordered_map<uint64_t, remote> _remotes;
  std::unordered_map<uint64_t, remote> _observers;
  std::unordered_map<uint64_t, remote> _witnesses;
  std::vector<uint64_t> _matched;
  protocol::message_vector _messages;
  read_index _read_index;
  protocol::ready_to_read_vector _ready_to_reads;
  protocol::log_entry_vector _dropped_entries;
  protocol::hint_vector _dropped_read_indexes;

  std::mt19937_64 _random_engine;
  uint64_t _tick;
  uint64_t _election_tick;
  uint64_t _heartbeat_tick;
  uint64_t _election_timeout;
  uint64_t _heartbeat_timeout;
  uint64_t _randomized_election_timeout;

  using message_handler = seastar::future<> (raft::*)(protocol::message&);
  static constexpr auto NUM_OF_STATE =
      static_cast<uint8_t>(protocol::raft_role::num_of_role);
  static constexpr auto NUM_OF_TYPE =
      static_cast<uint8_t>(protocol::message_type::num_of_type);
  message_handler _handlers[NUM_OF_STATE][NUM_OF_TYPE];
};

std::ostream& operator<<(std::ostream& os, const raft& r);

}  // namespace rafter::core
