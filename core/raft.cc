//
// Created by jason on 2021/10/9.
//

#include "raft.hh"

#include "core/logger.hh"

namespace rafter::core {

using namespace protocol;

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

}  // namespace rafter::core
