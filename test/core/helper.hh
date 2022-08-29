//
// Created by jason on 2022/5/29.
//

#pragma once

#include "core/quiesce.hh"
#include "core/raft.hh"
#include "core/raft_log.hh"
#include "test/base.hh"

namespace rafter::test {

// This class is for testing private members of various classes.
// Instead of adding a bunch of friend classes, the private members are
// explicitly exposed via this helper.
class core_helper {
 public:
  // core::read_index
  PUBLISH_VARIABLE(core::read_index, _pending);
  PUBLISH_VARIABLE(core::read_index, _queue);

  // core:quiesce
  PUBLISH_VARIABLE(core::quiesce, _enabled);
  PUBLISH_VARIABLE(core::quiesce, _current_tick);
  PUBLISH_VARIABLE(core::quiesce, _idle_since);

  // core::rate_limiter
  PUBLISH_VARIABLE(core::rate_limiter, _peers);

  // core::in_memory_log
  PUBLISH_VARIABLE(core::in_memory_log, _shrunk);
  PUBLISH_VARIABLE(core::in_memory_log, _snapshot);
  PUBLISH_VARIABLE(core::in_memory_log, _entries);
  PUBLISH_VARIABLE(core::in_memory_log, _marker);
  PUBLISH_VARIABLE(core::in_memory_log, _saved);
  PUBLISH_VARIABLE(core::in_memory_log, _applied);

  // core::log_reader
  PUBLISH_VARIABLE(core::log_reader, _marker);
  PUBLISH_VARIABLE(core::log_reader, _length);

  // core::raft_log
  PUBLISH_METHOD(core::raft_log, check_range);
  PUBLISH_VARIABLE(core::raft_log, _in_memory);

  // core::raft
  PUBLISH_METHOD(core::raft, broadcast_replicate);
  PUBLISH_METHOD(core::raft, remove_node);
  PUBLISH_METHOD(core::raft, become_leader);
  PUBLISH_METHOD(core::raft, become_candidate);
  PUBLISH_METHOD(core::raft, become_pre_candidate);
  PUBLISH_METHOD(core::raft, become_follower);
  PUBLISH_METHOD(core::raft, become_observer);
  PUBLISH_METHOD(core::raft, become_witness);
  PUBLISH_METHOD(core::raft, abort_leader_transfer);
  PUBLISH_METHOD(core::raft, reset);
  PUBLISH_METHOD(core::raft, try_commit);
  PUBLISH_METHOD(core::raft, tick);
  PUBLISH_METHOD(core::raft, time_to_elect);
  PUBLISH_METHOD(core::raft, set_randomized_election_timeout);
  PUBLISH_METHOD(core::raft, node_heartbeat);
  PUBLISH_METHOD(core::raft, node_replicate);
  PUBLISH_VARIABLE(core::raft, _gid);
  PUBLISH_VARIABLE(core::raft, _role);
  PUBLISH_VARIABLE(core::raft, _leader_id);
  PUBLISH_VARIABLE(core::raft, _leader_transfer_target);
  PUBLISH_VARIABLE(core::raft, _term);
  PUBLISH_VARIABLE(core::raft, _vote);
  PUBLISH_VARIABLE(core::raft, _check_quorum);
  PUBLISH_VARIABLE(core::raft, _log);
  PUBLISH_VARIABLE(core::raft, _remotes);
  PUBLISH_VARIABLE(core::raft, _observers);
  PUBLISH_VARIABLE(core::raft, _witnesses);
  PUBLISH_VARIABLE(core::raft, _election_tick);
  PUBLISH_VARIABLE(core::raft, _election_timeout);
  PUBLISH_VARIABLE(core::raft, _heartbeat_timeout);
  PUBLISH_VARIABLE(core::raft, _randomized_election_timeout);
  PUBLISH_VARIABLE(core::raft, _handlers);
};

}  // namespace rafter::test
