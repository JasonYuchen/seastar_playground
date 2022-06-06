//
// Created by jason on 2022/5/29.
//

#pragma once

#include "core/raft.hh"
#include "core/raft_log.hh"
#include "test/base.hh"

namespace rafter::test {

// This class is for testing private members of various classes.
// Instead of adding a bunch of friend classes, the private members are
// explicitly exposed via this helper.
class core_helper {
 public:
  // core::in_memory_log
  PUBLISH_VARIABLE(core::in_memory_log, _snapshot);

  // core::raft
  PUBLISH_METHOD(core::raft, abort_leader_transfer);
  PUBLISH_METHOD(core::raft, reset);
  PUBLISH_METHOD(core::raft, tick);
  PUBLISH_VARIABLE(core::raft, _role);
  PUBLISH_VARIABLE(core::raft, _leader_id);
  PUBLISH_VARIABLE(core::raft, _leader_transfer_target);
  PUBLISH_VARIABLE(core::raft, _term);
  PUBLISH_VARIABLE(core::raft, _check_quorum);
  PUBLISH_VARIABLE(core::raft, _log);
  PUBLISH_VARIABLE(core::raft, _remotes);
  PUBLISH_VARIABLE(core::raft, _observers);
  PUBLISH_VARIABLE(core::raft, _witnesses);
  PUBLISH_VARIABLE(core::raft, _election_timeout);
  PUBLISH_VARIABLE(core::raft, _randomized_election_timeout);
};

}  // namespace rafter::test
