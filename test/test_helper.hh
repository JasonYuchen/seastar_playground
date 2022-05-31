//
// Created by jason on 2022/5/29.
//

#pragma once

#include "core/raft.hh"
#include "test/base.hh"

namespace rafter::test {

// This class is for testing private members of various classes.
// Instead of adding a bunch of friend classes, the private members are
// explicitly exposed via this helper.
class helper {
 public:
  // core::raft
  PUBLISH_METHOD(core::raft, reset);
  PUBLISH_VARIABLE(core::raft, _role);
  PUBLISH_VARIABLE(core::raft, _leader_id);
  PUBLISH_VARIABLE(core::raft, _leader_transfer_target);
  PUBLISH_VARIABLE(core::raft, _term);
  PUBLISH_VARIABLE(core::raft, _remotes);
  PUBLISH_VARIABLE(core::raft, _observers);
  PUBLISH_VARIABLE(core::raft, _witnesses);
  PUBLISH_VARIABLE(core::raft, _randomized_election_timeout);
};

}  // namespace rafter::test
