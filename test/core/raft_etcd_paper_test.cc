//
// Created by jason on 2022/9/4.
//

#include "core/raft.hh"
#include "raft_test.hh"

namespace {

using namespace rafter;
using namespace rafter::test;

class raft_etcd_paper_test : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

RAFTER_TEST_F(raft_etcd_paper_test, update_term_from_message) {
  // If one server’s current term is smaller than the other’s, then it updates
  // its current term to the larger value. If a candidate or leader discovers
  // that its term is out of date, it immediately reverts to follower state.
  // Reference: section 5.1
  auto roles = {
      raft_role::follower,
      raft_role::pre_candidate,
      raft_role::candidate,
      raft_role::leader};
  for (auto role : roles) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, 1)};
    switch (role) {
      case raft_role::follower:
        helper::become_follower(r->raft(), 1, 2, true);
        break;
      case raft_role::pre_candidate:
        helper::become_pre_candidate(r->raft());
        break;
      case raft_role::candidate:
        helper::become_candidate(r->raft());
        break;
      case raft_role::leader:
        helper::become_candidate(r->raft());
        co_await helper::become_leader(r->raft());
        break;
      default:
        ADD_FAILURE();
    }
    co_await r->handle({.type = message_type::replicate, .term = 2});
    EXPECT_EQ(helper::_term(r->raft()), 2) << name(role);
    EXPECT_EQ(helper::_role(r->raft()), raft_role::follower) << name(role);
  }
  co_return;
}

RAFTER_TEST_F(raft_etcd_paper_test, reject_stale_term_message) {
  // If a server receives a request with a stale term number, it rejects the
  // request. If it is a leader message, we will reply a noop with higher term
  // to step down the old leader, otherwise we ignore the message.
  // Reference: section 5.1
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, 1)};
  helper::set_state(r->raft(), hard_state{.term = 2});
  co_await r->handle({.type = message_type::replicate, .term = 1});
  // should be directly dropped and replied with a noop message
  auto msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0].type, message_type::noop);
}

RAFTER_TEST_F(raft_etcd_paper_test, start_as_follower) {
  // When servers start up, they begin as followers.
  // Reference: section 5.2
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, 1)};
  ASSERT_EQ(helper::_role(r->raft()), raft_role::follower);
}

RAFTER_TEST_F(raft_etcd_paper_test, leader_broadcast_heartbeat) {
  // If the leader receives a heartbeat tick, it will send an append with
  // m.lid.term/index = 0, and empty entries as heartbeat to all followers.
  // Reference: section 5.2
  uint64_t hi = 1;
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, hi)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  auto to_append = test::util::new_entries({1, 11}, 0);
  co_await helper::append_entries(r->raft(), to_append);
  for (uint64_t i = 0; i < hi; ++i) {
    co_await helper::tick(r->raft());
  }
  auto msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 2);
  std::set<uint64_t> to{2, 3};
  for (auto& msg : msgs) {
    EXPECT_TRUE(to.contains(msg.to));
    to.erase(msg.to);
    EXPECT_EQ(msg.from, 1);
    EXPECT_EQ(msg.term, 1);
    EXPECT_EQ(msg.type, message_type::heartbeat);
  }
  ASSERT_TRUE(to.empty());
}

}  // namespace
