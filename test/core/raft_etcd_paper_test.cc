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

  static message accept_and_reply(const message& m) {
    return {
        .type = replicate_resp,
        .from = m.to,
        .to = m.from,
        .term = m.term,
        .lid = {0, m.lid.index + m.entries.size()}};
  }

  static future<> commit_noop_entry(raft_sm* r, db* s) {
    ASSERT_EQ(helper::_role(r->raft()), leader);
    co_await helper::broadcast_replicate(r->raft());
    auto msgs = r->read_messages();
    for (auto& m : msgs) {
      ASSERT_EQ(m.type, replicate);
      ASSERT_EQ(m.entries.size(), 1);
      ASSERT_TRUE(m.entries[0].payload.empty());
      co_await r->handle(accept_and_reply(m));
    }
    // ignore further messages to refresh followers' commit index
    r->read_messages();
    log_entry_vector to_append;
    helper::_log(r->raft()).get_entries_to_save(to_append);
    co_await s->append(std::move(to_append));
    auto term = co_await helper::_log(r->raft()).last_term();
    auto index = helper::_log(r->raft()).last_index();
    helper::_log(r->raft()).commit_update(update_commit{
        .processed = helper::_log(r->raft()).committed(),
        .stable_log_id = {term, index}});
  }
};

RAFTER_TEST_F(raft_etcd_paper_test, update_term_from_message) {
  // If one server’s current term is smaller than the other’s, then it updates
  // its current term to the larger value. If a candidate or leader discovers
  // that its term is out of date, it immediately reverts to follower state.
  // Reference: section 5.1
  auto roles = {follower, pre_candidate, candidate, leader};
  for (auto role : roles) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, 1)};
    switch (role) {
      case follower:
        helper::become_follower(r->raft(), 1, 2, true);
        break;
      case pre_candidate:
        helper::become_pre_candidate(r->raft());
        break;
      case candidate:
        helper::become_candidate(r->raft());
        break;
      case leader:
        helper::become_candidate(r->raft());
        co_await helper::become_leader(r->raft());
        break;
      default:
        ADD_FAILURE();
    }
    co_await r->handle({.type = replicate, .term = 2});
    EXPECT_EQ(helper::_term(r->raft()), 2) << name(role);
    EXPECT_EQ(helper::_role(r->raft()), follower) << name(role);
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
  co_await r->handle({.type = replicate, .term = 1});
  // should be directly dropped and replied with a noop message
  auto msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0].type, noop);
}

RAFTER_TEST_F(raft_etcd_paper_test, start_as_follower) {
  // When servers start up, they begin as followers.
  // Reference: section 5.2
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, 1)};
  ASSERT_EQ(helper::_role(r->raft()), follower);
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
    EXPECT_EQ(msg.type, heartbeat);
  }
  ASSERT_TRUE(to.empty());
}

RAFTER_TEST_F(raft_etcd_paper_test, non_leader_start_election) {
  // TODO(jyc): test candidate start election
  // If a follower receives no communication over election timeout, it begins an
  // election to choose a new leader. It increments its current term and
  // transitions to candidate state. It then votes for itself and issues
  // request_vote RPCs in parallel to each of the other servers in the shard.
  // Since we always enable prevote, we will test the request_prevote RPCs
  // Reference: section 5.2
  // Also if a candidate fails to obtain a majority, it will time out and
  // start a new election by incrementing its term and initiating another
  // round of request_vote RPCs.
  // Reference: section 5.2
  uint64_t timeout = 10;
  auto roles = {follower, pre_candidate};
  for (auto role : roles) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, timeout, 1)};
    switch (role) {
      case follower:
        helper::become_follower(r->raft(), 1, 2, true);
        break;
      case pre_candidate:
        helper::become_follower(r->raft(), 1, 2, true);
        helper::become_pre_candidate(r->raft());
        break;
      default:
        ADD_FAILURE();
    }
    for (uint64_t i = 1; i < timeout * 2; ++i) {
      co_await helper::tick(r->raft());
    }
    EXPECT_EQ(helper::_term(r->raft()), 1);
    EXPECT_EQ(helper::_role(r->raft()), pre_candidate);
    EXPECT_EQ(helper::_votes(r->raft())[1], true);
    auto msgs = r->read_messages();
    EXPECT_EQ(msgs.size(), 2);
    std::set<uint64_t> to{2, 3};
    for (auto& msg : msgs) {
      EXPECT_TRUE(to.contains(msg.to));
      to.erase(msg.to);
      EXPECT_EQ(msg.from, 1);
      EXPECT_EQ(msg.term, 2);
      EXPECT_EQ(msg.type, request_prevote);
    }
    EXPECT_TRUE(to.empty());
  }
}

RAFTER_TEST_F(raft_etcd_paper_test, leader_election_in_one_round_rpc) {
  // leader election during one round of request_vote RPC:
  // TODO(jyc): consider prevote (assume prevote is always successful here)
  // a) it wins the election
  // b) it loses the election
  // c) it is unclear about the result
  // Reference: section 5.2
  struct {
    uint64_t size;
    std::unordered_map<uint64_t, bool> votes;
    raft_role role;
  } tests[] = {
      {1, {}, leader},
      {3, {{2, true}, {3, true}}, leader},
      {3, {{2, true}}, leader},
      {5, {{2, true}, {3, true}, {4, true}, {5, true}}, leader},
      {5, {{2, true}, {3, true}, {4, true}}, leader},
      {5, {{2, true}, {3, true}}, leader},

      {3, {{2, false}, {3, false}}, follower},
      {5,
       {
           {2, false},
           {3, false},
           {4, false},
           {5, false},
       },
       follower},
      {5,
       {
           {2, true},
           {3, false},
           {4, false},
           {5, false},
       },
       follower},

      {3, {}, candidate},
      {5, {{2, true}}, candidate},
      {5, {{2, false}, {3, false}}, candidate},
      {5, {}, candidate},
  };
  for (auto& t : tests) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, test_peers(t.size), 10, 1)};
    co_await r->handle({.type = election, .from = 1, .to = 1});
    for (uint64_t id = 1; id <= t.size; ++id) {
      // become candidate
      co_await r->handle(
          {.type = request_prevote_resp, .from = id, .to = 1, .reject = false});
    }
    for (auto [id, vote] : t.votes) {
      co_await r->handle(
          {.type = request_vote_resp, .from = id, .to = 1, .reject = !vote});
    }
    EXPECT_EQ(helper::_role(r->raft()), t.role) << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_term(r->raft()), 1) << CASE_INDEX(t, tests);
  }
}

RAFTER_TEST_F(raft_etcd_paper_test, follower_vote) {
  // Each follower will vote for at most one candidate in a given term, on a
  // first-come-first-served basis.
  // Reference: section 5.2
  struct {
    uint64_t vote;
    uint64_t target;
    bool exp_reject;
  } tests[] = {
      {group_id::INVALID_NODE, 1, false},
      {group_id::INVALID_NODE, 2, false},
      {1, 1, false},
      {2, 2, false},
      {1, 2, true},
      {2, 1, true},
  };
  for (auto& t : tests) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, 1)};
    helper::set_state(r->raft(), hard_state{.term = 1, .vote = t.vote});
    co_await r->handle(
        {.type = request_vote, .from = t.target, .to = 1, .term = 1});
    auto msgs = r->read_messages();
    EXPECT_EQ(msgs.size(), 1);
    EXPECT_EQ(msgs[0].type, request_vote_resp);
    EXPECT_EQ(msgs[0].term, 1);
    EXPECT_EQ(msgs[0].reject, t.exp_reject);
  }
}

RAFTER_TEST_F(raft_etcd_paper_test, candidate_fallback) {
  // While waiting for votes, if a candidate receives an append_entries RPC from
  // another server claiming to be leader whose term is at least as large as the
  // candidate's current term, it recognizes the leader as legitimate and
  // returns to follower state.
  // Reference: section 5.2
  for (auto term : {1ULL, 2ULL}) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, 1)};
    co_await r->handle({.type = election, .from = 1, .to = 1});
    for (uint64_t id = 2; id <= 3; ++id) {
      // become candidate
      co_await r->handle(
          {.type = request_prevote_resp, .from = id, .to = 1, .reject = false});
    }
    EXPECT_EQ(helper::_role(r->raft()), candidate);
    co_await r->handle({.type = replicate, .from = 2, .to = 1, .term = term});
    EXPECT_EQ(helper::_role(r->raft()), follower);
    EXPECT_EQ(helper::_term(r->raft()), term);
  }
}

RAFTER_TEST_F(raft_etcd_paper_test, election_timeout_is_randomized) {
  // Election timeout for follower or pre_candidate or candidate is randomized.
  // Reference: section 5.2
  // avoid info log flooding
  l.set_level(log_level::warn);
  uint64_t timeout = 10;
  for (auto role : {follower, pre_candidate, candidate}) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, timeout, 1)};
    std::unordered_map<uint64_t, bool> timeouts;
    for (uint64_t round = 0; round < 50 * timeout; ++round) {
      if (role == follower) {
        helper::become_follower(r->raft(), 1, 2, true);
      } else if (role == pre_candidate) {
        helper::become_pre_candidate(r->raft());
      } else if (role == candidate) {
        helper::become_candidate(r->raft());
      }
      auto time = 0;
      while (r->read_messages().empty()) {
        co_await helper::tick(r->raft());
        time++;
      }
      timeouts[time] = true;
    }
    for (auto time = timeout + 1; time < 2 * timeout; ++time) {
      EXPECT_TRUE(timeouts[time]);
    }
  }
  l.set_level(log_level::info);
}

RAFTER_TEST_F(raft_etcd_paper_test, SKIP_election_timeout_non_conflict) {
  // In most cases only a single server(follower or candidate) will time out,
  // which reduces the likelihood of split vote in the new election.
  // Reference: section 5.2
  // TODO(jyc): add this test
  co_return;
}

RAFTER_TEST_F(raft_etcd_paper_test, leader_commit_entry) {
  // When the entry has been safely replicated, the leader gives out the applied
  // entries, which can be applied to its state machine. Also, the leader keeps
  // track of the highest index it knows to be committed, and it includes that
  // index in future append_entries RPCs so that the other servers eventually
  // find out.
  // Reference: section 5.3
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, 1)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  co_await commit_noop_entry(r.get(), r->logdb0());
  auto li = helper::_log(r->raft()).last_index();
  co_await r->handle(naive_proposal(1, 1, "some data"));
  auto msgs = r->read_messages();
  for (auto& m : msgs) {
    co_await r->handle(accept_and_reply(m));
  }
  ASSERT_EQ(helper::_log(r->raft()).committed(), li + 1);
  log_entry_vector to_apply;
  co_await helper::_log(r->raft()).get_entries_to_apply(to_apply);
  ASSERT_EQ(to_apply.size(), 1);
  log_entry exp_entry{1, li + 1};
  exp_entry.copy_of("some data");
  ASSERT_EQ(to_apply[0], exp_entry);
  msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 2);
  std::set<uint64_t> to{2, 3};
  for (auto& msg : msgs) {
    EXPECT_TRUE(to.contains(msg.to));
    to.erase(msg.to);
    EXPECT_EQ(msg.from, 1);
    EXPECT_EQ(msg.commit, li + 1);
    EXPECT_EQ(msg.type, replicate);
  }
  ASSERT_TRUE(to.empty());
}

RAFTER_TEST_F(raft_etcd_paper_test, leader_acknowledge_commit) {
  // A log entry is committed once the leader that created the entry has
  // replicated it on a majority of the servers.
  // Reference: section 5.3
  struct {
    uint64_t size;
    std::unordered_map<uint64_t, bool> acceptors;
    bool exp_ack;
  } tests[] = {
      {1, {}, true},
      {3, {}, false},
      {3, {{2, true}}, true},
      {3, {{2, true}, {3, true}}, true},
      {5, {}, false},
      {5, {{2, true}}, false},
      {5, {{2, true}, {3, true}}, true},
      {5, {{2, true}, {3, true}, {4, true}}, true},
      {5, {{2, true}, {3, true}, {4, true}, {5, true}}, true},
  };
  for (auto& t : tests) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, test_peers(t.size), 10, 1)};
    helper::become_candidate(r->raft());
    co_await helper::become_leader(r->raft());
    co_await commit_noop_entry(r.get(), r->logdb0());
    auto li = helper::_log(r->raft()).last_index();
    co_await r->handle(naive_proposal(1, 1, "some data"));
    auto msgs = r->read_messages();
    for (auto& m : msgs) {
      if (t.acceptors[m.to]) {
        co_await r->handle(accept_and_reply(m));
      }
    }
    EXPECT_EQ(helper::_log(r->raft()).committed() > li, t.exp_ack);
  }
}

}  // namespace
