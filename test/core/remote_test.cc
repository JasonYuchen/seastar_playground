//
// Created by jason on 2022/6/5.
//

#include "core/remote.hh"

#include "helper.hh"
#include "test/base.hh"
#include "util/error.hh"

namespace {

using namespace rafter;
using namespace rafter::protocol;

using helper = rafter::test::core_helper;
using rafter::test::l;
using state = enum core::remote::state;

class remote_test : public ::testing::Test {
 protected:
};

RAFTER_TEST_F(remote_test, snapshot_ack_tick) {
  auto r = core::remote{.state = state::snapshot};
  r.set_snapshot_ack(10, false);
  for (int i = 0; i < 10; ++i) {
    auto t = r.snapshot_ack_tick();
    if (i == 9) {
      ASSERT_TRUE(t);
    } else {
      ASSERT_FALSE(t);
    }
  }
}

RAFTER_TEST_F(remote_test, snapshot_ack) {
  auto r = core::remote{.state = state::snapshot};
  r.set_snapshot_ack(10, true);
  ASSERT_EQ(r.snapshot_tick, 10);
  ASSERT_TRUE(r.snapshot_rejected);
  r.clear_snapshot_ack();
  ASSERT_EQ(r.snapshot_tick, 0);
  ASSERT_FALSE(r.snapshot_rejected);
}

RAFTER_TEST_F(remote_test, set_snapshot_but_not_in_snapshot) {
  auto r = core::remote{.state = state::replicate};
  ASSERT_THROW(
      r.set_snapshot_ack(10, true), rafter::util::failed_precondition_error);
}

RAFTER_TEST_F(remote_test, become_retry) {
  auto r = core::remote{.state = state::replicate};
  r.become_retry();
  ASSERT_EQ(r.next, r.match + 1);
  ASSERT_EQ(r.state, state::retry);
}

RAFTER_TEST_F(remote_test, become_retry_from_snapshot) {
  auto r = core::remote{.snapshot_index = 100, .state = state::snapshot};
  r.become_retry();
  ASSERT_EQ(r.next, 101);
  ASSERT_EQ(r.state, state::retry);
  ASSERT_EQ(r.snapshot_index, 0);
  r = core::remote{.match = 10, .state = state::snapshot};
  r.become_retry();
  ASSERT_EQ(r.next, 11);
  ASSERT_EQ(r.state, state::retry);
  ASSERT_EQ(r.snapshot_index, 0);
}

RAFTER_TEST_F(remote_test, become_snapshot) {
  auto from = {
      state::replicate,
      state::retry,
      state::snapshot,
  };
  for (auto st : from) {
    auto r = core::remote{.match = 10, .next = 11, .state = st};
    r.become_snapshot(12);
    EXPECT_EQ(r.state, state::snapshot);
    EXPECT_EQ(r.match, 10);
    EXPECT_EQ(r.snapshot_index, 12);
  }
  co_return;
}

RAFTER_TEST_F(remote_test, become_replicate) {
  auto r = core::remote{.match = 10, .next = 11, .state = state::retry};
  r.become_replicate();
  ASSERT_EQ(r.state, state::replicate);
  ASSERT_EQ(r.match, 10);
  ASSERT_EQ(r.next, 11);
}

RAFTER_TEST_F(remote_test, progress) {
  auto r = core::remote{.match = 10, .next = 11, .state = state::replicate};
  r.optimistic_update(12);
  ASSERT_EQ(r.match, 10);
  ASSERT_EQ(r.next, 13);
  r = core::remote{.match = 10, .next = 11, .state = state::retry};
  ASSERT_FALSE(r.is_paused());
  r.optimistic_update(12);
  ASSERT_TRUE(r.is_paused());
  ASSERT_EQ(r.match, 10);
  ASSERT_EQ(r.next, 11);
  r = core::remote{.state = state::snapshot};
  ASSERT_THROW(
      r.optimistic_update(12), rafter::util::failed_precondition_error);
}

RAFTER_TEST_F(remote_test, paused) {
  struct {
    state st;
    bool paused;
  } tests[] = {
      {state::retry, false},
      {state::wait, true},
      {state::replicate, false},
      {state::snapshot, true},
  };
  for (auto& t : tests) {
    auto r = core::remote{.state = t.st};
    EXPECT_EQ(r.is_paused(), t.paused) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(remote_test, responded_to) {
  struct {
    state st;
    uint64_t match;
    uint64_t next;
    uint64_t snapshot_index;
    state exp_st;
    uint64_t exp_next;
  } tests[] = {
      {state::retry, 10, 12, 0, state::replicate, 11},
      {state::replicate, 10, 12, 0, state::replicate, 12},
      {state::snapshot, 10, 12, 8, state::retry, 11},
      {state::snapshot, 10, 11, 12, state::snapshot, 11},
  };
  for (auto& t : tests) {
    auto r = core::remote{
        .match = t.match,
        .next = t.next,
        .snapshot_index = t.snapshot_index,
        .state = t.st,
    };
    r.responded_to();
    EXPECT_EQ(r.state, t.exp_st) << CASE_INDEX(t, tests);
    EXPECT_EQ(r.next, t.exp_next) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(remote_test, try_update) {
  uint64_t match = 10;
  uint64_t next = 20;
  struct {
    uint64_t index;
    bool paused;
    uint64_t expected_match;
    uint64_t expected_next;
    bool expected_paused;
    bool expected_updated;
  } tests[] = {
      {next, false, next, next + 1, false, true},
      {next, true, next, next + 1, false, true},
      {next - 2, false, next - 2, next, false, true},
      {next - 2, true, next - 2, next, false, true},
      {next - 1, false, next - 1, next, false, true},
      {next - 1, true, next - 1, next, false, true},
      {match - 1, false, match, next, false, false},
      {match - 1, true, match, next, true, false},
  };
  for (auto& t : tests) {
    auto r = core::remote{.match = match, .next = next};
    if (t.paused) {
      r.retry_to_wait();
    }
    auto updated = r.try_update(t.index);
    EXPECT_EQ(updated, t.expected_updated) << CASE_INDEX(t, tests);
    EXPECT_EQ(r.match, t.expected_match) << CASE_INDEX(t, tests);
    EXPECT_EQ(r.next, t.expected_next) << CASE_INDEX(t, tests);
    if (t.expected_paused) {
      EXPECT_EQ(r.state, state::wait) << CASE_INDEX(t, tests);
    }
  }
  co_return;
}

RAFTER_TEST_F(remote_test, decrease_to_in_replicate) {
  struct {
    uint64_t match;
    uint64_t next;
    uint64_t rejected;
    bool decreased;
    uint64_t expected_next;
  } tests[] = {
      {10, 15, 9, false, 15},
      {10, 15, 10, false, 15},
      {10, 15, 12, true, 11},
  };
  for (auto& t : tests) {
    auto r = core::remote{
        .match = t.match, .next = t.next, .state = state::replicate};
    auto decreased = r.try_decrease(t.rejected, 100);
    EXPECT_EQ(decreased, t.decreased) << CASE_INDEX(t, tests);
    EXPECT_EQ(r.next, t.expected_next) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(remote_test, decrease_to_not_in_replicate) {
  struct {
    uint64_t match;
    uint64_t next;
    uint64_t rejected;
    uint64_t last;
    bool decreased;
    uint64_t expected_next;
  } tests[] = {
      {10, 15, 20, 100, false, 15},
      {10, 15, 14, 100, true, 14},
      {10, 15, 14, 10, true, 11},
  };
  for (auto& t : tests) {
    for (auto st : {state::retry, state::snapshot}) {
      auto r = core::remote{.match = t.match, .next = t.next, .state = st};
      r.retry_to_wait();
      auto decreased = r.try_decrease(t.rejected, t.last);
      EXPECT_EQ(decreased, t.decreased) << CASE_INDEX(t, tests);
      EXPECT_EQ(r.next, t.expected_next) << CASE_INDEX(t, tests);
      if (decreased) {
        EXPECT_NE(r.state, state::wait) << CASE_INDEX(t, tests);
      }
    }
  }
  co_return;
}

RAFTER_TEST_F(remote_test, try_update_to_resume) {
  auto r = core::remote{.next = 5};
  r.retry_to_wait();
  r.try_decrease(4, 4);
  ASSERT_NE(r.state, state::wait);
  r.retry_to_wait();
  r.try_update(5);
  ASSERT_NE(r.state, state::wait);
}

}  // namespace
