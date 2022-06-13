//
// Created by jason on 2022/6/5.
//

#include "core/quiesce.hh"

#include "helper.hh"
#include "test/base.hh"

namespace {

using namespace rafter;
using namespace rafter::protocol;

using helper = rafter::test::core_helper;
using rafter::test::l;

class quiesce_test : public ::testing::Test {
 protected:
  static auto test_quiesce() { return core::quiesce{{1, 1}, true, 10}; }
};

RAFTER_TEST_F(quiesce_test, enter_quiesce) {
  auto q = test_quiesce();
  auto threshold = q.threshold();
  struct {
    uint64_t tick;
    bool quiesced;
  } tests[] = {
      {threshold / 2, false},
      {threshold, false},
      {threshold + 1, true},
  };
  for (auto& t : tests) {
    q = test_quiesce();
    for (uint64_t k = 0; k < t.tick; ++k) {
      q.tick();
    }
    EXPECT_EQ(q.quiesced(), t.quiesced);
  }
  co_return;
}

RAFTER_TEST_F(quiesce_test, can_be_disabled) {
  auto q = test_quiesce();
  auto threshold = q.threshold();
  struct {
    uint64_t tick;
    bool quiesced;
  } tests[] = {
      {threshold / 2, false},
      {threshold, false},
      {threshold + 1, false},
  };
  for (auto& t : tests) {
    q = test_quiesce();
    helper::_enabled(q) = false;
    for (uint64_t k = 0; k < t.tick; ++k) {
      q.tick();
    }
    EXPECT_EQ(q.quiesced(), t.quiesced);
  }
  co_return;
}

RAFTER_TEST_F(quiesce_test, leave_quiesce) {
  struct {
    message_type event;
  } tests[] = {
      {message_type::replicate},
      {message_type::replicate_resp},
      {message_type::request_prevote},
      {message_type::request_prevote_resp},
      {message_type::request_vote},
      {message_type::request_vote_resp},
      {message_type::install_snapshot},
      {message_type::propose},
      {message_type::read_index},
      {message_type::config_change},
  };
  for (auto& t : tests) {
    auto q = test_quiesce();
    for (uint64_t k = 0; k < q.threshold() + 1; ++k) {
      q.tick();
    }
    EXPECT_TRUE(q.quiesced()) << CASE_INDEX(t, tests);
    q.record(t.event);
    EXPECT_FALSE(q.quiesced()) << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_current_tick(q), helper::_idle_since(q))
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(quiesce_test, heartbeat_will_not_stop_entering_quiesce) {
  auto q = test_quiesce();
  auto threshold = q.threshold();
  struct {
    uint64_t tick;
    bool quiesced;
  } tests[] = {
      {threshold / 2, false},
      {threshold, false},
      {threshold + 1, true},
  };
  for (auto& t : tests) {
    q = test_quiesce();
    for (uint64_t k = 0; k < t.tick; ++k) {
      q.tick();
      q.record(message_type::heartbeat);
    }
    EXPECT_EQ(q.quiesced(), t.quiesced);
  }
  co_return;
}

RAFTER_TEST_F(quiesce_test, delayed_heartbeat_will_not_leave_quiesce) {
  auto q = test_quiesce();
  for (uint64_t k = 0; k < q.threshold() + 1; ++k) {
    q.tick();
  }
  ASSERT_TRUE(q.quiesced());
  ASSERT_TRUE(q.new_to_quiesce());
  while (q.new_to_quiesce()) {
    q.record(message_type::heartbeat);
    ASSERT_TRUE(q.quiesced());
    q.tick();
  }
  q.record(message_type::heartbeat);
  ASSERT_FALSE(q.quiesced());
}

}  // namespace
