//
// Created by jason on 2022/6/9.
//

#include "core/rate_limiter.hh"

#include "helper.hh"
#include "test/base.hh"

namespace {

using namespace rafter;
using namespace rafter::protocol;

using helper = rafter::test::core_helper;
using rafter::test::l;

class rate_limiter_test : public ::testing::Test {
 protected:
};

RAFTER_TEST_F(rate_limiter_test, enabled) {
  struct {
    uint64_t max_bytes;
    bool enabled;
  } tests[] = {
      {0, false},
      {UINT64_MAX, false},
      {1, true},
      {UINT64_MAX - 1, true},
  };
  for (auto& t : tests) {
    auto r = core::rate_limiter{t.max_bytes};
    EXPECT_EQ(r.enabled(), t.enabled) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(rate_limiter_test, get) {
  auto r = core::rate_limiter{100};
  ASSERT_EQ(r.get(), 0);
  r.increase(100);
  ASSERT_EQ(r.get(), 100);
  r.decrease(10);
  ASSERT_EQ(r.get(), 90);
  r.set(234);
  ASSERT_EQ(r.get(), 234);
}

RAFTER_TEST_F(rate_limiter_test, tick) {
  auto r = core::rate_limiter{100};
  for (int i = 0; i < 100; ++i) {
    r.tick();
    EXPECT_EQ(r.get_tick(), i + 2);
  }
  co_return;
}

RAFTER_TEST_F(rate_limiter_test, set_state) {
  auto r = core::rate_limiter{100};
  r.set_peer(100, 1);
  r.set_peer(101, 2);
  r.tick();
  r.tick();
  r.set_peer(101, 4);
  r.set_peer(102, 200);
  ASSERT_EQ(helper::_peers(r).size(), 3);
  struct {
    uint64_t node_id;
    uint64_t bytes;
    uint64_t tick;
  } tests[] = {
      {100, 1, 0},
      {101, 4, 2},
      {102, 200, 2},
  };
  for (auto& t : tests) {
    auto p = helper::_peers(r)[t.node_id];
    EXPECT_EQ(p.in_memory_log_bytes, t.bytes) << CASE_INDEX(t, tests);
    EXPECT_EQ(p.tick, t.tick + 1) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(rate_limiter_test, gc) {
  auto r = core::rate_limiter{100};
  r.set_peer(101, 1);
  r.tick();
  r.set_peer(102, 2);
  r.set_peer(103, 3);
  r.rate_limited();
  ASSERT_EQ(helper::_peers(r).size(), 3);
  for (uint64_t i = 0; i < core::rate_limiter::GC_INTERVAL; ++i) {
    r.tick();
  }
  r.rate_limited();
  ASSERT_EQ(helper::_peers(r).size(), 2);
  ASSERT_FALSE(helper::_peers(r).contains(101));
  r.tick();
  r.rate_limited();
  ASSERT_TRUE(helper::_peers(r).empty());
}

RAFTER_TEST_F(rate_limiter_test, rate_limited) {
  auto r = core::rate_limiter{100};
  r.increase(100);
  ASSERT_FALSE(r.rate_limited());
  r.increase(1);
  ASSERT_TRUE(r.rate_limited());
}

RAFTER_TEST_F(rate_limiter_test, rate_limited_when_follower_is_limited) {
  auto r = core::rate_limiter{100};
  r.increase(100);
  ASSERT_FALSE(r.rate_limited());
  r.set_peer(1, 100);
  r.set_peer(2, 101);
  ASSERT_TRUE(r.rate_limited());
}

RAFTER_TEST_F(rate_limiter_test, rate_not_limited_when_follower_out_of_date) {
  auto r = core::rate_limiter{100};
  r.increase(100);
  ASSERT_FALSE(r.rate_limited());
  r.set_peer(1, 100);
  r.set_peer(2, 101);
  r.tick();
  r.tick();
  r.tick();
  r.tick();
  ASSERT_FALSE(r.rate_limited());
  ASSERT_TRUE(helper::_peers(r).empty());
}

RAFTER_TEST_F(rate_limiter_test, no_affect_when_disabled) {
  auto r = core::rate_limiter{0};
  for (int i = 0; i < 10000; ++i) {
    r.increase(UINT64_MAX / 2);
    ASSERT_FALSE(r.rate_limited());
  }
}

RAFTER_TEST_F(rate_limiter_test, reset_state) {
  auto r = core::rate_limiter{1024};
  r.set_peer(1, 1025);
  ASSERT_TRUE(r.rate_limited());
  r.reset();
  for (uint64_t i = 0; i <= core::rate_limiter::CHANGE_INTERVAL; ++i) {
    r.tick();
  }
  ASSERT_FALSE(r.rate_limited());
}

RAFTER_TEST_F(rate_limiter_test, unlimited_threshold) {
  auto r = core::rate_limiter{100};
  r.increase(101);
  ASSERT_TRUE(r.rate_limited());
  for (uint64_t i = 0; i <= core::rate_limiter::CHANGE_INTERVAL; ++i) {
    r.tick();
  }
  r.set(99);
  ASSERT_TRUE(r.rate_limited());
  r.set(70);
  ASSERT_TRUE(r.rate_limited());
  r.set(69);
  ASSERT_FALSE(r.rate_limited());
}

RAFTER_TEST_F(rate_limiter_test, change_cannot_be_very_often) {
  auto r = core::rate_limiter{100};
  r.increase(101);
  for (uint64_t i = 0; i <= core::rate_limiter::CHANGE_INTERVAL; ++i) {
    r.tick();
  }
  ASSERT_TRUE(r.rate_limited());
  r.set(69);
  ASSERT_TRUE(r.rate_limited());
  for (uint64_t i = 0; i <= core::rate_limiter::CHANGE_INTERVAL; ++i) {
    r.tick();
  }
  ASSERT_FALSE(r.rate_limited());
}

}  // namespace
