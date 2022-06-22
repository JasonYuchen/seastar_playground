//
// Created by jason on 2022/6/5.
//

#include "core/read_index.hh"

#include "helper.hh"
#include "test/base.hh"
#include "util/error.hh"

namespace {

using namespace rafter;
using namespace rafter::protocol;

using helper = rafter::test::core_helper;
using rafter::test::l;

class read_index_test : public ::testing::Test {
 protected:
  static hint test_ctx(uint64_t low) { return {low, low + 1}; }
};

RAFTER_TEST_F(read_index_test, same_ctx_cannot_be_added_twice) {
  auto r = core::read_index{};
  r.add_request(1, test_ctx(10001), 1);
  ASSERT_EQ(helper::_pending(r).size(), 1);
  r.add_request(2, test_ctx(10001), 2);
  ASSERT_EQ(helper::_pending(r).size(), 1);
}

RAFTER_TEST_F(read_index_test, inconsistent_pending_queue) {
  auto r = core::read_index{};
  r.add_request(1, test_ctx(10001), 1);
  helper::_queue(r).push_back(test_ctx(10003));
  ASSERT_THROW(
      r.add_request(2, test_ctx(10002), 2), rafter::util::invalid_raft_state);
}

RAFTER_TEST_F(read_index_test, add_request) {
  auto r = core::read_index{};
  r.add_request(1, test_ctx(10001), 1);
  r.add_request(2, test_ctx(10002), 2);
  ASSERT_TRUE(r.has_pending_request());
  ASSERT_EQ(helper::_pending(r).size(), 2);
  ASSERT_EQ(helper::_queue(r).size(), 2);
  ASSERT_EQ(r.peep(), test_ctx(10002));
}

RAFTER_TEST_F(read_index_test, check_input_index) {
  auto r = core::read_index{};
  r.add_request(3, test_ctx(10001), 1);
  r.add_request(5, test_ctx(10002), 3);
  ASSERT_THROW(
      r.add_request(4, test_ctx(10003), 2), rafter::util::invalid_raft_state);
}

RAFTER_TEST_F(read_index_test, add_confirmation_checks_pending_queue) {
  auto r = core::read_index{};
  r.add_request(3, test_ctx(10001), 1);
  r.add_request(4, test_ctx(10002), 3);
  r.add_request(5, test_ctx(10003), 2);
  helper::_queue(r).push_front(test_ctx(10004));
  r.confirm(test_ctx(10001), 1, 3, [](auto) {});
  ASSERT_THROW(
      r.confirm(test_ctx(10001), 3, 3, [](auto) {}),
      rafter::util::invalid_raft_state);
}

RAFTER_TEST_F(read_index_test, leader_can_be_confirmed) {
  auto r = core::read_index{};
  auto ctx1 = test_ctx(10001);
  auto ctx2 = test_ctx(10002);
  auto ctx3 = test_ctx(10003);
  r.add_request(3, ctx2, 1);
  r.add_request(4, ctx1, 3);
  r.add_request(5, ctx3, 2);
  r.confirm(
      ctx1, 1, 3, [](auto) { ADD_FAILURE() << "too early confirmation"; });
  std::vector<core::read_index::status> ss;
  r.confirm(ctx1, 3, 3, [&ss](auto s) { ss.push_back(s); });
  ASSERT_EQ(ss.size(), 2);
  ASSERT_EQ(ss[1].index, 4);
  ASSERT_EQ(ss[1].from, 3);
  ASSERT_EQ(ss[1].ctx, ctx1);
  ASSERT_EQ(ss[0].index, 4);
  ASSERT_EQ(ss[0].from, 1);
  ASSERT_EQ(ss[0].ctx, ctx2);
  ASSERT_EQ(helper::_pending(r).size(), 1) << "confirmed request not removed";
  ASSERT_EQ(helper::_queue(r).size(), 1) << "confirmed request not removed";
}

RAFTER_TEST_F(read_index_test, DISABLED_reset_after_raft_state_change) {
  // FIXME(jyc): add this tests to core raft test
  co_return;
}

}  // namespace
