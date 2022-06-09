//
// Created by jason on 2022/6/5.
//

#include "core/raft_log.hh"

#include "helper.hh"
#include "test/base.hh"
#include "test/test_logdb.hh"
#include "test/util.hh"

namespace {

using namespace rafter;
using namespace rafter::protocol;

using helper = rafter::test::core_helper;
using rafter::test::l;

class in_memory_log_test : public ::testing::Test {
 protected:
  static void fill_entries(
      core::in_memory_log& im, const std::vector<log_id>& lids) {
    for (auto lid : lids) {
      helper::_entries(im).emplace_back(test::util::new_entry(lid));
    }
  }
  static void fill_snapshot(core::in_memory_log& im, log_id lid) {
    if (lid.term == log_id::INVALID_TERM) {
      helper::_snapshot(im) = nullptr;
      return;
    }
    auto s = make_lw_shared<snapshot>();
    s->log_id = lid;
    helper::_snapshot(im) = std::move(s);
  }
};

RAFTER_TEST_F(in_memory_log_test, get_snapshot_index) {
  auto im = core::in_memory_log{0};
  ASSERT_FALSE(im.get_snapshot_index().has_value());
  auto sp = make_lw_shared<snapshot>();
  sp->log_id = {100, 100};
  helper::_snapshot(im) = sp;
  ASSERT_EQ(im.get_snapshot_index().value(), 100);
}

RAFTER_TEST_F(in_memory_log_test, get_entries_with_invalid_range) {
  struct {
    uint64_t low;
    uint64_t high;
    uint64_t marker;
    uint64_t first;
    uint64_t len;
  } tests[] = {
      {10, 9, 10, 10, 10},   // low > high
      {10, 11, 11, 10, 10},  // low < marker
      {10, 11, 5, 5, 5},     // high > upper bound
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{t.marker - 1};
    helper::_entries(im) = test::util::new_entries({t.first, t.first + t.len});
    log_entry_vector v;
    EXPECT_THROW(
        im.query({t.low, t.high}, v, UINT64_MAX),
        rafter::util::out_of_range_error)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, get_entries) {
  auto im = core::in_memory_log{0};
  helper::_marker(im) = 2;
  helper::_entries(im) = test::util::new_entries({{2, 2}, {2, 3}});
  log_entry_vector v;
  ASSERT_GT(im.query({2, 3}, v, UINT64_MAX), 0);
  ASSERT_EQ(v.size(), 1);
  v.clear();
  ASSERT_GT(im.query({2, 4}, v, UINT64_MAX), 0);
  ASSERT_EQ(v.size(), 2);
}

RAFTER_TEST_F(in_memory_log_test, get_last_index_with_empty_entries) {
  auto im = core::in_memory_log{0};
  ASSERT_FALSE(im.get_last_index().has_value());
  auto sp = make_lw_shared<snapshot>();
  sp->log_id = {100, 100};
  helper::_snapshot(im) = sp;
  ASSERT_EQ(im.get_last_index().value(), 100);
}

RAFTER_TEST_F(in_memory_log_test, get_last_index_with_entries) {
  struct {
    uint64_t first;
    uint64_t len;
  } tests[] = {
      {100, 5},
      {1, 100},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{0};
    helper::_entries(im) = test::util::new_entries({t.first, t.first + t.len});
    EXPECT_EQ(im.get_last_index().value(), t.first + t.len - 1)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, get_term_from_snapshot) {
  struct {
    uint64_t marker;
    log_id snapshot_lid;
    log_id lid;
    bool ok;
  } tests[] = {
      {10, {0, 0}, {0, 5}, false},
      {10, {2, 5}, {2, 5}, true},
      {10, {2, 5}, {0, 4}, false},
      {10, {2, 5}, {0, 10}, false},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{0};
    helper::_marker(im) = t.marker;
    fill_snapshot(im, t.snapshot_lid);
    auto term = im.get_term(t.lid.index);
    EXPECT_EQ(term.has_value(), t.ok) << CASE_INDEX(t, tests);
    EXPECT_EQ(term.value_or(0), t.lid.term) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, get_term_from_entries) {
  struct {
    uint64_t first;
    uint64_t len;
    log_id lid;
    bool ok;
  } tests[] = {
      {100, 5, {103, 103}, true},
      {100, 5, {104, 104}, true},
      {100, 5, {0, 105}, false},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{t.first - 1};
    helper::_entries(im) = test::util::new_entries({t.first, t.first + t.len});
    auto term = im.get_term(t.lid.index);
    EXPECT_EQ(term.has_value(), t.ok) << CASE_INDEX(t, tests);
    EXPECT_EQ(term.value_or(0), t.lid.term) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, restore) {
  auto im = core::in_memory_log{0};
  helper::_shrunk(im) = true;
  helper::_marker(im) = 10;
  helper::_entries(im) = test::util::new_entries({{1, 10}, {1, 11}});
  auto s = make_lw_shared<snapshot>();
  s->log_id.index = 100;
  im.restore(s);
  ASSERT_FALSE(helper::_shrunk(im));
  ASSERT_TRUE(helper::_entries(im).empty());
  ASSERT_EQ(helper::_marker(im), 101);
  ASSERT_TRUE(helper::_snapshot(im));
}

RAFTER_TEST_F(in_memory_log_test, advance_saved_snapshot) {
  auto im = core::in_memory_log{0};
  fill_snapshot(im, {100, 100});
  im.advance_saved_snapshot(10);
  ASSERT_TRUE(helper::_snapshot(im));
  im.advance_saved_snapshot(100);
  ASSERT_FALSE(helper::_snapshot(im));
}

RAFTER_TEST_F(in_memory_log_test, full_append_merge) {
  for (bool shrunk : {false, true}) {
    auto im = core::in_memory_log{4};
    helper::_entries(im) = test::util::new_entries({5, 8});
    helper::_shrunk(im) = shrunk;
    auto to_merge = test::util::new_entries({8, 10});
    im.merge(to_merge);
    EXPECT_EQ(helper::_shrunk(im), shrunk);
    EXPECT_EQ(helper::_entries(im).size(), 5);
    EXPECT_EQ(helper::_marker(im), 5);
    EXPECT_EQ(im.get_last_index().value_or(0), 9);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, replace_merge) {
  auto im = core::in_memory_log{4};
  helper::_entries(im) = test::util::new_entries({5, 8});
  helper::_shrunk(im) = true;
  auto to_merge = test::util::new_entries({2, 4});
  im.merge(to_merge);
  EXPECT_EQ(helper::_shrunk(im), false);
  EXPECT_EQ(helper::_entries(im).size(), 2);
  EXPECT_EQ(helper::_marker(im), 2);
  EXPECT_EQ(im.get_last_index().value_or(0), 3);
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, merge_with_gap) {
  auto im = core::in_memory_log{4};
  helper::_entries(im) = test::util::new_entries({5, 8});
  helper::_shrunk(im) = true;
  auto to_merge = test::util::new_entries({9, 11});
  ASSERT_THROW(im.merge(to_merge), rafter::util::failed_precondition_error);
}

RAFTER_TEST_F(in_memory_log_test, merge) {
  auto im = core::in_memory_log{4};
  helper::_entries(im) = test::util::new_entries({5, 8});
  helper::_shrunk(im) = true;
  auto to_merge = test::util::new_entries({{7, 6}, {10, 7}});
  im.merge(to_merge);
  EXPECT_EQ(helper::_shrunk(im), false);
  EXPECT_EQ(helper::_entries(im).size(), 3);
  EXPECT_EQ(helper::_marker(im), 5);
  EXPECT_EQ(im.get_last_index().value_or(0), 7);
  EXPECT_EQ(im.get_term(6).value_or(0), 7);
  EXPECT_EQ(im.get_term(7).value_or(0), 10);
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, entries_to_save) {
  auto im = core::in_memory_log{4};
  helper::_entries(im) = test::util::new_entries({5, 8});
  auto to_save = im.get_entries_to_save();
  EXPECT_EQ(to_save.size(), 3);
  EXPECT_EQ(to_save[0]->lid.index, 5);
  helper::_saved(im) = 5;
  to_save = im.get_entries_to_save();
  EXPECT_EQ(to_save.size(), 2);
  EXPECT_EQ(to_save[0]->lid.index, 6);
  helper::_saved(im) = 7;
  to_save = im.get_entries_to_save();
  EXPECT_TRUE(to_save.empty());
  helper::_saved(im) = 7;
  to_save = im.get_entries_to_save();
  EXPECT_TRUE(to_save.empty());
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, advance_saved_log) {
  struct {
    log_id lid;
    uint64_t saved_to;
  } tests[] = {
      {{1, 4}, 4},
      {{1, 8}, 4},
      {{7, 6}, 4},
      {{6, 6}, 6},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{4};
    helper::_entries(im) = test::util::new_entries({5, 8});
    im.advance_saved_log(t.lid);
    EXPECT_EQ(helper::_saved(im), t.saved_to);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, set_saved_when_restoring) {
  auto s = make_lw_shared<snapshot>();
  s->log_id = {10, 100};
  auto im = core::in_memory_log{4};
  fill_entries(im, {{5, 5}});
  im.restore(s);
  ASSERT_EQ(helper::_saved(im), s->log_id.index);
}

RAFTER_TEST_F(in_memory_log_test, set_saved_when_merging) {
  auto im = core::in_memory_log{5};
  helper::_entries(im) = test::util::new_entries({{6, 6}, {7, 7}});
  auto to_merge = test::util::new_entries({{6, 6}, {8, 7}});
  im.merge(to_merge);
  EXPECT_EQ(helper::_saved(im), 5);

  im = core::in_memory_log{4};
  helper::_entries(im) = test::util::new_entries({5, 11});
  im.merge(to_merge);
  EXPECT_EQ(helper::_saved(im), 4);

  im = core::in_memory_log{4};
  helper::_saved(im) = 6;
  helper::_entries(im) = test::util::new_entries({5, 11});
  im.merge(to_merge);
  EXPECT_EQ(helper::_saved(im), 5);

  im = core::in_memory_log{5};
  helper::_entries(im) = test::util::new_entries({{6, 6}, {7, 7}});
  to_merge = test::util::new_entries({{8, 8}, {9, 9}});
  im.merge(to_merge);
  EXPECT_EQ(helper::_saved(im), 5);
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, advance_applied_log) {
  struct {
    uint64_t first;
    uint64_t len;
    uint64_t applied_to;
  } tests[] = {
      {5, 6, 4},
      {6, 5, 5},
      {5, 6, 11},
      {7, 4, 6},
      {11, 0, 10},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{4};
    helper::_entries(im) = test::util::new_entries({5, 11});
    im.advance_applied_log(t.applied_to);
    EXPECT_EQ(helper::_entries(im).size(), t.len);
    if (!helper::_entries(im).empty()) {
      EXPECT_EQ(helper::_entries(im).front()->lid.index, t.first);
    }
  }
  co_return;
}

}  // namespace
