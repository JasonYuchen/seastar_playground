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
using rafter::test::base;
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

RAFTER_TEST_F(in_memory_log_test, DISABLED_assert_marker) { co_return; }

RAFTER_TEST_F(in_memory_log_test, DISABLED_assert_marker_panic) { co_return; }

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
  ASSERT_THROW(im.merge(to_merge), rafter::util::panic);
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

RAFTER_TEST_F(in_memory_log_test, rate_limited) {
  struct {
    uint64_t rate_limit_bytes;
    bool limited;
  } tests[] = {
      {0, false},
      {UINT64_MAX, false},
      {1, true},
      {UINT64_MAX - 1, true},
  };
  auto im = core::in_memory_log{0};
  ASSERT_FALSE(im.rate_limited());
  for (auto& t : tests) {
    auto rl = core::rate_limiter{t.rate_limit_bytes};
    im = core::in_memory_log{0, &rl};
    EXPECT_EQ(im.rate_limited(), t.limited);
  }
}

RAFTER_TEST_F(in_memory_log_test, rate_limit_cleared_after_restoring) {
  auto rl = core::rate_limiter{10000};
  auto im = core::in_memory_log{0, &rl};
  auto to_merge = test::util::new_entries({{1, 1}});
  to_merge[0]->payload.resize(1024);
  im.merge(to_merge);
  ASSERT_GT(rl.get(), 0);
  im.restore(make_lw_shared<snapshot>());
  ASSERT_EQ(rl.get(), 0);
}

RAFTER_TEST_F(in_memory_log_test, rate_limit_updated_after_merging) {
  auto rl = core::rate_limiter{10000};
  auto im = core::in_memory_log{0, &rl};
  auto to_merge = test::util::new_entries({{1, 1}});
  to_merge[0]->payload.resize(1024);
  im.merge(to_merge);
  auto old_bytes = rl.get();
  to_merge = test::util::new_entries({{2, 2}, {3, 3}});
  to_merge[0]->payload.resize(16);
  to_merge[1]->payload.resize(64);
  auto new_bytes = old_bytes + log_entry::in_memory_bytes(to_merge);
  im.merge(to_merge);
  ASSERT_EQ(rl.get(), new_bytes);
}

RAFTER_TEST_F(in_memory_log_test, rate_limit_decreased_after_applying) {
  auto rl = core::rate_limiter{10000};
  auto im = core::in_memory_log{2, &rl};
  auto to_merge = test::util::new_entries({{2, 2}, {3, 3}, {4, 4}});
  to_merge[0]->payload.resize(16);
  to_merge[1]->payload.resize(64);
  to_merge[2]->payload.resize(128);
  im.merge(to_merge);
  ASSERT_EQ(rl.get(), log_entry::in_memory_bytes(to_merge));
  for (uint64_t i = 2; i < 5; ++i) {
    im.advance_applied_log(i);
    if (!helper::_entries(im).empty()) {
      ASSERT_EQ(helper::_entries(im).front()->lid.index, i + 1);
    }
    ASSERT_EQ(rl.get(), log_entry::in_memory_bytes(helper::_entries(im)));
  }
}

RAFTER_TEST_F(in_memory_log_test, rate_limit_reset_when_merging) {
  auto rl = core::rate_limiter{10000};
  auto im = core::in_memory_log{2, &rl};
  auto to_merge = test::util::new_entries({{2, 2}, {3, 3}, {4, 4}});
  to_merge[0]->payload.resize(16);
  to_merge[1]->payload.resize(64);
  to_merge[2]->payload.resize(128);
  im.merge(to_merge);
  to_merge = test::util::new_entries({{1, 1}});
  to_merge[0]->payload.resize(16);
  im.merge(to_merge);
  ASSERT_EQ(rl.get(), log_entry::in_memory_bytes(to_merge));
}

RAFTER_TEST_F(in_memory_log_test, rate_limit_updated_after_cut_merging) {
  auto rl = core::rate_limiter{10000};
  auto im = core::in_memory_log{2, &rl};
  auto to_merge = test::util::new_entries({{2, 2}, {3, 3}, {4, 4}});
  to_merge[0]->payload.resize(16);
  to_merge[1]->payload.resize(64);
  to_merge[2]->payload.resize(128);
  im.merge(to_merge);
  to_merge = test::util::new_entries({{3, 3}, {4, 4}});
  to_merge[0]->payload.resize(1024);
  to_merge[1]->payload.resize(1024);
  im.merge(to_merge);
  ASSERT_EQ(rl.get(), log_entry::in_memory_bytes(helper::_entries(im)));
}

RAFTER_TEST_F(in_memory_log_test, DISABLED_resize) {
  // TestResize
  // TestTryResize
  // TestNewEntrySlice
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, unstable_maybe_first_index) {
  struct {
    uint64_t marker;
    log_id ss_lid;
    std::vector<log_id> entries;
    std::optional<uint64_t> index;
  } tests[] = {
      {0, {}, {}, std::nullopt},
      {5, {}, {{1, 5}}, std::nullopt},
      {5, {1, 4}, {{1, 5}}, 4},
      {5, {1, 4}, {}, 4},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{0};
    helper::_marker(im) = t.marker;
    fill_entries(im, t.entries);
    fill_snapshot(im, t.ss_lid);
    EXPECT_EQ(im.get_snapshot_index(), t.index) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, maybe_last_index) {
  struct {
    uint64_t marker;
    log_id ss_lid;
    std::vector<log_id> entries;
    std::optional<uint64_t> index;
  } tests[] = {
      {0, {}, {}, std::nullopt},
      {5, {}, {{1, 5}}, 5},
      {5, {1, 4}, {{1, 5}}, 5},
      {5, {1, 4}, {}, 4},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{0};
    helper::_marker(im) = t.marker;
    fill_entries(im, t.entries);
    fill_snapshot(im, t.ss_lid);
    EXPECT_EQ(im.get_last_index(), t.index) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, unstable_maybe_term) {
  struct {
    uint64_t marker;
    log_id ss_lid;
    std::vector<log_id> entries;
    uint64_t index;
    std::optional<uint64_t> term;
  } tests[] = {
      {0, {}, {}, 5, std::nullopt},
      {5, {}, {{1, 5}}, 5, 1},
      {5, {}, {{1, 5}}, 6, std::nullopt},
      {5, {}, {{1, 5}}, 4, std::nullopt},
      {5, {1, 4}, {{1, 5}}, 5, 1},
      {5, {1, 4}, {{1, 5}}, 6, std::nullopt},
      {5, {1, 4}, {{1, 5}}, 4, 1},
      {5, {1, 4}, {{1, 5}}, 3, std::nullopt},
      {5, {1, 4}, {}, 5, std::nullopt},
      {5, {1, 4}, {}, 4, 1},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{0};
    helper::_marker(im) = t.marker;
    fill_entries(im, t.entries);
    fill_snapshot(im, t.ss_lid);
    EXPECT_EQ(im.get_term(t.index), t.term) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, unstable_restore) {
  auto im = core::in_memory_log{4};
  fill_entries(im, {{1, 5}});
  fill_snapshot(im, {1, 4});
  auto ss = test::util::new_snapshot({2, 6});
  im.restore(ss);
  ASSERT_EQ(helper::_marker(im), ss->log_id.index + 1);
  ASSERT_TRUE(helper::_entries(im).empty());
  ASSERT_EQ(helper::_snapshot(im)->log_id, ss->log_id);
}

RAFTER_TEST_F(in_memory_log_test, unstable_truncate_append) {
  struct {
    uint64_t marker;
    std::vector<log_id> entries;
    std::vector<log_id> to_append;
    uint64_t exp_marker;
    std::vector<log_id> exp_entries;
  } tests[] = {
      // append to end
      {5, {{1, 5}}, {{1, 6}, {1, 7}}, 5, {{1, 5}, {1, 6}, {1, 7}}},
      // replace all
      {5, {{1, 5}}, {{2, 5}, {2, 6}}, 5, {{2, 5}, {2, 6}}},
      {5, {{1, 5}}, {{2, 4}, {2, 5}, {2, 6}}, 4, {{2, 4}, {2, 5}, {2, 6}}},
      // truncate and append
      {5, {{1, 5}, {1, 6}, {1, 7}}, {{2, 6}}, 5, {{1, 5}, {2, 6}}},
      {5,
       {{1, 5}, {1, 6}, {1, 7}},
       {{2, 7}, {2, 8}},
       5,
       {{1, 5}, {1, 6}, {2, 7}, {2, 8}}},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{0};
    helper::_marker(im) = t.marker;
    fill_entries(im, t.entries);
    im.merge(test::util::new_entries(t.to_append));
    EXPECT_EQ(helper::_marker(im), t.exp_marker) << CASE_INDEX(t, tests);
    EXPECT_TRUE(test::util::compare(
        helper::_entries(im), test::util::new_entries(t.exp_entries)))
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(in_memory_log_test, unstable_stable_to) {
  struct {
    uint64_t marker;
    log_id ss_lid;
    std::vector<log_id> entries;
    log_id stable_to;
    uint64_t exp_saved_to;
    uint64_t exp_marker;
    uint64_t exp_len;
  } tests[] = {
      // null
      {0, {}, {}, {1, 5}, 0, 0, 0},
      // stable to 1st entry
      {5, {}, {{1, 5}}, {1, 5}, 5, 6, 0},
      // stable to 1st entry
      {5, {}, {{1, 5}, {1, 6}}, {1, 5}, 5, 6, 1},
      // stable to 1st entry and term mismatch
      {6, {}, {{2, 6}}, {1, 6}, 0, 7, 0},
      // stable to old entry
      {5, {}, {{1, 5}}, {1, 4}, 0, 5, 1},
      // stable to old entry
      {5, {}, {{1, 5}}, {2, 4}, 0, 5, 1},
      // has snapshot, stable to 1st entry
      {5, {1, 4}, {{1, 5}}, {1, 5}, 5, 6, 0},
      // has snapshot, stable to 1st entry
      {5, {1, 4}, {{1, 5}, {1, 6}}, {1, 5}, 5, 6, 1},
      // has snapshot, stable to 1st entry and term mismatch
      {6, {1, 5}, {{2, 6}}, {1, 6}, 0, 7, 0},
      // has snapshot, stable to snapshot
      {5, {1, 4}, {{1, 5}}, {1, 4}, 0, 5, 1},
      // has snapshot, stable to old entry
      {5, {2, 4}, {{2, 5}}, {1, 4}, 0, 5, 1},
  };
  for (auto& t : tests) {
    auto im = core::in_memory_log{0};
    helper::_marker(im) = t.marker;
    fill_entries(im, t.entries);
    fill_snapshot(im, t.ss_lid);
    im.advance_saved_log(t.stable_to);
    im.advance_applied_log(t.stable_to.index);
    EXPECT_EQ(helper::_saved(im), t.exp_saved_to) << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_marker(im), t.exp_marker) << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_entries(im).size(), t.exp_len) << CASE_INDEX(t, tests);
  }
  co_return;
}

class log_reader_test : public ::testing::Test {
 protected:
  static snapshot_ptr make_test_snapshot() {
    auto ss = make_lw_shared<snapshot>();
    ss->log_id = {124, 123};
    ss->membership = make_lw_shared<membership>();
    ss->membership->config_change_id = 1234;
    ss->membership->addresses = {{123, "address123"}, {234, "address234"}};
    return ss;
  }

  test::test_logdb _db;
};

RAFTER_TEST_F(log_reader_test, initial_state) {
  auto lr = core::log_reader({1, 1}, _db);
  ASSERT_EQ(helper::_length(lr), 1);
  auto st = hard_state{.term = 100, .vote = 112, .commit = 123};
  lr.set_state(st);
  auto ss = make_test_snapshot();
  lr.create_snapshot(ss);
  ASSERT_EQ(lr.get_state(), st);
  ASSERT_EQ(*lr.get_membership(), *ss->membership);
}

RAFTER_TEST_F(log_reader_test, apply_snapshot) {
  auto lr = core::log_reader({1, 1}, _db);
  auto ss = make_test_snapshot();
  lr.apply_snapshot(ss);
  log_id expected_marker{124, 123};
  ASSERT_EQ(helper::_marker(lr), expected_marker);
  ASSERT_EQ(helper::_length(lr), 1);
  ASSERT_EQ(*lr.get_snapshot()->membership, *ss->membership);
}

RAFTER_TEST_F(log_reader_test, index_range) {
  auto lr = core::log_reader({1, 1}, _db);
  auto ss = make_test_snapshot();
  lr.apply_snapshot(ss);
  auto range = lr.get_range();
  // for only a snapshot available, last_index + 1 = first_index
  ASSERT_EQ(range.low, 124);
  ASSERT_EQ(range.high, 123);
}

RAFTER_TEST_F(log_reader_test, set_range) {
  struct {
    uint64_t marker;
    uint64_t length;
    uint64_t index;
    uint64_t idx_len;
    uint64_t exp_len;
  } tests[] = {
      {1, 10, 1, 1, 10},
      {1, 10, 1, 0, 10},
      {10, 10, 8, 10, 8},
      {10, 10, 20, 10, 20},
  };
  for (auto& t : tests) {
    auto lr = core::log_reader({1, 1}, _db);
    helper::_marker(lr) = {1, t.marker};
    helper::_length(lr) = t.length;
    lr.set_range({t.index, t.index + t.length});
    EXPECT_EQ(helper::_length(lr), t.exp_len);
  }
  co_return;
}

RAFTER_TEST_F(log_reader_test, panic_when_gap) {
  auto lr = core::log_reader({1, 1}, _db);
  helper::_marker(lr) = {1, 10};
  helper::_length(lr) = 10;
  ASSERT_THROW(lr.set_range({50, 60}), rafter::util::panic);
}

class raft_log_test : public ::testing::Test {
 protected:
  void SetUp() override {
    base::submit([this]() -> future<> {
      _db = std::make_unique<test::test_logdb>();
      _lr = std::make_unique<core::log_reader>(group_id{1, 1}, *_db);
      co_return;
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      _lr.reset();
      _db.reset();
      co_return;
    });
  }

  future<> append_to_test_logdb(
      const log_entry_vector& entries,
      snapshot_ptr snap = {},
      bool reset = false) {
    if (reset) {
      _db = std::make_unique<test::test_logdb>();
      _lr = std::make_unique<core::log_reader>(group_id{1, 1}, *_db);
    }
    if (snap) {
      snap->group_id = {1, 1};
    }
    update up{
        .gid = {1, 1},
        .entries_to_save = entries,
        .snapshot = snap,
    };
    storage::update_pack pack(up);
    co_await _db->save({&pack, 1});
    co_await pack.done.get_future();
    _lr->apply_entries(entries);
    if (snap) {
      _lr->apply_snapshot(snap);
    }
  }

  std::unique_ptr<test::test_logdb> _db;
  std::unique_ptr<core::log_reader> _lr;
};

RAFTER_TEST_F(raft_log_test, create) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}}));
  auto expected_range = hint{.low = 1, .high = 3};
  ASSERT_EQ(_lr->get_range(), expected_range);
  auto rl = core::raft_log({1, 1}, *_lr);
  ASSERT_EQ(rl.committed(), 0);
  ASSERT_EQ(rl.processed(), 0);
  ASSERT_EQ(helper::_marker(helper::_in_memory(rl)), 4);
}

RAFTER_TEST_F(raft_log_test, snapshot_index_as_first_index) {
  auto rl = core::raft_log({1, 1}, *_lr);
  auto ss = make_lw_shared<snapshot>();
  ss->log_id = {3, 100};
  helper::_in_memory(rl).restore(ss);
  ASSERT_EQ(rl.first_index(), 101);
}

RAFTER_TEST_F(raft_log_test, log_with_in_memory_snapshot_only) {
  auto rl = core::raft_log({1, 1}, *_lr);
  auto ss = make_lw_shared<snapshot>();
  ss->log_id = {3, 100};
  rl.restore(ss);
  ASSERT_EQ(rl.first_index(), 101);
  ASSERT_EQ(rl.last_index(), 100);
  for (uint64_t i = 0; i < 110; ++i) {
    log_entry_vector e;
    ASSERT_THROW(
        co_await rl.query({i, i + 1}, e, UINT64_MAX),
        rafter::util::compacted_error);
    ASSERT_TRUE(e.empty());
  }
}

RAFTER_TEST_F(raft_log_test, no_entries_to_apply_after_restored) {
  auto rl = core::raft_log({1, 1}, *_lr);
  auto ss = make_lw_shared<snapshot>();
  ss->log_id = {3, 100};
  rl.restore(ss);
  ASSERT_FALSE(rl.has_entries_to_apply());
}

RAFTER_TEST_F(raft_log_test, first_not_applied_index_after_restored) {
  auto rl = core::raft_log({1, 1}, *_lr);
  auto ss = make_lw_shared<snapshot>();
  ss->log_id = {3, 100};
  rl.restore(ss);
  ASSERT_EQ(rl.first_not_applied_index(), 101);
  ASSERT_EQ(rl.apply_index_limit(), 101);
}

RAFTER_TEST_F(raft_log_test, iterate_ready_to_be_applied) {
  auto entries = test::util::new_entries({.low = 1, .high = 129});
  for (int i = 1; i <= 10; ++i) {
    // no greater than max_entry_bytes = 8MB
    entries[i * 10]->payload.resize(7UL * MB);
  }
  co_await append_to_test_logdb(entries);
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.set_committed(128);
  rl.set_processed(0);
  entries.clear();
  int count = 0;
  while (true) {
    co_await rl.get_entries_to_apply(entries);
    ASSERT_FALSE(entries.empty());
    if (rl.processed() == entries.back()->lid.index) {
      break;
    }
    count++;
    // for default config
    // (max_entry_bytes = 8MB, max_apply_entry_bytes = 64MB)
    if (count == 1) {
      ASSERT_EQ(entries.back()->lid.index, 100);
    }
    if (count == 2) {
      ASSERT_EQ(entries.back()->lid.index, 128);
    }
    rl.set_processed(entries.back()->lid.index);
  }
  ASSERT_EQ(entries.size(), 128);
  for (uint64_t i = 0; i < 128; ++i) {
    ASSERT_EQ(entries[i]->lid.index, i + 1);
  }
  ASSERT_EQ(count, 2);
}

RAFTER_TEST_F(raft_log_test, log_append) {
  auto entries = test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(entries);
  log_entry_vector to_save;
  rl.get_entries_to_save(to_save);
  ASSERT_EQ(to_save.size(), entries.size());
}

RAFTER_TEST_F(raft_log_test, panic_if_append_committed_entries) {
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.set_committed(2);
  ASSERT_THROW(
      rl.append(test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}})),
      rafter::util::panic);
}

RAFTER_TEST_F(raft_log_test, first_index_from_logdb) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  ASSERT_EQ(rl.first_index(), 1);
}

RAFTER_TEST_F(raft_log_test, last_index_from_inmemory) {
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  ASSERT_EQ(rl.last_index(), 4);
}

RAFTER_TEST_F(raft_log_test, last_index_from_logdb) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  ASSERT_EQ(rl.last_index(), 3);
}

RAFTER_TEST_F(raft_log_test, last_term_from_logdb) {
  co_await append_to_test_logdb(test::util::new_entries({{1, 1}, {5, 2}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  ASSERT_EQ(co_await rl.last_term(), 5);
}

RAFTER_TEST_F(raft_log_test, last_term_from_inmemory) {
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  ASSERT_EQ(co_await rl.last_term(), 3);
}

RAFTER_TEST_F(raft_log_test, log_term_from_logdb) {
  auto entries = test::util::new_entries({{1, 1}, {5, 2}});
  co_await append_to_test_logdb(entries);
  auto rl = core::raft_log({1, 1}, *_lr);
  for (const auto& e : entries) {
    EXPECT_EQ(co_await rl.term(e->lid.index), e->lid.term);
  }
}

RAFTER_TEST_F(raft_log_test, log_term_from_inmemory) {
  auto entries = test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(entries);
  for (const auto& e : entries) {
    EXPECT_EQ(co_await rl.term(e->lid.index), e->lid.term);
  }
}

RAFTER_TEST_F(raft_log_test, query_inmemory) {
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  log_entry_vector entries;
  co_await rl.query({1, 5}, entries, UINT64_MAX);
  ASSERT_EQ(entries.size(), 4);
  entries.clear();
  co_await rl.query({2, 4}, entries, UINT64_MAX);
  ASSERT_EQ(entries.size(), 2);
}

RAFTER_TEST_F(raft_log_test, query_logdb) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  log_entry_vector entries;
  co_await rl.query({1, 5}, entries, UINT64_MAX);
  ASSERT_EQ(entries.size(), 4);
  entries.clear();
  co_await rl.query({2, 4}, entries, UINT64_MAX);
  ASSERT_EQ(entries.size(), 2);
}

RAFTER_TEST_F(raft_log_test, query_inmemory_and_logdb) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{3, 5}, {3, 6}, {4, 7}}));
  log_entry_vector entries;
  co_await rl.query({1, 8}, entries, UINT64_MAX);
  ASSERT_EQ(entries.size(), 7);
  entries.clear();
  co_await rl.query({2, 7}, entries, UINT64_MAX);
  ASSERT_EQ(entries.size(), 5);
  ASSERT_EQ(entries[0]->lid.index, 2);
  ASSERT_EQ(entries[4]->lid.index, 6);
  entries.clear();
  co_await rl.query({1, 5}, entries, UINT64_MAX);
  ASSERT_EQ(entries.size(), 4);
  ASSERT_EQ(entries[0]->lid.index, 1);
  ASSERT_EQ(entries[3]->lid.index, 4);
  entries.clear();
  co_await rl.query(2, entries, UINT64_MAX);
  ASSERT_EQ(entries.size(), 6);
}

RAFTER_TEST_F(raft_log_test, get_snapshot) {
  auto inmemory_s = make_lw_shared<snapshot>();
  inmemory_s->log_id = {2, 123};
  auto logdb_s = make_lw_shared<snapshot>();
  logdb_s->log_id = {3, 234};
  auto rl = core::raft_log({1, 1}, *_lr);
  helper::_in_memory(rl).restore(inmemory_s);
  _lr->apply_snapshot(logdb_s);
  auto ss = rl.get_snapshot();
  ASSERT_EQ(ss->log_id, inmemory_s->log_id);
  helper::_in_memory(rl).advance_saved_snapshot(inmemory_s->log_id.index);
  ss = rl.get_snapshot();
  ASSERT_EQ(ss->log_id, logdb_s->log_id);
}

RAFTER_TEST_F(raft_log_test, restore_snapshot) {
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  auto s = make_lw_shared<snapshot>();
  s->log_id = {10, 100};
  rl.restore(s);
  ASSERT_EQ(rl.committed(), 100);
  ASSERT_EQ(rl.processed(), 100);
  ASSERT_EQ(helper::_marker(helper::_in_memory(rl)), 101);
  ASSERT_EQ(helper::_in_memory(rl).get_snapshot()->log_id.index, 100);
}

RAFTER_TEST_F(raft_log_test, log_match_term) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{3, 5}, {3, 6}, {4, 7}}));
  struct {
    log_id id;
    bool match;
  } tests[] = {
      {{1, 1}, true},
      {{2, 1}, false},
      {{4, 4}, false},
      {{3, 4}, true},
      {{3, 5}, true},
      {{4, 5}, false},
      {{4, 7}, true},
      {{5, 8}, false},
  };
  for (auto& t : tests) {
    EXPECT_EQ(co_await rl.term_index_match(t.id), t.match)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_log_test, log_up_to_date) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{3, 5}, {3, 6}, {4, 7}}));
  struct {
    log_id id;
    bool up_to_date;
  } tests[] = {
      {{2, 1}, false},
      {{2, 8}, false},
      {{4, 1}, false},
      {{4, 7}, true},
      {{4, 8}, true},
      {{5, 8}, true},
      {{5, 2}, true},
  };
  for (auto& t : tests) {
    EXPECT_EQ(co_await rl.up_to_date(t.id), t.up_to_date)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_log_test, get_conflict_index) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{3, 5}, {3, 6}, {4, 7}}));
  struct {
    log_entry_vector entries;
    uint64_t conflict_index;
  } tests[] = {
      {{}, log_id::INVALID_INDEX},
      {test::util::new_entries({{2, 1}}), 1},
      {test::util::new_entries({{1, 1}, {1, 2}}), log_id::INVALID_INDEX},
      {test::util::new_entries({{1, 1}, {2, 2}}), 2},
      {test::util::new_entries({{3, 6}, {4, 7}}), log_id::INVALID_INDEX},
      {test::util::new_entries({{3, 6}, {5, 7}}), 7},
      {test::util::new_entries({{4, 7}, {4, 8}}), 8},
  };
  for (auto& t : tests) {
    EXPECT_EQ(co_await rl.get_conflict_index(t.entries), t.conflict_index)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_log_test, commit_to) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{3, 5}, {3, 6}, {4, 7}}));
  rl.commit(3);
  ASSERT_EQ(rl.committed(), 3);
  rl.commit(2);
  ASSERT_EQ(rl.committed(), 3);
}

RAFTER_TEST_F(raft_log_test, panic_when_commit_to_unavailable_index) {
  co_await append_to_test_logdb(
      test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{3, 5}, {3, 6}, {4, 7}}));
  ASSERT_THROW(rl.commit(8), rafter::util::panic);
}

RAFTER_TEST_F(raft_log_test, commit_update) {
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.set_committed(10);
  update_commit uc{.processed = 5};
  rl.commit_update(uc);
  ASSERT_EQ(rl.processed(), 5);
}

RAFTER_TEST_F(raft_log_test, commit_update_twice_will_throw) {
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.set_processed(6);
  rl.set_committed(10);
  update_commit uc{.processed = 5};
  ASSERT_THROW(rl.commit_update(uc), rafter::util::panic);
}

RAFTER_TEST_F(raft_log_test, panic_when_commit_update_has_not_committed_entry) {
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.set_processed(6);
  rl.set_committed(10);
  update_commit uc{.processed = 12};
  ASSERT_THROW(rl.commit_update(uc), rafter::util::panic);
}

RAFTER_TEST_F(raft_log_test, get_uncommitted_entries) {
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(test::util::new_entries({{1, 1}, {1, 2}, {2, 3}, {3, 4}}));
  struct {
    uint64_t committed;
    uint64_t length;
    uint64_t first_index;
  } tests[] = {
      {0, 4, 1},
      {1, 3, 2},
      {2, 2, 3},
      {3, 1, 4},
      {4, 0, 0},
  };
  for (auto& t : tests) {
    rl.set_committed(t.committed);
    log_entry_vector entries;
    rl.get_uncommitted_entries(entries);
    EXPECT_EQ(entries.size(), t.length) << CASE_INDEX(t, tests);
    if (!entries.empty()) {
      EXPECT_EQ(entries[0]->lid.index, t.first_index) << CASE_INDEX(t, tests);
    }
  }
  co_return;
}

RAFTER_TEST_F(raft_log_test, find_conflict) {
  auto previous_entries = test::util::new_entries({{1, 1}, {2, 2}, {3, 3}});
  struct {
    std::vector<log_id> entries;
    uint64_t exp_conflict;
  } tests[] = {
      // empty
      {{}, 0},
      // no conflict
      {{{1, 1}, {2, 2}, {3, 3}}, 0},
      {{{2, 2}, {3, 3}}, 0},
      {{{3, 3}}, 0},
      // no conflict with new entries
      {{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {4, 5}}, 4},
      {{{2, 2}, {3, 3}, {4, 4}, {4, 5}}, 4},
      {{{3, 3}, {4, 4}, {4, 5}}, 4},
      // conflict
      {{{4, 1}, {4, 2}}, 1},
      {{{1, 2}, {4, 3}, {4, 4}}, 2},
      {{{1, 3}, {2, 4}, {4, 5}, {4, 6}}, 3},
  };
  for (auto& t : tests) {
    auto rl = core::raft_log({1, 1}, *_lr);
    rl.append(previous_entries);
    EXPECT_EQ(
        co_await rl.get_conflict_index(test::util::new_entries(t.entries)),
        t.exp_conflict)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_log_test, is_up_to_date) {
  auto previous_entries = test::util::new_entries({{1, 1}, {2, 2}, {3, 3}});
  auto rl = core::raft_log({1, 1}, *_lr);
  rl.append(previous_entries);
  struct {
    log_id last_lid;
    bool up_to_date;
  } tests[] = {
      // greater term, always up to date
      {{4, rl.last_index() - 1}, true},
      {{4, rl.last_index()}, true},
      {{4, rl.last_index() + 1}, true},
      // smaller term, always not up to date
      {{2, rl.last_index() - 1}, false},
      {{2, rl.last_index()}, false},
      {{2, rl.last_index() + 1}, false},
      // equal term, depends on index
      {{3, rl.last_index() - 1}, false},
      {{3, rl.last_index()}, true},
      {{3, rl.last_index() + 1}, true},
  };
  for (auto& t : tests) {
    EXPECT_EQ(co_await rl.up_to_date(t.last_lid), t.up_to_date)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_log_test, append) {
  auto previous_entries = test::util::new_entries({{1, 1}, {2, 2}});
  struct {
    std::vector<log_id> entries;
    std::vector<log_id> exp_entries;
    uint64_t exp_index;
    uint64_t exp_unstable;
  } tests[] = {
      // empty and append
      {{}, {{1, 1}, {2, 2}}, 2, 3},
      // normal append
      {{{2, 3}}, {{1, 1}, {2, 2}, {2, 3}}, 3, 3},
      // replace
      {{{2, 1}}, {{2, 1}}, 1, 1},
      // truncate and append
      {{{3, 2}, {3, 3}}, {{1, 1}, {3, 2}, {3, 3}}, 3, 2},
  };
  for (auto& t : tests) {
    co_await append_to_test_logdb(
        previous_entries, /* snapshot = */ {}, /*reset = */ true);
    auto rl = core::raft_log({1, 1}, *_lr);
    rl.append(test::util::new_entries(t.entries));
    EXPECT_EQ(rl.last_index(), t.exp_index) << CASE_INDEX(t, tests);
    log_entry_vector queried;
    co_await rl.query(1, queried, UINT64_MAX);
    EXPECT_TRUE(
        test::util::compare(queried, test::util::new_entries(t.exp_entries)))
        << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_marker(helper::_in_memory(rl)), t.exp_unstable)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_log_test, maybe_append) {
  auto previous_entries = test::util::new_entries({{1, 1}, {2, 2}, {3, 3}});
  uint64_t li = 3;  // last index
  uint64_t lt = 3;  // last term
  uint64_t commit = 1;
  struct {
    log_id lid;
    uint64_t committed;
    std::vector<log_id> entries;
    uint64_t exp_last_index;
    bool exp_append;
    uint64_t exp_commit;
    bool exp_throw;
  } tests[] = {
      // not match: term is different
      {{lt - 1, li}, li, {{4, li + 1}}, 0, false, commit, false},
      // not match: index is out of range
      {{lt, li + 1}, li, {{4, li + 2}}, 0, false, commit, false},
      // match: match last entry
      {{lt, li}, li, {}, li, true, li, false},
      // match: commit cannot exceed last entry's index
      {{lt, li}, li + 1, {}, li, true, li, false},
      // match: commit to the specified index even we have a newer entry
      {{lt, li}, li - 1, {}, li, true, li - 1, false},
      // match: commit cannot move backward
      {{lt, li}, 0, {}, li, true, commit, false},
      // noop: commit cannot move backward
      {{0, 0}, li, {}, 0, true, commit, false},
      // match: append will advance last index but not commit index
      {{lt, li}, li, {{4, li + 1}}, li + 1, true, li, false},
      //
      {{lt, li}, li + 1, {{4, li + 1}}, li + 1, true, li + 1, false},
      //
      {{lt, li}, li + 2, {{4, li + 1}}, li + 1, true, li + 1, false},
      //
      {{lt, li},
       li + 2,
       {{4, li + 1}, {4, li + 2}},
       li + 2,
       true,
       li + 2,
       false},
      // match: match with entry in middle
      {{lt - 1, li - 1}, li, {{4, li}}, li, true, li, false},
      //
      {{lt - 2, li - 2}, li, {{4, li - 1}}, li - 1, true, li - 1, false},
      // conflict
      {{lt - 3, li - 3}, li, {{4, li - 2}}, li - 2, true, li - 2, true},
      //
      {{lt - 2, li - 2}, li, {{4, li - 1}, {4, li}}, li, true, li, false},

  };
  for (auto& t : tests) {
    auto rl = core::raft_log({1, 1}, *_lr);
    rl.append(previous_entries);
    rl.set_committed(commit);
    // refine exception flow
    try {
      uint64_t cur_last_index = 0;
      bool cur_append = false;
      if (co_await rl.term_index_match(t.lid)) {
        cur_append = true;
        co_await rl.try_append(t.lid.index, test::util::new_entries(t.entries));
        cur_last_index = t.lid.index + t.entries.size();
        rl.commit(std::min(cur_last_index, t.committed));
      }
      EXPECT_EQ(cur_last_index, t.exp_last_index) << CASE_INDEX(t, tests);
      EXPECT_EQ(cur_append, t.exp_append) << CASE_INDEX(t, tests);
      EXPECT_EQ(rl.committed(), t.exp_commit) << CASE_INDEX(t, tests);
      if (cur_append && !t.entries.empty()) {
        log_entry_vector e;
        co_await rl.query(
            {.low = rl.last_index() - t.entries.size() + 1,
             .high = rl.last_index() + 1},
            e,
            UINT64_MAX);
        EXPECT_TRUE(test::util::compare(e, test::util::new_entries(t.entries)))
            << CASE_INDEX(t, tests);
      }
    } catch (rafter::util::panic& e) {
      EXPECT_TRUE(t.exp_throw) << CASE_INDEX(t, tests);
    }
  }
}

RAFTER_TEST_F(raft_log_test, next_entries) {
  auto snap = test::util::new_snapshot({1, 3});
  auto entries = test::util::new_entries({{1, 4}, {1, 5}, {1, 6}});
  struct {
    uint64_t applied;
    bool exp_has_next;
    std::vector<log_id> exp_entries;
  } tests[] = {
      {0, true, {{1, 4}, {1, 5}}},
      {3, true, {{1, 4}, {1, 5}}},
      {4, true, {{1, 5}}},
      {5, false, {}},
  };
  for (auto& t : tests) {
    co_await append_to_test_logdb({}, snap, true);
    auto rl = core::raft_log({1, 1}, *_lr);
    rl.append(entries);
    co_await rl.try_commit({1, 5});
    rl.commit_update({.processed = t.applied});
    EXPECT_EQ(rl.has_entries_to_apply(), t.exp_has_next)
        << CASE_INDEX(t, tests);
    log_entry_vector queried;
    co_await rl.get_entries_to_apply(queried);
    EXPECT_TRUE(
        test::util::compare(queried, test::util::new_entries(t.exp_entries)))
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_log_test, commit_to_2) {
  auto entries = test::util::new_entries({{1, 1}, {2, 2}, {3, 3}});
  uint64_t commit = 2;
  struct {
    uint64_t commit;
    uint64_t exp_commit;
    bool exp_throw;
  } tests[] = {
      {3, 3, false}, {1, 2, false}, {4, 0, true},  // commit out-of-range
  };
  for (auto& t : tests) {
    co_await append_to_test_logdb({}, {}, true);
    auto rl = core::raft_log({1, 1}, *_lr);
    rl.append(entries);
    rl.set_committed(commit);
    if (t.exp_throw) {
      EXPECT_THROW(rl.commit(t.commit), rafter::util::panic)
          << CASE_INDEX(t, tests);
    } else {
      rl.commit(t.commit);
      EXPECT_EQ(rl.committed(), t.exp_commit) << CASE_INDEX(t, tests);
    }
  }
  co_return;
}

RAFTER_TEST_F(raft_log_test, compaction) {
  struct {
    uint64_t last_index;
    std::vector<uint64_t> compact;
    std::vector<uint64_t> exp_left;
    bool exp_throw;
  } tests[] = {
      // beyond upper bound
      {1000, {1001}, {1000}, true},
      // within range
      {1000, {300, 500, 800, 900, 1000}, {700, 500, 200, 100, 0}, false},
      // below lower bound
      {1000, {300, 299}, {700, 700}, true},
  };
  for (auto& t : tests) {
    auto to_append = test::util::new_entries({1, t.last_index + 1});
    co_await append_to_test_logdb(to_append, {}, true);
    auto rl = core::raft_log({1, 1}, *_lr);
    co_await rl.try_commit({t.last_index, t.last_index});
    rl.commit_update({.processed = rl.committed()});
    for (uint64_t j = 0; j < t.compact.size(); ++j) {
      try {
        co_await _lr->apply_compaction(t.compact[j]);
      } catch (rafter::util::raft_error& e) {
        EXPECT_TRUE(t.exp_throw) << CASE_INDEX(t, tests);
      }
      log_entry_vector entries;
      co_await rl.query(rl.first_index(), entries, UINT64_MAX);
      EXPECT_EQ(entries.size(), t.exp_left[j]);
    }
  }
}

}  // namespace
