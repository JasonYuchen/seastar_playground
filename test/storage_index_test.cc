//
// Created by jason on 2021/11/15.
//

#include "storage/index.hh"
#include "test/base.hh"
#include "util/error.hh"

#include <seastar/core/sleep.hh>

using namespace rafter;
using namespace seastar;

namespace {

protocol::group_id testing_id{1, 2};
std::vector<storage::index::entry> testing_indexes = {
    {
        .id = testing_id,
        .first_index = 10,
        .last_index = 12,
        .filename = 101,
        .offset = 100,
        .length = 10,
        .type = storage::index::entry::type::normal,
    },
    {
        .id = testing_id,
        .first_index = 13,
        .last_index = 15,
        .filename = 101,
        .offset = 110,
        .length = 20,
        .type = storage::index::entry::type::normal,
    },
    {
        .id = testing_id,
        .first_index = 16,
        .last_index = 18,
        .filename = 102,
        .offset = 130,
        .length = 30,
        .type = storage::index::entry::type::normal,
    },
    {
        .id = testing_id,
        .first_index = 19,
        .last_index = 21,
        .filename = 110,
        .offset = 160,
        .length = 40,
        .type = storage::index::entry::type::normal,
    },
};

class index_test : public ::testing::Test {
 protected:
  void SetUp() override {
    _init_idx = testing_indexes;
    for (auto&& e : testing_indexes) {
      _idx.update(e);
    }
  }

  storage::index _idx;
  std::vector<storage::index::entry> _init_idx;
};

RAFTER_TEST_F(index_test, binary_search) {
  struct {
    uint64_t raft_index;
    uint64_t expected_idx;
    bool expected_found;
  } tests[] = {
      {9, 0, false},
      {10, 0, true},
      {11, 0, true},
      {12, 0, true},
      {13, 1, true},
      {14, 1, true},
      {15, 1, true},
      {16, 2, true},
      {17, 2, true},
      {18, 2, true},
      {19, 3, true},
      {20, 3, true},
      {21, 3, true},
      {22, 0, false},
  };
  for (auto [ri, ei, ef] : tests) {
    auto [i, f] = _idx.binary_search(0, _idx.size() - 1, ri);
    EXPECT_EQ(i, ei) << "raft_index=" << ri;
    EXPECT_EQ(f, ef) << "raft_index=" << ri;
  }
  co_return;
}

RAFTER_TEST_F(index_test, update_invalid) {
  EXPECT_THROW(_idx.update({.first_index = 9, .last_index = 10}),
               rafter::util::logic_error);
  EXPECT_THROW(_idx.update({.first_index = 23, .last_index = 24}),
               rafter::util::logic_error);
  co_return;
}

RAFTER_TEST_F(index_test, update_overwrite) {
  auto e = storage::index::entry{
      .id = {1, 2},
      .first_index = 19,
      .last_index = 25,
      .filename = 111,
      .offset = 161,
      .length = 41,
      .type = storage::index::entry::type::normal,
  };
  _idx.update(e);
  auto es = _idx.query({.low = 19, .high = 25});
  EXPECT_EQ(es.size(), 1);
  EXPECT_EQ(es[0], e);

  e = storage::index::entry{
      .id = {1, 2},
      .first_index = 17,
      .last_index = 20,
      .filename = 112,
      .offset = 42,
      .type = storage::index::entry::type::normal,
  };
  _idx.update(e);
  es = _idx.query({.low = 16, .high = 25});
  EXPECT_EQ(es.size(), 2);
  EXPECT_EQ(es[0].first_index, 16);
  EXPECT_EQ(es[0].last_index, 16);
  EXPECT_EQ(es[0].filename, 102);
  EXPECT_EQ(es[1], e);

  e = storage::index::entry{
      .id = {1, 2},
      .first_index = 13,
      .last_index = 14,
      .filename = 113,
      .offset = 43,
      .type = storage::index::entry::type::normal,
  };
  _idx.update(e);
  es = _idx.query({.low = 10, .high = 25});
  EXPECT_EQ(es.size(), 2);
  EXPECT_EQ(es[0].first_index, 10);
  EXPECT_EQ(es[0].last_index, 12);
  EXPECT_EQ(es[0].filename, 101);
  EXPECT_EQ(es[1], e);

  e = storage::index::entry{
      .id = {1, 2},
      .first_index = 10,
      .last_index = 20,
      .filename = 114,
      .offset = 44,
      .type = storage::index::entry::type::normal,
  };
  _idx.update(e);
  es = _idx.query({.low = 10, .high = 25});
  EXPECT_EQ(es.size(), 1);
  EXPECT_EQ(es[0], e);
  co_return;
}

RAFTER_TEST_F(index_test, query) {
  struct {
    rafter::protocol::hint range;
    std::vector<storage::index::entry> expected_idx;
  } tests[] = {
      {{9, 10},{},},
      {{10, 10},{_init_idx[0]}},
      {{10, 11},{_init_idx[0]}},
      {{11, 13},{_init_idx[0], _init_idx[1]}},
      {{12, 16},{_init_idx[0], _init_idx[1], _init_idx[2]}},
      {{15, 21},{_init_idx[1], _init_idx[2], _init_idx[3]}},
      {{21, 22},{_init_idx[3]}},
      {{22, 23},{}},
  };
  for (auto&& [range, expect] : tests) {
    auto es = _idx.query(range);
    auto ves = std::vector<storage::index::entry>{es.begin(), es.end()};
    EXPECT_EQ(ves, expect)
        << fmt::format("query [{}, {}] failed", range.low, range.high);
  }
  co_return;
}

RAFTER_TEST_F(index_test, compaction) {
  EXPECT_EQ(_idx.compacted_to(), protocol::log_id::invalid_index);
  EXPECT_EQ(_idx.compaction(), 0);
  struct {
    uint64_t compacted_to;
    uint64_t max_obsolete;
  } tests[] = {
      {9, 0},
      {10, 0},
      {12, 0},
      {15, 101},
      {16, 101},
      {18, 102},
      {20, 102},
      {21, 102},
      {23, 102},
  };
  for (auto&& [compacted_to, max_obsolete] : tests) {
    _idx.set_compacted_to(compacted_to);
    EXPECT_EQ(_idx.compacted_to(), compacted_to);
    EXPECT_EQ(_idx.compaction(), max_obsolete);
  }
  _idx.set_compacted_to(5);
  EXPECT_EQ(_idx.compacted_to(), 23) << "compaction index move backward";
  co_return;
}

RAFTER_TEST_F(index_test, remove_obsolete_entries) {
  struct {
    uint64_t compacted_to;
    // remain in the index object but no longer accessible due to compaction
    std::vector<storage::index::entry> remaining_idx;
    // still accessible indexes after compaction
    std::vector<storage::index::entry> expected_idx;
  } tests[] = {
      {9, {_init_idx}, {_init_idx}},
      {10, {_init_idx}, {_init_idx}},
      {12, {_init_idx}, {_init_idx[1], _init_idx[2], _init_idx[3]}},
      {15, {_init_idx[2], _init_idx[3]}, {_init_idx[2], _init_idx[3]}},
      {16, {_init_idx[2], _init_idx[3]}, {_init_idx[2], _init_idx[3]}},
      {18, {_init_idx[3]}, {_init_idx[3]}},
      {20, {_init_idx[3]}, {_init_idx[3]}},
      {21, {_init_idx[3]}, {}},
      {23, {_init_idx[3]}, {}},
  };
  for (auto&& [compacted_to, remaining_idx, expected_idx] : tests) {
    _idx.set_compacted_to(compacted_to);
    _idx.remove_obsolete_entries(_idx.compaction());
    EXPECT_EQ(_idx.size(), remaining_idx.size());
    EXPECT_TRUE(_idx.query({compacted_to, UINT64_MAX}).empty());
    auto es = _idx.query({compacted_to + 1, UINT64_MAX});
    auto ves = std::vector<storage::index::entry>{es.begin(), es.end()};
    EXPECT_EQ(ves, expected_idx) << "failed when compacted_to:" << compacted_to;
  }
  co_return;
}

class node_index_test : public ::testing::Test {
 protected:
  void SetUp() override {
    // TODO
  }

  storage::node_index _idx{testing_id};
};

RAFTER_TEST_F(node_index_test, update_query_entry) {
  // TODO
  co_return;
}

RAFTER_TEST_F(node_index_test, update_query_state) {
  // TODO
  co_return;
}

RAFTER_TEST_F(node_index_test, update_query_snapshot) {
  // TODO
  co_return;
}

RAFTER_TEST_F(node_index_test, compaction) {
  // TODO
  co_return;
}

class index_group_test : public ::testing::Test {
 protected:
  void SetUp() override {
    // TODO
  }
};

}  // namespace
