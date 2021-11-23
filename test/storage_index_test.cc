//
// Created by jason on 2021/11/15.
//

#include "storage/index.hh"

#include "test/base.hh"

using namespace rafter::storage;
using namespace seastar;

namespace {

class index_basic : public ::testing::Test {
 protected:
  void SetUp() override {
    rafter::protocol::group_id id{1, 2};
    uint64_t fn = 101;
    auto tp = index::entry::type::normal;
    index::entry es[] = {
        {
            .id = id,
            .first_index = 10,
            .last_index = 12,
            .filename = fn,
            .offset = 100,
            .length = 10,
            .type = tp,
        },
        {
            .id = id,
            .first_index = 13,
            .last_index = 15,
            .filename = fn,
            .offset = 110,
            .length = 20,
            .type = tp,
        },
        {
            .id = id,
            .first_index = 16,
            .last_index = 18,
            .filename = fn,
            .offset = 130,
            .length = 30,
            .type = tp,
        },
        {
            .id = id,
            .first_index = 19,
            .last_index = 21,
            .filename = fn,
            .offset = 160,
            .length = 40,
            .type = tp,
        },
    };
    for (auto&& e : es) {
      idx.update(e);
    }
  }

  rafter::storage::index idx;
};

RAFTER_TEST_F(index_basic, binary_search) {
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
    auto [i, f] = idx.binary_search(0, idx.size() - 1, ri);
    EXPECT_EQ(i, ei) << "raft_index=" << ri;
    EXPECT_EQ(f, ef) << "raft_index=" << ri;
  }
  co_return;
}

RAFTER_TEST_F(index_basic, update) {

  co_return;
}

}  // namespace
