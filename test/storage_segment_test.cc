//
// Created by jason on 2021/9/24.
//

#include "storage/segment.hh"

#include <random>

#include "test/base.hh"

using namespace rafter::protocol;
using namespace rafter::storage;
using namespace seastar;

using rafter::test::base;
using rafter::test::l;

namespace {

class segment_test : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    base::submit([]() -> future<> {
      co_await recursive_touch_directory(test_dir);
    });
  }

  static void TearDownTestSuite() {
    base::submit([]() -> future<> {
      co_await recursive_remove_directory(test_dir);
    });
  }

  void SetUp() override {
    base::submit([this]() -> future<> {
      _segment = co_await segment::open(
          1, test_dir + "/" + test_file);
      std::vector<index::entry> ies;
      auto ups = generate_updates();
      for (const auto& up : ups) {
        auto&& ie = _indexes.emplace_back();
        ie.id = up.group_id;
        ie.first_index = up.first_index;
        ie.last_index = up.last_index;
        ie.filename = 1;
        ie.offset = _segment->bytes();
        ie.length = co_await _segment->append(up) - ie.offset;
        ie.type = index::entry::type::normal;
        EXPECT_EQ(ie.offset, 548 * ie.first_index / 5);
        EXPECT_EQ(ie.length, 548);
      }
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      co_await _segment->close();
      co_await remove_file(test_dir + "/" + test_file);
      // data created on a shard must also be released in this shard
      _segment.reset(nullptr);
    });
  }

  std::vector<update> generate_updates() {
    std::vector<update> updates;
    hard_state state {.term = 1, .vote = 2, .commit = 0};
    for (size_t i = 0; i < 10; ++i) {
      auto&& up = updates.emplace_back();
      up.group_id = {.cluster = 2, .node = 1};
      state.term += i % 2;
      state.commit++;
      up.state = state;
      up.first_index = i * 5 + 1;
      up.last_index = (i + 1) * 5;
      for (size_t j = up.first_index; j <= up.last_index; ++j) {
        up.entries_to_save.emplace_back(make_lw_shared<log_entry>());
        auto&& e = *up.entries_to_save.back();
        e.id = {.term = state.term, .index = j};
        e.payload = "test_payload";
      }
      up.snapshot = make_lw_shared<snapshot>();
      auto&& sn = *up.snapshot;
      sn.group_id = up.group_id;
      sn.log_id = {.term = state.term, .index = up.last_index};
      sn.file_path = "test_snapshot";
    }
    return updates;
  }

  static inline const std::string test_dir = "test_data";
  static inline const std::string test_file = "00000_00000000000000000001.log";
  std::unique_ptr<segment> _segment;
  std::vector<index::entry> _indexes;
};

RAFTER_TEST_F(segment_test, create) {
  EXPECT_TRUE(*_segment);
  l.info("{}", _segment->debug());
  co_return;
}

RAFTER_TEST_F(segment_test, open) {
  co_return;
}

RAFTER_TEST_F(segment_test, append) {
  co_return;
}

RAFTER_TEST_F(segment_test, query_entry) {
  co_return;
}

RAFTER_TEST_F(segment_test, query_state) {
  co_return;
}

RAFTER_TEST_F(segment_test, query_snapshot) {
  co_return;
}

RAFTER_TEST_F(segment_test, batch_query_entry) {
  co_return;
}

RAFTER_TEST_F(segment_test, list_update) {
  co_return;
}

}  // namespace
