//
// Created by jason on 2021/9/24.
//

#include "storage/segment.hh"

#include <memory>

#include "test/base.hh"

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

  future<> dump_updates() {
    index::entry e {
        .filename = 1,
    };
    rafter::protocol::update up {
        .group_id = {1, 2},

    };
    co_await _segment->append(up);
  }

  static inline const std::string test_dir = "test_data";
  static inline const std::string test_file = "00000_00000000000000000001.log";
  std::unique_ptr<segment> _segment;
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
