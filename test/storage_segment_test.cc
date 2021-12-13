//
// Created by jason on 2021/9/24.
//

#include "storage/segment.hh"

#include <random>

#include "test/base.hh"
#include "test/util.hh"
#include "util/util.hh"

using namespace rafter::protocol;
using namespace rafter::storage;
using namespace seastar;

using rafter::test::base;
using rafter::test::l;

namespace {

class segment_test : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    _config = rafter::test::util::default_config();
    base::submit([]() -> future<> {
      co_await recursive_touch_directory(_config.data_dir);
    });
  }

  static void TearDownTestSuite() {
    base::submit([]() -> future<> {
      co_await recursive_remove_directory(_config.data_dir);
    });
  }

  void SetUp() override {
    base::submit([this]() -> future<> {
      _segment = co_await segment::open(
          _segment_filename,
          segment::form_path(_config.data_dir, _segment_filename),
          false);
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      _gids.clear();
      _updates.clear();
      _index_group = {};
      co_await _segment->close();
      co_await remove_file(
          segment::form_path(_config.data_dir, _segment_filename));
      // data created on a shard must also be released in this shard
      _segment.reset(nullptr);
    });
  }

  future<> fulfill_segment() {
    EXPECT_TRUE(*_segment);
    std::random_device rd;
    std::mt19937 g(rd());
    std::uniform_int_distribution<int> r(0, 100);
    std::vector<std::vector<update>> gid_updates;
    for (auto gid : _gids) {
      gid_updates.emplace_back(
          rafter::test::util::make_updates(gid, 10, 1, 3, 4));
    }
    for (size_t i = 0; i < 10; ++i) {
      std::vector<update> shuffled;
      for (size_t j = 0; j < _gids.size(); ++j) {
        shuffled.emplace_back(gid_updates[j][i]);
      }
      std::shuffle(shuffled.begin(), shuffled.end(), g);
      _updates.insert(_updates.end(), shuffled.begin(), shuffled.end());
    }
    for (const auto& up : _updates) {
      index::entry ie;
      ie.filename = 1;
      ie.offset = _segment->bytes();
      ie.length = co_await _segment->append(up) - ie.offset;
      _index_group.update(up, ie);
      _index.emplace_back(ie);
    }
  }

  static inline rafter::config _config;
  static inline constexpr uint64_t _segment_filename = 1;
  std::vector<group_id> _gids;
  std::vector<update> _updates;
  std::vector<index::entry> _index;
  index_group _index_group;
  std::unique_ptr<segment> _segment;
};

RAFTER_TEST_F(segment_test, create) {
  EXPECT_TRUE(*_segment) << _segment->debug_string();
  _gids = {{1,1}, {1,2}, {2,1}, {2,2}, {3,3}};
  co_await fulfill_segment();
  EXPECT_GT(_segment->bytes(), 0) << _segment->debug_string();
  co_return;
}

RAFTER_TEST_F(segment_test, open_and_list) {
  index_group ig;
  std::vector<index::entry> ie;
  _gids = {{1,1}, {1,2}, {2,1}, {2,2}, {3,3}};
  co_await fulfill_segment();
  co_await _segment->list_update(
      [&ig, &ie](const update& up, index::entry e) -> future<> {
        ig.update(up, e);
        ie.emplace_back(e);
        co_return;
      });
  if (ig != _index_group || ie != _index) {
    l.error("failed to list_update in an on-the-fly segment");
    EXPECT_TRUE(false) << "Write:\n" << rafter::util::print(_index)
                       << "Read:\n" << rafter::util::print(ie);
  }

  auto temp_segment = co_await segment::open(
      _segment_filename,
      segment::form_path(_config.data_dir, _segment_filename),
      true);
  EXPECT_TRUE(*temp_segment) << temp_segment->debug_string();
  ig = index_group{};
  ie.clear();
  co_await _segment->list_update(
      [&ig, &ie](const update& up, index::entry e) -> future<> {
        ig.update(up, e);
        ie.emplace_back(e);
        co_return;
      });
  if (ig != _index_group || ie != _index) {
    l.error("failed to list_update in an archived segment");
    EXPECT_TRUE(false) << "Write:\n" << rafter::util::print(_index)
                       << "Read:\n" << rafter::util::print(ie);
  }
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

}  // namespace
