//
// Created by jason on 2021/9/24.
//

#include "storage/segment.hh"

#include <random>

#include "protocol/serializer.hh"
#include "test/base.hh"
#include "test/util.hh"
#include "util/error.hh"
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
      _gids = {{1,1}, {1,2}, {2,1}, {2,2}, {3,3}};
      co_await fulfill_segment();
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      _gids.clear();
      _updates.clear();
      _index_group = {};
      co_await _segment->close();
      co_await _segment->remove();
      EXPECT_FALSE(co_await file_exists(
          segment::form_path(_config.data_dir, _segment_filename)));
      // data created on a shard must also be released in this shard
      _segment.reset(nullptr);
    });
  }

  future<> fulfill_segment() {
    EXPECT_TRUE(*_segment);
    std::random_device rd;
    std::mt19937 g(rd());
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
  EXPECT_GT(_segment->bytes(), 0) << _segment->debug_string();
  co_return;
}

RAFTER_TEST_F(segment_test, open_and_list) {
  index_group ig;
  std::vector<index::entry> ie;
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

RAFTER_TEST_F(segment_test, append_large_entry) {
  update up;
  up.first_index = 100;
  up.last_index = 100;
  auto en = up.entries_to_save.emplace_back(make_lw_shared<log_entry>());
  en->payload = std::string(10 * MB, 'c');
  index::entry ie;
  ie.filename = 1;
  ie.offset = _segment->bytes();
  ie.length = co_await _segment->append(up) - ie.offset;
  auto up2 = co_await _segment->query(ie);
  if (!rafter::test::util::compare(up, up2)) {
    EXPECT_TRUE(false) << "failed to append a 10 MiB entry";
  }
  co_return;
}

RAFTER_TEST_F(segment_test, query) {
  auto stat = co_await file_stat(
      segment::form_path(_config.data_dir, _segment_filename));
  EXPECT_GE(stat.size, _index.back().offset + _index.back().length);
  for (size_t i = 0; i < _updates.size(); ++i) {
    auto up = co_await _segment->query(_index[i]);
    if (!rafter::test::util::compare(up, _updates[i])) {
      EXPECT_TRUE(false) << "inconsistent update";
      l.error("the {} update not equal with {}", i, _index[i].debug_string());
      co_return;
    }
  }
  auto ie = _index[0];
  ie.filename = _segment_filename + 1;
  EXPECT_THROW(co_await _segment->query(ie), rafter::util::logic_error)
      << "incorrect filename should be reported: " << _segment->debug_string();
  co_return;
}

RAFTER_TEST_F(segment_test, batch_query) {
  std::vector<index::entry> query;
  log_entry_vector expected;
  size_t entry_size = 0;
  auto gid = _updates.front().gid;
  for (size_t i = 0; i < _index.size(); ++i) {
    if (_updates[i].gid != gid) {
      continue;
    }
    auto& ie = query.emplace_back(_index[i]);
    ie.first_index = _updates[i].first_index;
    ie.last_index = _updates[i].last_index;
    entry_size += rafter::test::util::extract_entries(_updates[i], expected);
  }
  log_entry_vector fetched;
  auto left = co_await _segment->query(query, fetched, UINT64_MAX);
  EXPECT_EQ(left, UINT64_MAX - entry_size);
  update u1 {.entries_to_save = fetched};
  update u2 {.entries_to_save = expected};
  if (!rafter::test::util::compare(u1, u2)) {
    EXPECT_TRUE(false) << fmt::format("batch query failed");
  }

  fetched.clear();
  size_t cutoff = 10;
  left = co_await _segment->query(query, fetched, entry_size - cutoff);
  EXPECT_EQ(left, 0);
  EXPECT_EQ(fetched.size() + 1, expected.size());
  u1.entries_to_save = fetched;
  u2.entries_to_save = expected;
  u2.entries_to_save.pop_back();
  if (!rafter::test::util::compare(u1, u2)) {
    EXPECT_TRUE(false)
        << fmt::format("batch query failed with size limitation {}",
                       entry_size - cutoff);
  }
  co_return;
}

RAFTER_TEST(static_segment_test, form_name) {
  // TODO
  co_return;
}

RAFTER_TEST(static_segment_test, form_path) {
  // TODO
  co_return;
}

RAFTER_TEST(static_segment_test, parse_name) {
  // TODO
  co_return;
}

}  // namespace
