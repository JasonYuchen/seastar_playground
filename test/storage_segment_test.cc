//
// Created by jason on 2021/9/24.
//

#include <random>

#include "protocol/serializer.hh"
#include "storage/segment.hh"
#include "test/base.hh"
#include "test/util.hh"
#include "util/error.hh"
#include "util/util.hh"

using namespace rafter;
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
      config::initialize(test::util::default_config());
      co_await recursive_touch_directory(config::shard().data_dir);
    });
  }

  static void TearDownTestSuite() {
    base::submit([]() -> future<> {
      co_await recursive_remove_directory(config::shard().data_dir);
    });
  }

  void SetUp() override {
    base::submit([this]() -> future<> {
      _segment = co_await segment::open(
          SEGMENT_FILENAME,
          segment::form_path(config::shard().data_dir, SEGMENT_FILENAME),
          false);
      _gids = {{1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 3}};
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
          segment::form_path(config::shard().data_dir, SEGMENT_FILENAME)));
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

  static inline constexpr uint64_t SEGMENT_FILENAME = 1;
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
    ADD_FAILURE() << "Write:\n"
                  << rafter::util::print(_index) << "Read:\n"
                  << rafter::util::print(ie);
  }

  auto temp_segment = co_await segment::open(
      SEGMENT_FILENAME,
      segment::form_path(config::shard().data_dir, SEGMENT_FILENAME),
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
    ADD_FAILURE() << "Write:\n"
                  << rafter::util::print(_index) << "Read:\n"
                  << rafter::util::print(ie);
  }
  co_return;
}

RAFTER_TEST_F(segment_test, partial_written) {
  auto path = segment::form_path(config::shard().data_dir, SEGMENT_FILENAME);
  auto file = co_await open_file_dma(path, open_flags::wo);
  // truncate the update but leave a complete meta
  co_await file.truncate(_index.back().offset + _index.back().length - 1);
  co_await file.flush();
  std::vector<index::entry> ie;
  auto appender = [&ie](const update& up, index::entry e) -> future<> {
    ie.emplace_back(e);
    co_return;
  };
  co_await _segment->list_update(appender);
  EXPECT_EQ(ie.size() + 1, _index.size());
  ie.emplace_back(_index.back());
  if (ie != _index) {
    l.error("failed to list_update in an truncated segment");
    ADD_FAILURE() << "Write:\n"
                  << rafter::util::print(_index) << "Read:\n"
                  << rafter::util::print(ie);
  }
  // truncate the update but leave a complete meta
  co_await file.truncate(_index.back().offset + 1);
  co_await file.flush();
  ie.clear();
  co_await _segment->list_update(appender);
  EXPECT_EQ(ie.size() + 1, _index.size());
  ie.emplace_back(_index.back());
  if (ie != _index) {
    l.error("failed to list_update in an truncated segment");
    ADD_FAILURE() << "Write:\n"
                  << rafter::util::print(_index) << "Read:\n"
                  << rafter::util::print(ie);
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
    ADD_FAILURE() << "failed to append a 10 MiB entry";
  }
  co_return;
}

RAFTER_TEST_F(segment_test, query) {
  auto stat = co_await file_stat(
      segment::form_path(config::shard().data_dir, SEGMENT_FILENAME));
  EXPECT_GE(stat.size, _index.back().offset + _index.back().length);
  for (size_t i = 0; i < _updates.size(); ++i) {
    auto up = co_await _segment->query(_index[i]);
    if (!rafter::test::util::compare(up, _updates[i])) {
      ADD_FAILURE() << "inconsistent update";
      l.error("the {} update not equal with {}", i, _index[i].debug_string());
      co_return;
    }
  }
  auto ie = _index[0];
  ie.filename = SEGMENT_FILENAME + 1;
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
  update u1{.entries_to_save = fetched};
  update u2{.entries_to_save = expected};
  if (!rafter::test::util::compare(u1, u2)) {
    ADD_FAILURE() << fmt::format("batch query failed");
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
    ADD_FAILURE() << fmt::format(
        "batch query failed with size limitation {}", entry_size - cutoff);
  }
  co_return;
}

RAFTER_TEST(static_segment_test, parse_name) {
  struct {
    std::string_view entry;
    std::pair<unsigned, uint64_t> expected;
  } tests[] = {
      {".", {0, segment::INVALID_FILENAME}},
      {"..", {0, segment::INVALID_FILENAME}},
      {"some_dir/", {0, segment::INVALID_FILENAME}},
      {"123_567.log", {0, segment::INVALID_FILENAME}},
      {"00123_01234567890123456789.logex", {0, segment::INVALID_FILENAME}},
      {"00123_99999999999999999999.log", {0, segment::INVALID_FILENAME}},
      {"0012X_01234567890123456789.log", {0, segment::INVALID_FILENAME}},
      {"00123_0123456789012345678X.log", {0, segment::INVALID_FILENAME}},
      {"00123_01234567890123456789.log", {123, 1234567890123456789ULL}},
  };
  for (const auto& [entry, expected] : tests) {
    EXPECT_EQ(segment::parse_name(entry), expected) << "parsing " << entry;
  }
  co_return;
}

}  // namespace
