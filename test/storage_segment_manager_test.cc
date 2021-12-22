//
// Creat
// ed by jason on 2021/12/11.
//
#include "storage/segment_manager.hh"

#include <random>

#include "rafter/config.hh"
#include "test/base.hh"
#include "test/util.hh"

using namespace rafter::protocol;
using namespace rafter::storage;
using namespace seastar;

using rafter::test::base;
using rafter::test::l;

namespace {

class segment_manager_test
    : public ::testing::Test,
      public ::testing::WithParamInterface<bool> {
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
      co_await recursive_remove_directory(_config.data_dir);
      co_await recursive_touch_directory(_config.data_dir + "/wal");
      _gids = {{1,1}, {1,2}, {2,1}, {2,2}, {3,3}};
      if (GetParam()) {
        co_await prepare_segments();
      }
      _manager = std::make_unique<segment_manager>(_config);
      co_await _manager->start();
      co_return;
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      co_await _manager->stop();
      _manager.reset();
      _gids.clear();
      _updates.clear();
      _index_group = {};
      co_return;
    });
  }

  future<> prepare_segments() {
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
    std::string wal_dir = std::filesystem::path(_config.data_dir).append("wal");
    auto seg1 = co_await segment::open(3, segment::form_path(wal_dir, 3));
    auto seg2 = co_await segment::open(4, segment::form_path(wal_dir, 4));
    auto ignored_segment = co_await segment::open(
        5, segment::form_path(wal_dir, this_shard_id() + 1, 5));
    for (size_t i = 0; i < _updates.size(); ++i) {
      index::entry ie;
      auto&& seg = (i < _updates.size() / 2) ? seg1 : seg2;
      ie.filename = seg->filename();
      ie.offset = seg->bytes();
      ie.length = co_await seg->append(_updates[i]) - ie.offset;
      _index_group.update(_updates[i], ie);
      _index.emplace_back(ie);
    }
    co_await seg1->sync();
    co_await seg1->close();
    co_await seg2->sync();
    co_await seg2->close();
    co_await ignored_segment->sync();
    co_await ignored_segment->close();
    co_return;
  }

  static inline rafter::config _config;
  std::unique_ptr<segment_manager> _manager;
  std::vector<group_id> _gids;
  std::vector<update> _updates;
  std::vector<index::entry> _index;
  index_group _index_group;
};

INSTANTIATE_TEST_SUITE_P(
    segment_manager,
    segment_manager_test,
    ::testing::Values(false, true),
    ::testing::PrintToStringParamName());

RAFTER_TEST_P(segment_manager_test, load_existing_segments) {
  if (GetParam()) {
    EXPECT_EQ(_index.front().filename, 3);
    EXPECT_EQ(_index.back().filename, 4);
    const auto& up1 = _updates.front();
    const auto& up2 = _updates.back();
    EXPECT_EQ(_manager->stats()._new_segment, 3) << "failed to load 2 segments";
    auto entries = co_await _manager->query_entries(
        up1.gid, {.low = up1.first_index, .high = up1.last_index}, UINT64_MAX);
    EXPECT_TRUE(rafter::test::util::compare(
        update{.entries_to_save = entries},
        update{.entries_to_save = up1.entries_to_save}))
        << "failed to query existing segment 3";
    entries = co_await _manager->query_entries(
        up2.gid, {.low = up2.first_index, .high = up2.last_index}, UINT64_MAX);
    EXPECT_TRUE(rafter::test::util::compare(
        update{.entries_to_save = entries},
        update{.entries_to_save = up2.entries_to_save}))
        << "failed to query existing segment 4";
    auto gid = up1.gid;
    size_t size = 0;
    log_entry_vector expected;
    for (const auto& up : _updates) {
      if (up.gid == gid) {
        size += rafter::test::util::extract_entries(up, expected);
      }
    }
    entries = co_await _manager->query_entries(
        gid,
        {.low = expected.front()->lid.index,
         .high = expected.back()->lid.index},
        size - 1);
    expected.pop_back();
    EXPECT_TRUE(rafter::test::util::compare(
        update{.entries_to_save = entries},
        update{.entries_to_save = expected}))
        << "failed to query across existing segments";
  }
  l.info("{}", _manager->debug_string());
  co_return;
}

RAFTER_TEST_P(segment_manager_test, append) {
  co_return;
}

RAFTER_TEST_P(segment_manager_test, remove) {
  co_return;
}

RAFTER_TEST_P(segment_manager_test, query_snapshot) {
  co_return;
}

RAFTER_TEST_P(segment_manager_test, query_raft_state) {
  co_return;
}

RAFTER_TEST_P(segment_manager_test, query_entries) {
  co_return;
}

RAFTER_TEST_P(segment_manager_test, integrated) {
  co_return;
}

}  // namespace
