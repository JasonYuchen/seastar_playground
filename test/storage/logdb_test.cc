//
// Created by jason on 2022/6/29.
//

#include "core/raft_log.hh"
#include "rafter/config.hh"
#include "storage/segment_manager.hh"
#include "test/base.hh"
#include "test/test_logdb.hh"
#include "test/util.hh"

using namespace rafter;
using namespace rafter::protocol;
using namespace rafter::storage;
using namespace seastar;

using rafter::test::base;
using rafter::test::l;

namespace {

class logdb_test : public ::testing::TestWithParam<std::string> {
 protected:
  void SetUp() override {
    base::submit([this]() -> future<> {
      // ignore exceptions
      co_await recursive_remove_directory(config::shard().data_dir)
          .handle_exception([](auto) {});
      co_await recursive_touch_directory(config::shard().data_dir);
      if (GetParam() == "test_logdb") {
        _logdb = std::make_unique<test::test_logdb>();
      }
      if (GetParam() == "segment_manager") {
        auto* db = new segment_manager(test::util::partition_func());
        co_await db->start();
        _logdb.reset(db);
      }
      co_return;
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      if (GetParam() == "segment_manager") {
        co_await dynamic_cast<segment_manager*>(_logdb.get())->stop();
      }
      _logdb.reset();
      co_return;
    });
  }

  // generate cluster ids that belong to the same shard
  // with default partitioner (i.e. modulo)
  static uint64_t generate_cluster_id(uint64_t sequence) {
    return sequence * smp::count;
  }

  std::unique_ptr<logdb> _logdb;
};

// test against all logdb implementations
INSTANTIATE_TEST_SUITE_P(
    storage,
    logdb_test,
    testing::Values(std::string("test_logdb"), std::string("segment_manager")));

RAFTER_TEST_P(logdb_test, missing_bootstrap) {
  auto info = co_await _logdb->load_bootstrap({1, 1});
  ASSERT_FALSE(info.has_value());
}

RAFTER_TEST_P(logdb_test, save_and_load_bootstrap) {
  // use special cluster id due to a naive modulo partitioner
  auto cluster_id_1 = generate_cluster_id(1);
  auto cluster_id_2 = generate_cluster_id(2);
  auto cluster_id_3 = generate_cluster_id(3);
  member_map members{{100, "address1"}, {200, "address2"}, {300, "address3"}};
  bootstrap info{
      .addresses = members,
      .join = true,
      .smtype = state_machine_type::regular};
  co_await _logdb->save_bootstrap({cluster_id_1, 2}, info);
  auto loaded_info = co_await _logdb->load_bootstrap({cluster_id_1, 2});
  ASSERT_TRUE(loaded_info.has_value());
  ASSERT_EQ(loaded_info.value(), info);
  auto nodes = co_await _logdb->list_nodes();
  ASSERT_EQ(nodes.size(), 1);
  ASSERT_EQ(nodes[0].cluster, cluster_id_1);
  ASSERT_EQ(nodes[0].node, 2);
  co_await _logdb->save_bootstrap({cluster_id_2, 3}, info);
  co_await _logdb->save_bootstrap({cluster_id_3, 4}, info);
  nodes = co_await _logdb->list_nodes();
  ASSERT_EQ(nodes.size(), 3);
}

RAFTER_TEST_P(logdb_test, DISABLED_snapshot_has_max_index_set) {
  // TODO(jyc): add check
  co_return;
}

RAFTER_TEST_P(
    logdb_test, DISABLED_save_snapshot_with_unexpected_entries_will_panic) {
  // TODO(jyc): add check
  co_return;
}

RAFTER_TEST_P(logdb_test, save_snapshot) {
  auto hs1 = hard_state{.term = 2, .vote = 3, .commit = 100};
  auto e1 = log_entry({.term = 1, .index = 10});
  e1.type = entry_type::application;
  e1.copy_of("test data");
  auto sp1 = test::util::new_snapshot({.term = 1, .index = 5});
  sp1->group_id = {3, 4};
  sp1->file_path = "p1";
  sp1->file_size = 100;
  auto ud1 = update{.gid = {3, 4}, .state = hs1, .snapshot = sp1};
  ud1.entries_to_save.emplace_back(std::move(e1));
  ud1.fill_meta();
  auto hs2 = hard_state{.term = 2, .vote = 3, .commit = 100};
  auto e2 = log_entry({.term = 1, .index = 20});
  e2.type = entry_type::application;
  e2.copy_of("test data");
  auto sp2 = test::util::new_snapshot({.term = 1, .index = 12});
  sp2->group_id = {3, 3};
  sp2->file_path = "p2";
  sp2->file_size = 200;
  auto ud2 = update{.gid = {3, 3}, .state = hs2, .snapshot = sp2};
  ud2.entries_to_save.emplace_back(std::move(e2));
  ud2.fill_meta();
  std::vector<update_pack> packs;
  packs.emplace_back(ud1);
  packs.emplace_back(ud2);
  co_await _logdb->save(packs);
  co_await packs[0].done.get_future();
  co_await packs[1].done.get_future();
  auto sp = co_await _logdb->query_snapshot({3, 4});
  ASSERT_TRUE(sp);
  ASSERT_EQ(sp->log_id, sp1->log_id);
  ASSERT_EQ(sp->file_path, sp1->file_path);
  ASSERT_EQ(sp->file_size, sp1->file_size);
  sp = co_await _logdb->query_snapshot({3, 3});
  ASSERT_TRUE(sp);
  ASSERT_EQ(sp->log_id, sp2->log_id);
  ASSERT_EQ(sp->file_path, sp2->file_path);
  ASSERT_EQ(sp->file_size, sp2->file_size);
  co_return;
}

RAFTER_TEST_P(logdb_test, snapshot_read_raft_state) {
  auto ss = test::util::new_snapshot({2, 100});
  auto hs = hard_state{.term = 2, .vote = 3, .commit = 100};
  auto ud = update{.gid = {3, 4}, .state = hs, .snapshot = ss};
  ud.fill_meta();
  update_pack pack{ud};
  co_await _logdb->save({&pack, 1});
  co_await pack.done.get_future();
  auto rs = co_await _logdb->query_raft_state({3, 4}, 100);
  // FIXME(jyc): snapshot is not included as first index
  // ASSERT_EQ(rs.first_index, 100);
  ASSERT_EQ(rs.entry_count, 0);
  ASSERT_EQ(rs.hard_state, hs);
}

RAFTER_TEST_P(logdb_test, open_new_db) {
  auto state = co_await _logdb->query_raft_state({2, 3}, 0);
  ASSERT_TRUE(state.empty());
  auto snap = co_await _logdb->query_snapshot({2, 3});
  ASSERT_FALSE(snap);
  log_entry_vector entries;
  auto size =
      co_await _logdb->query_entries({2, 3}, {0, 100}, entries, UINT64_MAX);
  ASSERT_TRUE(entries.empty());
  ASSERT_EQ(size, UINT64_MAX);
}

RAFTER_TEST_P(logdb_test, basic_read_write) {
  // TODO(jyc): different entry payload size
  auto ud1 = update{
      .gid = {2, 3},
      .state = {.term = 5, .vote = 3, .commit = 100},
      .entries_to_save =
          test::util::new_entries({{5, 198}, {5, 199}, {5, 200}}),
      .snapshot = test::util::new_snapshot({5, 197}),
  };
  ud1.fill_meta();
  auto ud2 = update{
      .gid = {2, 3},
      .state = {.term = 10, .vote = 6, .commit = 200},
      .entries_to_save =
          test::util::new_entries({{10, 201}, {10, 202}, {10, 203}}),
      .snapshot = test::util::new_snapshot({10, 200}),
  };
  ud2.fill_meta();
  {
    update_pack pack{ud1};
    co_await _logdb->save({&pack, 1});
    co_await pack.done.get_future();
  }
  {
    update_pack pack{ud2};
    co_await _logdb->save({&pack, 1});
    co_await pack.done.get_future();
  }
  auto rs = co_await _logdb->query_raft_state({2, 3}, 200);
  ASSERT_EQ(rs.hard_state, ud2.state);
  ASSERT_EQ(rs.first_index, 201);
  ASSERT_EQ(rs.entry_count, 3);
  auto sp = co_await _logdb->query_snapshot({2, 3});
  ASSERT_EQ(sp->log_id, ud2.snapshot->log_id);
  log_entry_vector queried;
  co_await _logdb->query_entries({2, 3}, {201, 203}, queried, UINT64_MAX);
  ASSERT_EQ(queried.size(), 2);
  ASSERT_EQ(queried[0], ud2.entries_to_save[0]);
}

}  // namespace
