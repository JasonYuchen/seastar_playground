//
// Created by jason on 2022/6/29.
//

#include "core/raft_log.hh"
#include "rafter/config.hh"
#include "storage/segment_manager.hh"
#include "test/base.hh"
#include "test/util.hh"

using namespace rafter;
using namespace rafter::protocol;
using namespace rafter::storage;
using namespace seastar;

using rafter::test::base;
using rafter::test::l;

namespace {

// TODO(jyc): test test_logdb and segment_manager
class logdb_test : public ::testing::Test {
 protected:
  void SetUp() override {
    base::submit([this]() -> future<> {
      // ignore exceptions
      co_await recursive_remove_directory(config::shard().data_dir)
          .handle_exception([](auto) {});
      co_await recursive_touch_directory(config::shard().data_dir);
      auto db = std::make_unique<segment_manager>(test::util::partition_func());
      co_await db->start();
      _logdb.reset(db.release());
      co_return;
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      co_await dynamic_cast<segment_manager*>(_logdb.get())->stop();
      _logdb.reset();
      co_return;
    });
  }
  std::unique_ptr<logdb> _logdb;
};

RAFTER_TEST_F(logdb_test, missing_bootstrap) {
  auto info = co_await _logdb->load_bootstrap({1, 1});
  ASSERT_FALSE(info.has_value());
}

RAFTER_TEST_F(logdb_test, save_and_load_bootstrap) {
  // use special cluster id due to a naive modulo partitioner
  uint64_t cluster_id_1 = 1 * smp::count;
  uint64_t cluster_id_2 = 2 * smp::count;
  uint64_t cluster_id_3 = 3 * smp::count;
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

}  // namespace
