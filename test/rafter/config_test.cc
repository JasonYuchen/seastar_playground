//
// Created by jason on 2022/6/5.
//

#include "rafter/config.hh"

#include <sstream>

#include "helper.hh"
#include "test/base.hh"

namespace {

using namespace rafter;

using helper = rafter::test::rafter_helper;
using rafter::test::l;

class config_test : public ::testing::Test {
 protected:
};

RAFTER_TEST_F(config_test, config_read_from) {
  std::stringstream s{
      "{"
      "rtt_ms: 200, "
      "data_dir: ., "
      "wal_rolling_size: 10, "
      "wal_gc_queue_capacity: 2, "
      "listen_address: 127.0.0.1, "
      "listen_port: 8866, "
      "snapshot_chunk_size: 99, "
      "in_memory_gc_timeout: 0, "
      "max_entry_bytes: 10976, "
      "max_replicate_entry_bytes: 20976, "
      "max_apply_entry_bytes: 30976, "
      "incoming_proposal_queue_length: 100, "
      "incoming_read_index_queue_length: 200, "
      "task_queue_capacity: 300, "
      "max_send_queue_bytes: 400, "
      "max_receive_queue_bytes: 500, "
      "lru_max_session_count: 128}"};
  auto cfg = config::read_from(s);
  ASSERT_EQ(cfg.rtt_ms, 200);
  ASSERT_EQ(cfg.data_dir, ".");
  ASSERT_EQ(cfg.wal_rolling_size, 10);
  ASSERT_EQ(cfg.wal_gc_queue_capacity, 2);
  ASSERT_EQ(cfg.listen_address, "127.0.0.1");
  ASSERT_EQ(cfg.listen_port, 8866);
  ASSERT_EQ(cfg.snapshot_chunk_size, 99);
  ASSERT_EQ(cfg.in_memory_gc_timeout, 0);
  ASSERT_EQ(cfg.max_entry_bytes, 10976);
  ASSERT_EQ(cfg.max_replicate_entry_bytes, 20976);
  ASSERT_EQ(cfg.max_apply_entry_bytes, 30976);
  ASSERT_EQ(cfg.incoming_proposal_queue_length, 100);
  ASSERT_EQ(cfg.incoming_read_index_queue_length, 200);
  ASSERT_EQ(cfg.task_queue_capacity, 300);
  ASSERT_EQ(cfg.max_send_queue_bytes, 400);
  ASSERT_EQ(cfg.max_receive_queue_bytes, 500);
  ASSERT_EQ(cfg.lru_max_session_count, 128);
}

}  // namespace
