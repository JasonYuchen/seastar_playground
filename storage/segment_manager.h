//
// Created by jason on 2021/10/13.
//

#pragma once

#include <seastar/core/queue.hh>
#include <seastar/core/shared_mutex.hh>

#include "index.hh"
#include "segment.hh"

namespace rafter::storage {

struct raft_state {
  protocol::hard_state hard_state;
  uint64_t first_index;
  uint64_t entry_count;
};

// sharded<segment_manager>
class segment_manager {
 public:
  // TODO: storage configuration class
  segment_manager(protocol::group_id id, std::filesystem::path data_dir);

  // TODO: implement logdb
  seastar::future<bool> append(const protocol::update& update);
  seastar::future<> remove(protocol::group_id id, uint64_t index);
  seastar::future<protocol::snapshot> query_snapshot(protocol::group_id id);
  seastar::future<raft_state> query_raft_state(
      protocol::group_id id, uint64_t last_index);
  seastar::future<protocol::log_entry_vector> query_entries(
      protocol::hint range, uint64_t max_size);
  seastar::future<> sync();

 private:
  seastar::future<> ensure_enough_space();
  seastar::future<> rolling();
  seastar::future<> segement_gc_service();
  seastar::future<> compaction(node_index& ni);
  void update_node_index(const protocol::update& update);

 private:
  const protocol::group_id _group_id;
  bool _open = false;
  // segments dir, e.g. <data_dir>
  std::filesystem::path _path;
  // TODO: more options
  const uint64_t _rolling_size = 1024 * 1024 * 1024;
  // used to allocate new segments
  // the format of a segment name is <shard_id:05d>_<segment_id:020d>
  uint64_t _next_filename = 0;
  // the minimal id of in-use segments
  uint64_t _minimal_in_use_filename = 0;
  // the segments from old to new, the _segments.back() is the active segment
  std::queue<std::unique_ptr<segment>> _segments;
  index_group _index_group;
  // segment ids ready for GC
  seastar::queue<uint64_t> _obsolete_queue;
};

}  // namespace rafter::storage
