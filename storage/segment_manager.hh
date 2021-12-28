//
// Created by jason on 2021/10/13.
//

#pragma once

#include <seastar/core/queue.hh>
#include <seastar/core/shared_mutex.hh>

#include "rafter/config.hh"
#include "storage/index.hh"
#include "storage/segment.hh"
#include "storage/stats.hh"

namespace rafter::storage {

struct raft_state {
  protocol::hard_state hard_state;
  uint64_t first_index = protocol::log_id::invalid_index;
  uint64_t entry_count = 0;
};

// sharded<segment_manager>
class segment_manager {
 public:
  // TODO: storage configuration class
  explicit segment_manager(const config& config);
  ~segment_manager() = default;
  seastar::future<> start();
  seastar::future<> stop();
  stats stats() const noexcept;

  // TODO: implement logdb
  // TODO: batch append
  seastar::future<bool> append(const protocol::update& up);
  seastar::future<> remove(protocol::group_id id, uint64_t index);
  seastar::future<protocol::snapshot_ptr> query_snapshot(protocol::group_id id);
  seastar::future<raft_state> query_raft_state(
      protocol::group_id id, uint64_t last_index);
  seastar::future<protocol::log_entry_vector> query_entries(
      protocol::group_id id, protocol::hint range, uint64_t max_size);
  seastar::future<> sync();

  std::string debug_string() const noexcept;

 private:
  void must_open() const;
  seastar::future<> parse_existing_segments(seastar::directory_entry s);
  seastar::future<> recovery_compaction();
  seastar::future<> update_index(const protocol::update& up, index::entry e);
  seastar::future<> rolling();
  seastar::future<> gc_service();
  seastar::future<> compaction(protocol::group_id id);

 private:
  const config& _config;
  bool _open = false;
  // segments dir, e.g. <data_dir>
  std::string _log_dir;
  // TODO: more options

  // used to allocate new segments
  // the format of a segment name is <shard_id:05d>_<segment_id:020d>
  uint64_t _next_filename = segment::INVALID_FILENAME;
  // id -> segment, the _segments.rbegin() is the active segment
  std::map<uint64_t, std::unique_ptr<segment>> _segments;
  // id -> reference count
  std::map<uint64_t, uint64_t> _segments_ref_count;
  index_group _index_group;
  // segment ids ready for GC
  std::unique_ptr<seastar::queue<uint64_t>> _obsolete_queue;
  std::optional<seastar::future<>> _gc_service;

  struct stats _stats;
};

}  // namespace rafter::storage
