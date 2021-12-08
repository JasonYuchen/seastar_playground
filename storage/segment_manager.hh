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
  explicit segment_manager(std::string log_dir);
  ~segment_manager();
  seastar::future<> start();
  seastar::future<> stop();

  // TODO: implement logdb
  seastar::future<bool> append(const protocol::update& up);
  seastar::future<> remove(protocol::group_id id, uint64_t index);
  seastar::future<protocol::snapshot> query_snapshot(protocol::group_id id);
  seastar::future<raft_state> query_raft_state(
      protocol::group_id id, uint64_t last_index);
  seastar::future<protocol::log_entry_vector> query_entries(
      protocol::group_id id, protocol::hint range, uint64_t max_size);
  seastar::future<> sync();

  std::string debug_string() const noexcept;

 private:
  seastar::future<> parse_existing_segments(seastar::directory_entry s);
  seastar::future<> update_index(const protocol::update& up, index::entry e);
  seastar::future<> rolling();
  seastar::future<> gc_service();
  seastar::future<> compaction(node_index& ni);
  static std::pair<unsigned, uint64_t> parse_segment_name(
      std::string_view name);

 private:
  bool _open = false;
  // segments dir, e.g. <data_dir>
  const std::string _log_dir;
  // TODO: more options
  static const uint64_t _rolling_size = 1024 * 1024 * 1024;
  static const uint64_t _gc_queue_length = 100;
  static constexpr char LOG_SUFFIX[] = "log";
  static constexpr uint64_t INVALID_FILENAME = 0;
  // used to allocate new segments
  // the format of a segment name is <shard_id:05d>_<segment_id:020d>
  uint64_t _next_filename = INVALID_FILENAME;
  // id -> segment, the _segments.rbegin() is the active segment
  std::map<uint64_t, std::unique_ptr<segment>> _segments;
  // id -> reference count
  std::map<uint64_t, uint64_t> _segments_ref_count;
  index_group _index_group;
  // segment ids ready for GC
  std::unique_ptr<seastar::queue<uint64_t>> _obsolete_queue;
  std::optional<seastar::future<>> _gc_service;

  seastar::shared_mutex _mutex;
};

}  // namespace rafter::storage
