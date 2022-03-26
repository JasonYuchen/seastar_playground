//
// Created by jason on 2021/10/13.
//

#pragma once

#include <seastar/core/queue.hh>
#include <seastar/core/shared_mutex.hh>

#include "storage/index.hh"
#include "storage/logdb.hh"
#include "storage/segment.hh"
#include "storage/stats.hh"
#include "util/worker.hh"

namespace rafter::storage {

// sharded<segment_manager>
class segment_manager final : public logdb {
 public:
  explicit segment_manager();
  ~segment_manager() = default;
  seastar::future<> start();
  seastar::future<> stop();
  stats stats() const noexcept;

  std::string name() const noexcept override { return "segment_manager"; }
  seastar::future<> save_bootstrap(
      protocol::group_id id, const protocol::bootstrap& info) override;
  seastar::future<std::optional<protocol::bootstrap>> load_bootstrap(
      protocol::group_id id) override;
  seastar::future<> save(std::span<protocol::update> updates) override;
  seastar::future<size_t> query_entries(
      protocol::group_id id,
      protocol::hint range,
      protocol::log_entry_vector& entries,
      uint64_t max_bytes) override;
  seastar::future<raft_state> query_raft_state(
      protocol::group_id id, uint64_t last_index) override;
  seastar::future<protocol::snapshot_ptr> query_snapshot(
      protocol::group_id id) override;
  seastar::future<> remove(protocol::group_id id, uint64_t index) override;
  seastar::future<> remove_node(protocol::group_id id) override;
  seastar::future<> import_snapshot(protocol::snapshot_ptr snapshot) override;

  seastar::future<bool> append(const protocol::update& up);
  seastar::future<> sync();

  std::string debug_string() const noexcept;

 private:
  seastar::future<> parse_existing_segments(seastar::directory_entry s);
  seastar::future<> recovery_compaction();
  seastar::future<> update_index(const protocol::update& up, index::entry e);
  seastar::future<> rolling();
  seastar::future<> gc_service(std::vector<uint64_t>& segs, bool& open);
  seastar::future<> compaction(protocol::group_id id);

  // segments dir, e.g. <data_dir>
  std::string _log_dir;
  // TODO(jyc): more options

  // used to allocate new segments
  // the format of a segment name is <shard_id:05d>_<segment_id:020d>
  uint64_t _next_filename = segment::INVALID_FILENAME;
  // id -> segment, the _segments.rbegin() is the active segment
  std::map<uint64_t, std::unique_ptr<segment>> _segments;
  // id -> reference count
  std::map<uint64_t, uint64_t> _segments_ref_count;
  index_group _index_group;
  // segment ids ready for GC
  util::worker<uint64_t> _gc_worker;

  struct stats _stats;
};

}  // namespace rafter::storage
