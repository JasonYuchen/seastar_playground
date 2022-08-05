//
// Created by jason on 2021/10/13.
//

#pragma once

#include <seastar/core/queue.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_mutex.hh>

#include "storage/index.hh"
#include "storage/logdb.hh"
#include "storage/segment.hh"
#include "storage/stats.hh"
#include "util/seastarx.hh"
#include "util/worker.hh"

namespace rafter::storage {

class segment_manager final
  : public async_sharded_service<segment_manager>
  , public peering_sharded_service<segment_manager>
  , public logdb {
 public:
  explicit segment_manager(std::function<unsigned(uint64_t)> func);
  ~segment_manager() override = default;
  future<> start();
  future<> stop();
  stats stats() const noexcept;

  std::string name() const noexcept override { return "segment_manager"; }
  future<std::vector<protocol::group_id>> list_nodes() override;
  future<> save_bootstrap(
      protocol::group_id id, const protocol::bootstrap& info) override;
  future<std::optional<protocol::bootstrap>> load_bootstrap(
      protocol::group_id id) override;
  future<> save(std::span<update_pack> updates) override;
  future<size_t> query_entries(
      protocol::group_id id,
      protocol::hint range,
      protocol::log_entry_vector& entries,
      uint64_t max_bytes) override;
  future<raft_state> query_raft_state(
      protocol::group_id id, uint64_t last_index) override;
  future<protocol::snapshot_ptr> query_snapshot(protocol::group_id id) override;
  future<> remove(protocol::group_id id, uint64_t index) override;
  future<> remove_node(protocol::group_id id) override;
  future<> import_snapshot(protocol::snapshot_ptr snapshot) override;

  future<bool> append(const protocol::update& up);
  future<> sync();

  std::string debug_string() const noexcept;

 private:
  future<> parse_existing_segments(file dir);
  future<> recovery_compaction();
  future<> update_index(const protocol::update& up, index::entry e);
  future<> rolling();
  future<> gc_service(std::vector<uint64_t>& segs, bool& open);
  future<> compaction(protocol::group_id id);
  void reference_segments(std::span<const index::entry> indexes);
  future<> unreference_segments(std::span<const index::entry> indexes);

  // segments dir, e.g. <data_dir>
  std::string _log_dir;
  // bootstrap dir
  std::string _boot_dir;
  // group_id -> shard_id
  std::function<unsigned(uint64_t)> _partitioner;

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
  // the underlying write operations must be serial
  shared_mutex _mtx;

  struct stats _stats;
};

}  // namespace rafter::storage
