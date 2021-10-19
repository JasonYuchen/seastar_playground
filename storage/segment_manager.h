//
// Created by jiancheng.cai on 2021/10/13.
//

#pragma once

#include <seastar/core/queue.hh>

#include "index.hh"
#include "segment.hh"

namespace rafter::storage {

// sharded<segment_manager>
class segment_manager {
 public:
  // TODO: storage configuration class
  segment_manager(protocol::group_id id, std::filesystem::path data_dir);

  // TODO: implement logdb
  seastar::future<bool> append(const protocol::update& update);

 private:
  void ensure_enough_space();
  seastar::future<> segement_gc_service();

 private:
  const protocol::group_id _group_id;
  bool _open = false;
  // segments dir, e.g. <data_dir>
  std::filesystem::path _path;
  // TODO: more options
  const uint64_t _rolling_size = 1024 * 1024 * 1024;
  // used to allocate new segments
  // the format of a segment name is <shard_id:05d>_<segment_id:020d>
  uint64_t _next_segment_id = 0;
  uint64_t _minimal_used_id = 0;  // the minimal id of in-use segments
  std::vector<std::unique_ptr<segment>> _segments;
  index_group _index_group;
  seastar::queue<uint64_t> _obsolete_queue;  // segment ids ready for GC
};

}  // namespace rafter::storage
