//
// Created by jason on 2022/5/16.
//

#pragma once

#include <seastar/core/future.hh>

#include "protocol/raft.hh"
#include "server/snapshot_context.hh"

namespace rafter::transport {

class snapshot_chunk_tracker {
 public:
  bool record(const protocol::snapshot_chunk& chunk);
  future<protocol::message> finalize(server::snapshot_context& ctx);
  bool have_all_chunks() const { return _expected_next_id == _first.count; }

 private:
  // snapshot validator
  protocol::snapshot_chunk _first;
  protocol::snapshot_files _files;
  uint64_t _expected_next_id = 0;
};

}  // namespace rafter::transport
