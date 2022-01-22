//
// Created by jason on 2022/1/12.
//

#pragma once

#include <seastar/rpc/rpc.hh>

#include "protocol/raft.hh"
#include "util/worker.hh"

namespace rafter::transport {

class exchanger;

class express : seastar::enable_lw_shared_from_this<express> {
 public:
  express(
      exchanger& exc,
      protocol::group_id local,
      protocol::group_id target,
      seastar::rpc::sink<protocol::snapshot_chunk_ptr> sink)
    : _exchanger(exc)
    , _local(local)
    , _target(target)
    , _open(true)
    , _sink(std::move(sink))
    , _worker(fmt::format("express_{}", _target), 10) {}
  protocol::group_id target() const noexcept { return _target; }
  seastar::future<> start();
  seastar::future<> close();
  seastar::future<> send(protocol::snapshot_ptr snapshot);

 private:
  seastar::future<> split(
      protocol::snapshot_ptr snapshot,
      uint64_t& chunk_id,
      uint64_t total_chunks,
      protocol::snapshot_file_ptr file);
  seastar::future<> split(protocol::snapshot_ptr snapshot);
  seastar::future<> main(
      std::vector<protocol::snapshot_chunk_ptr>& chunks, bool& open);

  exchanger& _exchanger;
  protocol::group_id _local;
  protocol::group_id _target;
  bool _open;
  seastar::rpc::sink<protocol::snapshot_chunk_ptr> _sink;
  util::worker<protocol::snapshot_chunk_ptr> _worker;
  std::optional<seastar::future<>> _split_task;
};

using express_ptr = seastar::lw_shared_ptr<express>;

}  // namespace rafter::transport
