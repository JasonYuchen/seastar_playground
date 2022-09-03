//
// Created by jason on 2022/1/12.
//

#pragma once

#include <seastar/rpc/rpc.hh>
#include <unordered_map>

#include "protocol/raft.hh"
#include "util/seastarx.hh"
#include "util/util.hh"

namespace rafter::transport {

class exchanger;

class express {
 public:
  struct pair {
    uint64_t cluster = protocol::group_id::INVALID_CLUSTER;
    uint64_t from = protocol::group_id::INVALID_NODE;
    uint64_t to = protocol::group_id::INVALID_NODE;
    std::strong_ordering operator<=>(const pair&) const = default;
  };
  explicit express(exchanger& exc) : _exchanger(exc) {}
  future<> stop();

  future<> send(protocol::message message);
  future<> receive(
      pair key,
      protocol::log_id lid,
      seastar::rpc::source<protocol::snapshot_chunk> source);

 private:
  struct sender : public enable_lw_shared_from_this<sender> {
    sender(exchanger& e, pair p) : _exchanger(e), _pair(p) {}
    future<> start(protocol::snapshot_ptr snapshot);
    future<> stop();
    future<> split_and_send(
        protocol::snapshot_ptr snapshot,
        protocol::snapshot_file_ptr file,
        uint64_t total_chunks,
        uint64_t& chunk_id,
        seastar::rpc::sink<protocol::snapshot_chunk>& sink) const;

    exchanger& _exchanger;
    pair _pair;
    bool _close = false;
    std::optional<future<>> _task;
  };
  using sender_ptr = lw_shared_ptr<sender>;
  struct receiver : public enable_lw_shared_from_this<receiver> {
    receiver(exchanger& e, pair p) : _exchanger(e), _pair(p) {}
    future<> start(
        protocol::log_id lid,
        seastar::rpc::source<protocol::snapshot_chunk> source);
    future<> stop();

    exchanger& _exchanger;
    pair _pair;
    bool _close = false;
    std::optional<future<>> _task;
  };
  using receiver_ptr = lw_shared_ptr<receiver>;

  exchanger& _exchanger;
  std::unordered_map<pair, sender_ptr, util::tri_hasher> _senders;
  std::unordered_map<pair, receiver_ptr, util::tri_hasher> _receivers;
};

using express_ptr = lw_shared_ptr<express>;

}  // namespace rafter::transport
