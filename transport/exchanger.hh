//
// Created by jason on 2021/12/16.
//

#pragma once

#include <seastar/rpc/rpc.hh>

#include "protocol/serializer.hh"
#include "transport/express.hh"
#include "transport/registry.hh"
#include "transport/rpc.hh"
#include "util/seastarx.hh"

namespace rafter::transport {

namespace srpc = seastar::rpc;

enum class messaging_verb : int32_t {
  // for exchanging various metadata about the node, e.g. shard number
  meta = 0,
  // for normal raft messages
  message = 1,
  // for transferring replicated state machine's snapshot
  snapshot = 2,
  // for recording dropped verbs
  num_of_verb
};

class exchanger final
  : public async_sharded_service<exchanger>
  , public peering_sharded_service<exchanger>
  , public rpc {
 public:
  explicit exchanger(registry& reg, protocol::snapshot_dir_func snapshot_dir);
  ~exchanger() override = default;

  using rpc_protocol = srpc::protocol<protocol::serializer, messaging_verb>;
  using rpc_protocol_client =
      srpc::protocol<protocol::serializer, messaging_verb>::client;
  using rpc_protocol_server =
      srpc::protocol<protocol::serializer, messaging_verb>::server;

  struct peer_info {
    explicit peer_info(shared_ptr<rpc_protocol_client>&& client)
      : rpc_client(std::move(client)) {}

    shared_ptr<rpc_protocol_client> rpc_client;
    srpc::stats stats() const { return rpc_client->get_stats(); }
  };

  future<> start_listen();
  future<> shutdown();
  future<> start() { return start_listen(); }
  future<> stop() { return shutdown(); }

  template <typename Ret, typename... Args>
  auto send(messaging_verb verb, peer_address address, Args&&... args) {
    auto self = shared_from_this();
    auto rpc_handler = _rpc->make_client<Ret(Args...)>(verb);
    auto client = get_rpc_client(verb, address);
    try {
      return rpc_handler(*client, std::forward<Args>(args)...);
    } catch (srpc::closed_error& ex) {
      remove_rpc_client(address);
      // TODO(jyc): increase the counter and notify the peer is unreachable
    }
    return make_ready_future<>();
  }

  future<> send_message(protocol::message m) override;

  future<> send_snapshot(protocol::message m) override;

  void register_unreachable_handler(unreachable_handler&& func) override {
    _notify_unreachable = std::move(func);
  }
  future<> notify_unreachable(protocol::group_id gid) {
    assert(_notify_unreachable);
    // remove the client since the peer is unreachable
    auto address = _registry.resolve(gid);
    if (address) {
      remove_rpc_client(*address);
    }
    return _notify_unreachable(gid);
  }

  void register_message_handler(message_handler&& func) override {
    _notify_message = std::move(func);
  }
  future<> notify_message(protocol::message m) {
    assert(_notify_message);
    return _notify_message(std::move(m));
  }

  void register_snapshot_handler(snapshot_handler&& func) override {
    _notify_snapshot = std::move(func);
  }
  future<> notify_snapshot(protocol::group_id gid, uint64_t from) {
    assert(_notify_snapshot);
    return _notify_snapshot(gid, from);
  }

  void register_snapshot_status_handler(
      snapshot_status_handler&& func) override {
    _notify_snapshot_status = std::move(func);
  }
  future<> notify_snapshot_status(protocol::group_id gid, bool failed) {
    assert(_notify_snapshot_status);
    return _notify_snapshot_status(gid, failed);
  }

  void register_message(
      std::function<srpc::no_wait_type(
          const srpc::client_info& info, protocol::message m)>&& func) {
    _rpc->register_handler(messaging_verb::message, std::move(func));
  }

  future<> unregister_message() {
    return _rpc->unregister_handler(messaging_verb::message);
  }

  void register_snapshot_chunk(
      std::function<future<>(
          const srpc::client_info& info,
          uint64_t cluster,
          uint64_t from,
          uint64_t to,
          protocol::log_id,
          srpc::source<protocol::snapshot_chunk> source)>&& func) {
    _rpc->register_handler(messaging_verb::snapshot, std::move(func));
  }

  future<> unregister_snapshot_chunk() {
    return _rpc->unregister_handler(messaging_verb::snapshot);
  }

  // TODO(jyc): use raft group_id to get address from registry
  future<srpc::sink<protocol::snapshot_chunk>> make_sink_for_snapshot_chunk(
      uint64_t cluster_id, uint64_t from, uint64_t to, protocol::log_id lid);

  std::string get_snapshot_dir(protocol::group_id gid) const {
    return _snapshot_dir(gid);
  }

  void finalize_handlers();

 private:
  // TODO(jyc): split normal client and streaming client
  shared_ptr<rpc_protocol_client> get_rpc_client(
      messaging_verb verb, peer_address address);

  bool remove_rpc_client(peer_address address);

  registry& _registry;
  express _express;
  bool _shutting_down = false;
  uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::num_of_verb)];
  std::unique_ptr<rpc_protocol> _rpc;
  std::unique_ptr<rpc_protocol_server> _server;
  std::unordered_map<peer_address, peer_info, peer_address::hash> _clients;
  protocol::snapshot_dir_func _snapshot_dir;
  unreachable_handler _notify_unreachable;
  message_handler _notify_message;
  snapshot_handler _notify_snapshot;
  snapshot_status_handler _notify_snapshot_status;
};

}  // namespace rafter::transport
