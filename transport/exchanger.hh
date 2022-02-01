//
// Created by jason on 2021/12/16.
//

#pragma once

#include <seastar/rpc/rpc.hh>

#include "protocol/serializer.hh"
#include "rafter/config.hh"
#include "transport/express.hh"
#include "transport/registry.hh"

namespace rafter::transport {

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

class exchanger
  : public seastar::async_sharded_service<exchanger>
  , public seastar::peering_sharded_service<exchanger> {
 public:
  explicit exchanger(const config& config, registry& reg);
  const config& config() const noexcept { return _config; }

  using rpc_protocol =
      seastar::rpc::protocol<protocol::serializer, messaging_verb>;
  using rpc_protocol_client =
      seastar::rpc::protocol<protocol::serializer, messaging_verb>::client;
  using rpc_protocol_server =
      seastar::rpc::protocol<protocol::serializer, messaging_verb>::server;

  struct peer_info {
    explicit peer_info(seastar::shared_ptr<rpc_protocol_client>&& client)
      : rpc_client(std::move(client)) {}

    seastar::shared_ptr<rpc_protocol_client> rpc_client;
    seastar::rpc::stats stats() const { return rpc_client->get_stats(); }
  };
  // TODO(jyc): initialize all handlers
  seastar::future<> start_listen();
  seastar::future<> shutdown();
  seastar::future<> stop() { return seastar::make_ready_future<>(); }

  template <typename Ret, typename... Args>
  auto send(messaging_verb verb, peer_address address, Args&&... args) {
    auto self = shared_from_this();
    auto rpc_handler = _rpc->make_client<Ret(Args...)>(verb);
    // TODO(jyc): if shutting down
    auto client = get_rpc_client(verb, address);
    try {
      return rpc_handler(*client, std::forward<Args>(args)...);
    } catch (seastar::rpc::closed_error& ex) {
      remove_rpc_client(address);
      // TODO(jyc): increase the counter and notify the peer is unreachable
    }
    return seastar::make_ready_future<>();
  }

  void register_message(
      std::function<seastar::rpc::no_wait_type(
          const seastar::rpc::client_info& info, protocol::message m)>&& func) {
    _rpc->register_handler(messaging_verb::message, std::move(func));
  }

  seastar::future<> unregister_message() {
    return _rpc->unregister_handler(messaging_verb::message);
  }

  seastar::future<> send_message(protocol::message m);
  seastar::future<> send_snapshot(protocol::message m);

  void register_snapshot_chunk(
      std::function<seastar::future<>(
          const seastar::rpc::client_info& info,
          uint64_t cluster,
          uint64_t from,
          uint64_t to,
          seastar::rpc::source<protocol::snapshot_chunk_ptr> source)>&& func) {
    _rpc->register_handler(messaging_verb::snapshot, std::move(func));
  }

  seastar::future<> unregister_snapshot_chunk() {
    return _rpc->unregister_handler(messaging_verb::snapshot);
  }

  // TODO(jyc): use raft group_id to get address from registry
  seastar::future<seastar::rpc::sink<protocol::snapshot_chunk_ptr>>
  make_sink_for_snapshot_chunk(uint64_t cluster_id, uint64_t from, uint64_t to);

  seastar::future<> notify_unreachable(protocol::group_id target);
  seastar::future<> notify_successful(protocol::group_id target);

 private:
  // TODO(jyc): split normal client and streaming client
  seastar::shared_ptr<rpc_protocol_client> get_rpc_client(
      messaging_verb verb, peer_address address);

  bool remove_rpc_client(peer_address address);

  const struct config& _config;
  registry& _registry;
  express _express;
  bool _shutting_down = false;
  uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::num_of_verb)];
  std::unique_ptr<rpc_protocol> _rpc;
  std::unique_ptr<rpc_protocol_server> _server;
  std::unordered_map<peer_address, peer_info, peer_address::hash> _clients;
};

}  // namespace rafter::transport
