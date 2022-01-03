//
// Created by jason on 2021/12/16.
//

#pragma once

#include <seastar/net/inet_address.hh>
#include <seastar/rpc/rpc.hh>

#include "protocol/serializer.hh"
#include "rafter/config.hh"

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

struct shard_address {
  seastar::net::inet_address address;
  uint32_t cpu_id;
  friend bool operator==(
      const shard_address& x, const shard_address& y) noexcept {
    return x.address == y.address;
  }
  friend bool operator<(
      const shard_address& x, const shard_address& y) noexcept {
    return to_string_view(x.address) < to_string_view(y.address);
  }
  friend std::ostream& operator<<(std::ostream& os, const shard_address& x) {
    return os << x.address << ':' << x.cpu_id;
  }
  struct hash {
    size_t operator()(const shard_address& id) const noexcept {
      return std::hash<std::string_view>()(to_string_view(id.address));
    }
  };
  shard_address(seastar::net::inet_address ip, uint32_t cpu) noexcept
    : address(ip), cpu_id(cpu) {}

  static std::string_view to_string_view(
      const seastar::net::inet_address& address) {
    return {reinterpret_cast<const char*>(address.data()), address.size()};
  }
};

class exchanger {
 public:
  explicit exchanger(const config& config) : _config(config) {}

  using rpc_protocol =
      seastar::rpc::protocol<protocol::serializer, messaging_verb>;
  using rpc_protocol_client =
      seastar::rpc::protocol<protocol::serializer, messaging_verb>::client;
  using rpc_protocol_server =
      seastar::rpc::protocol<protocol::serializer, messaging_verb>::server;

  struct shard_info {
    shard_info(seastar::shared_ptr<rpc_protocol_client>&& client)
      : rpc_client(std::move(client)) {}

    seastar::shared_ptr<rpc_protocol_client> rpc_client;
    seastar::rpc::stats stats() const { return rpc_client->get_stats(); }
  };
  seastar::future<> start_listen();
  seastar::future<> shutdown();
  seastar::future<> stop();

  void register_message(
      std::function<seastar::future<>(
          const seastar::rpc::client_info& info,
          seastar::rpc::source<protocol::message_ptr> source)>&& func) {
    _rpc->register_handler(messaging_verb::message, std::move(func));
  }

  seastar::future<> unregister_message() {
    return _rpc->unregister_handler(messaging_verb::message);
  }

  // TODO: use raft group_id to get address from registry
  seastar::future<seastar::rpc::sink<protocol::message_ptr>>
  make_sink_for_message(shard_address address) {
    // if shutting down
    return get_rpc_client(messaging_verb::message, address)
        ->make_stream_sink<protocol::serializer, protocol::message_ptr>();
  }

 private:
  seastar::future<> stop_server();
  seastar::future<> stop_client();

  seastar::shared_ptr<rpc_protocol_client> get_rpc_client(
      messaging_verb verb, shard_address address);

  const config& _config;
  uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::num_of_verb)];
  std::unique_ptr<rpc_protocol> _rpc;
  std::unordered_map<shard_address, shard_info, shard_address::hash> _clients;
};

}  // namespace rafter::transport
