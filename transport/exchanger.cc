//
// Created by jason on 2021/12/16.
//

#include "exchanger.hh"

#include <chrono>
#include <seastar/core/coroutine.hh>

#include "transport/logger.hh"
#include "util/error.hh"

namespace rafter::transport {

using namespace protocol;
using namespace seastar;
using namespace std::chrono_literals;

exchanger::exchanger(const config& config, registry& reg)
  : _config(config)
  , _registry(reg)
  , _dropped_messages{0}
  , _rpc(std::make_unique<rpc_protocol>(serializer{})) {
  _rpc->set_logger(&l);
}

future<> exchanger::start_listen() {
  l.info(
      "exchanger::start_listen: {}:{}",
      _config.listen_address,
      _config.listen_port);
  rpc::server_options opts;
  // TODO(jyc): compress, tcp_no_delay, encryption, resource limit, etc
  opts.load_balancing_algorithm = server_socket::load_balancing_algorithm::port;
  opts.streaming_domain = rpc::streaming_domain_type{0x0615};
  auto address = socket_address{
      net::inet_address{_config.listen_address}, _config.listen_port};
  _server =
      std::make_unique<rpc_protocol_server>(*_rpc, std::move(opts), address);
  return make_ready_future<>();
}

future<> exchanger::shutdown() {
  if (_shutting_down) {
    co_return;
  }
  _shutting_down = true;
  if (_server) {
    co_await _server->stop();
  }
  co_await parallel_for_each(_clients, [](auto& peer) -> future<> {
    l.info("exchanger::shutdown: closing connection to {}", peer.first);
    co_await peer.second.rpc_client->stop();
  });
  l.info("exchanger::shutdown: done");
}

future<> exchanger::send_message(message_ptr msg) {
  auto gid = group_id{.cluster = msg->cluster, .node = msg->to};
  auto address = _registry.resolve(gid);
  if (!address) {
    // TODO(jyc): just drop the message should be fine but remember to notify
    //  the peer is unreachable
    _dropped_messages[static_cast<int32_t>(messaging_verb::message)]++;
    throw util::peer_not_found_error(gid);
  }
  return send<rpc::no_wait_type>(
      messaging_verb::message, *address, std::move(msg));
}

future<rpc::sink<snapshot_chunk_ptr>> exchanger::make_sink_for_snapshot_chunk(
    group_id gid) {
  // if shutting down
  auto address = _registry.resolve(gid);
  if (!address) {
    // TODO(jyc): group unreachable
    co_return coroutine::make_exception(util::peer_not_found_error(gid));
  }
  auto client = get_rpc_client(messaging_verb::message, *address);
  auto sink =
      co_await client->make_stream_sink<serializer, snapshot_chunk_ptr>();
  // register streaming pipeline in the server
  auto rpc_handler = _rpc->make_client<void(rpc::sink<snapshot_chunk_ptr>)>(
      messaging_verb::snapshot);
  co_await rpc_handler(*client, sink);
  co_return sink;
}

shared_ptr<exchanger::rpc_protocol_client> exchanger::get_rpc_client(
    messaging_verb, peer_address address) {
  auto it = _clients.find(address);
  if (it != _clients.end()) {
    auto client = it->second.rpc_client;
    if (!client->error()) {
      return client;
    }
    remove_rpc_client(address);
  }

  auto peer = socket_address(address.address, address.port);
  auto local = socket_address(net::inet_address{_config.listen_address}, 0);
  rpc::client_options opts;
  opts.keepalive = {60s, 60s, 10};
  // TODO(jyc): compress, tcp_no_delay, encryption
  opts.tcp_nodelay = true;
  opts.reuseaddr = true;
  auto client =
      make_shared<rpc_protocol_client>(*_rpc, std::move(opts), peer, local);
  auto ret = _clients.emplace(address, peer_info(std::move(client)));
  if (!ret.second) [[unlikely]] {
    // TODO(jyc): insert fail?
  }
  return ret.first->second.rpc_client;
}

bool exchanger::remove_rpc_client(peer_address address) {
  // TODO(jyc): remove client and notify all relevant raft groups
  if (_shutting_down) {
    return false;
  }
  auto it = _clients.find(address);
  if (it == _clients.end()) {
    return false;
  }
  auto client = std::move(it->second.rpc_client);
  _clients.erase(it);
  (void)client->stop()
      .finally([address, client, exc = shared_from_this()] {
        l.debug(
            "exchanger::remove_rpc_client: dropped connection to {}", address);
      })
      .discard_result();
  return true;
}

}  // namespace rafter::transport
