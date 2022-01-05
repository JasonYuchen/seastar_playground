//
// Created by jason on 2021/12/16.
//

#include "exchanger.hh"

#include <chrono>
#include <seastar/core/coroutine.hh>

#include "transport/logger.hh"

namespace rafter::transport {

using namespace seastar;
using namespace std::chrono_literals;

exchanger::exchanger(const config& config)
  : _config(config)
  , _dropped_messages{0}
  , _rpc(std::make_unique<rpc_protocol>(protocol::serializer{})) {
  _rpc->set_logger(&l);
}

future<> exchanger::start_listen() {
  l.info("exchanger::start_listen: {}:{}",
         _config.listen_address,
         _config.listen_port);
  rpc::server_options opts;
  // TODO: compress, tcp_nodelay, encryption, connection filter, resource limit
  opts.load_balancing_algorithm = server_socket::load_balancing_algorithm::port;
  opts.streaming_domain = rpc::streaming_domain_type{0x0615};
  auto address = socket_address{net::inet_address{_config.listen_address}, _config.listen_port};
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

future<rpc::sink<protocol::message_ptr>> exchanger::make_sink_for_message(
    peer_address address) {
  // if shutting down
  auto client = get_rpc_client(messaging_verb::message, address);
  auto sink = co_await client->make_stream_sink<protocol::serializer, protocol::message_ptr>();
  // register streaming pipeline in the server
  auto rpc_handler = _rpc->make_client<void(rpc::sink<protocol::message_ptr>)>(messaging_verb::message);
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
  // TODO: compress, tcp_nodelay, encryption
  opts.tcp_nodelay = true;
  opts.reuseaddr = true;
  // TODO:
  auto client =
      make_shared<rpc_protocol_client>(*_rpc, std::move(opts), peer, local);
  auto ret = _clients.emplace(address, peer_info(std::move(client)));
  if (!ret.second) [[unlikely]] {
    // TODO:
  }
  return ret.first->second.rpc_client;
}

bool exchanger::remove_rpc_client(peer_address address) {
  // TODO: remove client and notify all relevant raft groups
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
