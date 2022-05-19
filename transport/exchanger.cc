//
// Created by jason on 2021/12/16.
//

#include "exchanger.hh"

#include <chrono>
#include <seastar/core/coroutine.hh>

#include "rafter/config.hh"
#include "transport/logger.hh"
#include "util/error.hh"

namespace rafter::transport {

using namespace protocol;
using namespace std::chrono_literals;

exchanger::exchanger(registry& reg, snapshot_dir_func snapshot_dir)
  : _registry(reg)
  , _express(*this)
  , _dropped_messages{0}
  , _rpc(std::make_unique<rpc_protocol>(serializer{}))
  , _snapshot_dir(std::move(snapshot_dir)) {
  _rpc->set_logger(&l);
}

future<> exchanger::start_listen() {
  finalize_handlers();
  l.info(
      "exchanger::start_listen: {}:{}",
      config::shard().listen_address,
      config::shard().listen_port);
  srpc::server_options opts;
  // TODO(jyc): compress, tcp_no_delay, encryption, resource limit, etc
  opts.load_balancing_algorithm = server_socket::load_balancing_algorithm::port;
  opts.streaming_domain = srpc::streaming_domain_type{0x0615};
  auto address = socket_address{
      net::inet_address{config::shard().listen_address},
      config::shard().listen_port};
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
  co_await _express.stop();
  l.info("exchanger::shutdown: done");
}

future<> exchanger::send_message(message m) {
  auto gid = group_id{.cluster = m.cluster, .node = m.to};
  auto address = _registry.resolve(gid);
  if (!address) {
    // TODO(jyc): just drop the message should be fine but remember to notify
    //  the peer is unreachable
    _dropped_messages[static_cast<int32_t>(messaging_verb::message)]++;
    throw util::peer_not_found_error(gid);
  }
  return send<srpc::no_wait_type>(
      messaging_verb::message, *address, std::move(m));
}

future<> exchanger::send_snapshot(message m) {
  if (!m.snapshot) {
    throw util::invalid_argument("snapshot", "empty snapshot in message");
  }
  return _express.send(std::move(m));
}

future<srpc::sink<snapshot_chunk>> exchanger::make_sink_for_snapshot_chunk(
    uint64_t cluster_id, uint64_t from, uint64_t to, log_id lid) {
  // if shutting down
  group_id remote = {.cluster = cluster_id, .node = to};
  auto address = _registry.resolve(remote);
  if (!address) {
    // TODO(jyc): group unreachable
    co_return coroutine::make_exception(util::peer_not_found_error(remote));
  }
  auto client = get_rpc_client(messaging_verb::message, *address);
  auto sink = co_await client->make_stream_sink<serializer, snapshot_chunk>();
  // register streaming pipeline in the server
  auto rpc_handler = _rpc->make_client<void(
      uint64_t, uint64_t, uint64_t, log_id, srpc::sink<snapshot_chunk>)>(
      messaging_verb::snapshot);
  co_await rpc_handler(*client, cluster_id, from, to, lid, sink);
  co_return std::move(sink);
}

void exchanger::finalize_handlers() {
  if (!_notify_unreachable) {
    throw util::panic("unreachable handler not set");
  }
  if (!_notify_message) {
    throw util::panic("message handler not set");
  }
  if (!_notify_snapshot) {
    throw util::panic("snapshot handler not set");
  }
  if (!_notify_snapshot_status) {
    throw util::panic("snapshot status handler not set");
  }
  if (!_rpc->has_handler(messaging_verb::message)) {
    // handler maybe set during test
    _rpc->register_handler(
        messaging_verb::message,
        [this](const srpc::client_info& info, message m) {
          // TODO(jyc): use this info to update registry
          // FIXME: will not wait for result, e.g. the message may be dropped by
          //  the handler due to heavy load, if we wait for the consuming result
          //  here, the upstream node may be blocked by this node, raft layer
          //  has its own mechanism to handle such situation
          (void)_notify_message(std::move(m));
          return srpc::no_wait;
        });
  }

  if (!_rpc->has_handler(messaging_verb::snapshot)) {
    // handler maybe set during test
    _rpc->register_handler(
        messaging_verb::snapshot,
        [this](
            const srpc::client_info& info,
            uint64_t cluster,
            uint64_t from,
            uint64_t to,
            log_id lid,
            srpc::source<snapshot_chunk> source) {
          // TODO(jyc): use this info to update registry
          return _express.receive({cluster, from, to}, lid, std::move(source));
        });
  }
}

shared_ptr<exchanger::rpc_protocol_client> exchanger::get_rpc_client(
    messaging_verb, peer_address address) {
  // TODO(jyc): use group_id to cache a rpc_protocol_client and avoid an extra
  //  group_id -> peer_address look up
  auto it = _clients.find(address);
  if (it != _clients.end()) {
    auto client = it->second.rpc_client;
    if (!client->error()) {
      return client;
    }
    remove_rpc_client(address);
  }

  auto peer = socket_address(address.address, address.port);
  auto local =
      socket_address(net::inet_address{config::shard().listen_address}, 0);
  srpc::client_options opts;
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
  (void)client->stop().finally([address, client, exc = shared_from_this()] {
    l.debug("exchanger::remove_rpc_client: dropped connection to {}", address);
  });
  return true;
}

}  // namespace rafter::transport
