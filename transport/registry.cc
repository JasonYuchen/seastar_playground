//
// Created by jason on 2021/12/28.
//

#include "registry.hh"

#include <charconv>

namespace rafter::transport {

bool operator==(const peer_address& x, const peer_address& y) noexcept {
  return x.address == y.address && x.port == y.port;
}

bool operator<(const peer_address& x, const peer_address& y) noexcept {
  auto xsv = peer_address::to_string_view(x.address);
  auto ysv = peer_address::to_string_view(y.address);
  return (xsv < ysv) || (xsv == ysv && x.port < y.port);
}

peer_address::peer_address(std::string_view url) {
  auto pos = url.find_last_of(':');
  address = net::inet_address(std::string(url.substr(0, pos)));
  auto err = std::from_chars(url.data() + pos + 1, url.end(), port);
  assert(err.ec == std::errc());
  // TODO(jyc): eventually we want to use a special gateway to exchange
  //  knowledge of each nodehost, default to 0 for now
  cpu_id = 0;
}

std::string_view peer_address::to_string_view(
    const seastar::net::inet_address& address) {
  return {reinterpret_cast<const char*>(address.data()), address.size()};
}

std::ostream& operator<<(std::ostream& os, const peer_address& x) {
  return os << x.address << ':' << x.port << ':' << x.cpu_id;
}

std::optional<peer_address> registry::resolve(protocol::group_id gid) {
  auto it = _peer.find(gid);
  if (it == _peer.end()) {
    return std::optional<peer_address>{};
  }
  return std::optional<peer_address>{it->second};
}

void registry::update(protocol::group_id gid, std::string_view url) {
  update(gid, peer_address{url});
}

void registry::update(protocol::group_id gid, peer_address address) {
  _peer.insert({gid, address});
}

void registry::remove(protocol::group_id gid) { _peer.erase(gid); }

void registry::remove_cluster(uint64_t cluster_id) {
  std::erase_if(_peer, [cluster_id](const auto& p) {
    return p.first.cluster == cluster_id;
  });
}

}  // namespace rafter::transport
