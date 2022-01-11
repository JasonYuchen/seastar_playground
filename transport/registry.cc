//
// Created by jason on 2021/12/28.
//

#include "registry.hh"

namespace rafter::transport {

bool operator==(const peer_address& x, const peer_address& y) noexcept {
  return x.address == y.address && x.port == y.port;
}

bool operator<(const peer_address& x, const peer_address& y) noexcept {
  auto xsv = peer_address::to_string_view(x.address);
  auto ysv = peer_address::to_string_view(y.address);
  return (xsv < ysv) || (xsv == ysv && x.port < y.port);
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
  return it->second;
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
