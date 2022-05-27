//
// Created by jason on 2021/12/28.
//

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/net/inet_address.hh>
#include <unordered_map>

#include "protocol/raft.hh"
#include "util/seastarx.hh"
#include "util/util.hh"

namespace rafter::transport {

struct peer_address {
  net::inet_address address;
  uint16_t port = 0;
  uint32_t cpu_id = 0;
  friend bool operator==(const peer_address& x, const peer_address& y) noexcept;
  friend bool operator<(const peer_address& x, const peer_address& y) noexcept;
  friend std::ostream& operator<<(std::ostream& os, const peer_address& x);
  struct hash {
    size_t operator()(const peer_address& id) const noexcept {
      return std::hash<std::string_view>()(to_string_view(id.address));
    }
  };
  // url should be validated by caller <name>:<port>, <cpu> is default to 0
  explicit peer_address(std::string_view url);
  peer_address(net::inet_address ip, uint16_t port, uint32_t cpu)
    : address(ip), port(port), cpu_id(cpu) {}

  static std::string_view to_string_view(
      const seastar::net::inet_address& address);
};

class registry
  : public async_sharded_service<registry>
  , public peering_sharded_service<registry> {
 public:
  // unengaged return means not found
  std::optional<peer_address> resolve(protocol::group_id gid);

  void update(protocol::group_id gid, std::string_view url);

  void update(protocol::group_id gid, peer_address address);

  void remove(protocol::group_id gid);

  void remove_cluster(uint64_t cluster_id);

  future<> start() { return make_ready_future<>(); }
  future<> stop() { return make_ready_future<>(); }

 private:
  std::unordered_map<protocol::group_id, peer_address, util::pair_hasher> _peer;
};

}  // namespace rafter::transport
