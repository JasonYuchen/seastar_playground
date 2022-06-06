//
// Created by jason on 2022/5/9.
//

#pragma once

#include "protocol/raft.hh"

namespace rafter::rsm {

class membership {
 public:
  membership(protocol::group_id gid, bool ordered)
    : _gid(gid), _ordered(ordered) {}
  void set(const protocol::membership& m) { _membership = m; }
  const protocol::membership& get() const { return _membership; }
  bool handle(const protocol::config_change& cc, uint64_t index);
  bool empty() const { return _membership.addresses.empty(); }

 private:
  protocol::group_id _gid;
  protocol::membership _membership;
  bool _ordered;
};

}  // namespace rafter::rsm
