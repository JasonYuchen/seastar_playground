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
  void apply(const protocol::config_change& cc, uint64_t index);
  bool empty() const { return _membership.addresses.empty(); }
  bool is_up_to_date(const protocol::config_change& cc) const;
  bool is_adding_removed_node(const protocol::config_change& cc) const;
  bool is_promoting_observer(const protocol::config_change& cc) const;
  bool is_invalid_observer_promotion(const protocol::config_change& cc) const;
  bool is_adding_existing_node(const protocol::config_change& cc) const;
  bool is_adding_node_as_observer(const protocol::config_change& cc) const;
  bool is_adding_node_as_witness(const protocol::config_change& cc) const;
  bool is_adding_witness_as_observer(const protocol::config_change& cc) const;
  bool is_adding_witness_as_node(const protocol::config_change& cc) const;
  bool is_adding_observer_as_witness(const protocol::config_change& cc) const;
  bool is_deleting_last_node(const protocol::config_change& cc) const;

 private:
  protocol::group_id _gid;
  protocol::membership _membership;
  bool _ordered;
};

}  // namespace rafter::rsm
