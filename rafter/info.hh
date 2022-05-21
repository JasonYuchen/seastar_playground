//
// Created by jason on 2022/3/15.
//

#pragma once

#include "protocol/raft.hh"

namespace rafter {

struct cluster_info {
  // group_id is the group id (cluster id, node id) of the Raft cluster node
  protocol::group_id group_id;
  // nodes is a map of member node IDs to their Raft addresses.
  protocol::member_map nodes;
  // config_change_index is Raft log index of the last applied membership change
  // entry.
  uint64_t config_change_index = 0;
  // type is the type of the state machine.
  protocol::state_machine_type type = protocol::state_machine_type::regular;
  // role indicates this node's role in the Raft cluster.
  protocol::raft_role role = protocol::raft_role::num_of_role;
};

struct nodehost_info {
  // nodehost_id is the unique identifier of the nodehost instance.
  std::string nodehost_id;
  // address is the public address of the nodehost used for exchanging Raft
  // messages, snapshots and other metadata with other nodehost instances.
  std::string address;
  // cluster_infos is a list of all Raft clusters managed by the nodehost.
  std::vector<cluster_info> cluster_infos;
};

}  // namespace rafter
