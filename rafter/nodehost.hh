//
// Created by jason on 2021/12/8.
//

#pragma once

#include <memory>
#include <seastar/core/sharded.hh>
#include <unordered_map>

#include "rafter/config.hh"
#include "rafter/node.hh"
#include "rafter/request.hh"
#include "rafter/statemachine.hh"
#include "storage/logdb.hh"
#include "transport/registry.hh"
#include "transport/rpc.hh"
#include "util/seastarx.hh"
#include "util/worker.hh"

namespace rafter {

class engine;

// TODO(jyc): provide a detail doc
class nodehost
  : public async_sharded_service<nodehost>
  , public peering_sharded_service<nodehost> {
 public:
  nodehost(
      struct config cfg,
      engine& ng,
      storage::logdb& logdb,
      transport::registry& registry,
      transport::rpc& rpc);
  const config& config() const { return _config; }
  future<> start();
  future<> stop();

  future<> start_cluster(
      raft_config cfg,
      protocol::member_map initial_members,
      bool join,
      protocol::state_machine_type type,
      statemachine::factory&& factory);

  future<> stop_cluster(uint64_t cluster_id);

  future<> stop_node(protocol::group_id gid);

  // TODO(jyc): support timeout parameter
  future<protocol::membership> get_membership(uint64_t cluster_id);

  future<uint64_t> get_leader(uint64_t cluster_id);

  future<protocol::session> get_session(uint64_t cluster_id);

  future<protocol::session> get_noop_session(uint64_t cluster_id);

  future<> close_session(protocol::session& s);

  future<request_result> propose(protocol::session& s, std::string_view cmd);

  future<request_result> propose_session(protocol::session& s);

  future<request_result> read_index(uint64_t cluster_id);

  future<request_result> linearizable_read(
      uint64_t cluster_id, std::string_view query);

  future<request_result> stale_read(
      uint64_t cluster_id, std::string_view query);

  future<request_result> request_snapshot(
      uint64_t cluster_id, const snapshot_option& option);

  future<request_result> request_compaction(protocol::group_id gid);

  future<request_result> request_add_node(
      protocol::group_id gid, std::string target, uint64_t config_change_index);

  future<request_result> request_add_observer(
      protocol::group_id gid, std::string target, uint64_t config_change_index);

  future<request_result> request_add_witness(
      protocol::group_id gid, std::string target, uint64_t config_change_index);

  future<request_result> request_delete_node(
      protocol::group_id gid, uint64_t config_change_index);

  future<request_result> request_leader_transfer(protocol::group_id gid);

  future<request_result> request_remove_data(protocol::group_id gid);

  future<> get_nodehost_info(bool skip_log_info);

  // TODO(jyc): support reusable node interface to avoid repeatedly locate a
  //  specific node in the cluster map of nodehost

 private:
  void initialize_handlers();
  void uninitialize_handlers();

  future<std::pair<protocol::member_map, bool>> bootstrap_cluster(
      const raft_config& cfg,
      const protocol::member_map& initial_members,
      bool join,
      protocol::state_machine_type type);

  future<> handle_unreachable(protocol::group_id gid);

  future<> handle_message(protocol::message m);

  future<> handle_snapshot(protocol::group_id gid, uint64_t from);

  future<> handle_snapshot_status(protocol::group_id gid, bool failed);

  future<> send(protocol::message m);

  uint64_t _id = 0;
  struct config _config;
  bool _stopped = false;
  timer<> _ticker;
  engine& _engine;
  storage::logdb& _logdb;
  transport::registry& _registry;
  transport::rpc& _rpc;
  [[maybe_unused]] util::worker<protocol::message> _sender;
  [[maybe_unused]] util::worker<protocol::message> _receiver;
  std::function<unsigned(uint64_t)> _partitioner;
  uint64_t _cluster_change_index = 0;
  std::unordered_map<uint64_t, lw_shared_ptr<node>> _clusters;
  // TODO(jyc): event
  // TODO(jyc): request pool ?
};

}  // namespace rafter
