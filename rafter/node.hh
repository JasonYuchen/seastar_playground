//
// Created by jason on 2022/3/15.
//

#pragma once

#include "core/peer.hh"
#include "core/quiesce.hh"
#include "core/raft_log.hh"
#include "protocol/rsm.hh"
#include "rafter/config.hh"
#include "rafter/info.hh"
#include "rafter/request.hh"
#include "rsm/statemachine_manager.hh"
#include "storage/logdb.hh"
#include "transport/registry.hh"
#include "util/buffering_queue.hh"
#include "util/seastarx.hh"

namespace rafter {

class engine;

class node {
 public:
  protocol::group_id id() const {
    return {_config.cluster_id, _config.node_id};
  }
  future<bool> start(
      raft_config cfg,
      const std::map<uint64_t, std::string>& peers,
      bool initial);
  future<> stop();
  future<protocol::update> step();

  future<request_result> propose_session(
      const protocol::session& s, uint64_t timeout);
  future<request_result> propose(
      const protocol::session& s, std::string_view cmd, uint64_t timeout);
  future<request_result> read(uint64_t timeout);
  future<request_result> request_snapshot(
      const snapshot_option& option, uint64_t timeout);
  future<request_result> request_config_change(
      protocol::config_change cc, uint64_t timeout);
  future<request_result> request_leader_transfer(uint64_t target_id);

  future<> apply_update(
      const protocol::log_entry& e,
      protocol::rsm_result result,
      bool rejected,
      bool ignored,
      bool notify_read);
  future<> apply_config_change(
      protocol::config_change cc, uint64_t key, bool rejected);
  future<> restore_remotes(protocol::snapshot_ptr ss);

 private:
  future<bool> replay_log();

  future<bool> handle_events();
  future<bool> handle_read_index();
  future<bool> handle_received_messages();
  future<bool> handle_config_change();
  future<bool> handle_proposals();
  future<bool> handle_leader_transfer();
  future<bool> handle_snapshot(uint64_t last_applied);
  future<bool> handle_compaction();

  void gc();
  future<> tick(uint64_t tick);
  void record_message(const protocol::message& m);
  uint64_t update_applied_index();

  raft_config _config;
  bool _stopped = true;
  bool _rate_limited = 0;
  cluster_info _cluster_info;
  engine& _engine;
  transport::registry& _node_registry;
  storage::logdb& _logdb;
  core::log_reader& _log_reader;
  pending_proposal _pending_proposal;
  pending_read_index _pending_read_index;
  pending_snapshot _pending_snapshot;
  pending_config_change _pending_config_change;
  pending_leader_transfer _pending_leader_transfer;
  util::buffering_queue<protocol::message> _received_messages;
  core::quiesce _quiesce;
  std::unique_ptr<core::peer> _peer;
  std::unique_ptr<rsm::statemachine_manager> _sm;
  protocol::snapshot_state _snapshot_state;
  uint64_t _current_tick = 0;
  uint64_t _gc_tick = 0;
  uint64_t _applied_index = 0;
  uint64_t _pushed_index = 0;
  uint64_t _confirmed_index = 0;
};

}  // namespace rafter
