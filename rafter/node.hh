//
// Created by jason on 2022/3/15.
//

#pragma once

#include "core/peer.hh"
#include "core/quiesce.hh"
#include "core/raft_log.hh"
#include "protocol/raft.hh"
#include "rafter/config.hh"
#include "rafter/info.hh"
#include "rafter/request.hh"
#include "rsm/statemachine_manager.hh"
#include "storage/logdb.hh"
#include "transport/registry.hh"
#include "util/buffering_queue.hh"
#include "util/seastarx.hh"

namespace rafter {

class nodehost;

class node {
 public:
  node(
      raft_config cfg,
      nodehost& host,
      transport::registry& registry,
      storage::logdb& logdb,
      std::unique_ptr<rsm::snapshotter> snapshotter,
      statemachine::factory&& sm_factory,
      std::function<future<>(protocol::message)>&& sender,
      std::function<future<>(protocol::group_id, bool)>&& snapshot_notifier);
  protocol::group_id id() const noexcept {
    return {_config.cluster_id, _config.node_id};
  }
  bool is_witness() const noexcept { return _config.witness; }

  future<> start(const protocol::member_map& peers, bool initial);
  future<> stop();
  future<std::optional<protocol::update>> step();

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

  future<> apply_entry(
      const protocol::log_entry& e,
      protocol::rsm_result result,
      bool rejected,
      bool ignored,
      bool notify_read);
  future<> apply_config_change(
      protocol::config_change cc, uint64_t key, bool rejected);
  future<> apply_snapshot(
      uint64_t key, bool ignored, bool aborted, uint64_t index);
  future<> restore_remotes(protocol::snapshot_ptr ss);
  future<> apply_raft_update(protocol::update& up);
  future<> commit_raft_update(const protocol::update& up);
  // This method will access underlying logdb, which is not multi-coroutine safe
  future<> process_raft_update(protocol::update& up);
  future<> process_dropped_entries(const protocol::update& up);
  future<> process_dropped_read_indexes(const protocol::update& up);
  future<> process_ready_to_read(const protocol::update& up);
  future<> process_snapshot(const protocol::update& up);

  bool is_busy_snapshotting() const;
  future<> handle_snapshot_task(protocol::rsm_task task);
  bool process_status_transition();

 private:
  // TODO(jyc): refine the interface and avoid friend
  friend class nodehost;
  friend class rsm::statemachine_manager;

  future<bool> replay_log();
  future<> remove_log();
  future<> compact_log(const protocol::rsm_task& task, uint64_t index);

  future<bool> handle_events();
  future<bool> handle_read_index();
  future<bool> handle_received_messages();
  future<bool> handle_config_change();
  future<bool> handle_proposals();
  future<bool> handle_leader_transfer();
  future<bool> handle_snapshot(uint64_t last_applied);
  future<bool> handle_compaction();

  bool process_save_status();
  bool process_stream_status();
  bool process_recover_status();
  bool process_unintialized_status();

  future<> send_enter_quiesce_messages();
  // only send replicate messages
  future<> send_replicate_messages(protocol::message_vector& msgs);
  // only send non-replicate messages
  future<> send_messages(protocol::message_vector& msgs);

  void node_ready();
  void gc();
  future<> tick();
  void record_message(const protocol::message& m);
  uint64_t update_applied_index();
  bool save_snapshot_required(uint64_t applied);
  future<std::optional<protocol::update>> get_update();
  void report_ignored_snapshot_request(uint64_t key);
  void report_save_snapshot(protocol::rsm_task task);
  void report_stream_snapshot(protocol::rsm_task task);
  void report_recover_snapshot(protocol::rsm_task task);

  raft_config _config;
  bool _stopped = true;
  bool _initialized = false;
  bool _new_node = false;
  bool _rate_limited = false;
  cluster_info _cluster_info;
  nodehost& _nodehost;
  transport::registry& _node_registry;
  storage::logdb& _logdb;
  core::log_reader _log_reader;
  pending_proposal _pending_proposal;
  pending_read_index _pending_read_index;
  pending_snapshot _pending_snapshot;
  pending_config_change _pending_config_change;
  pending_leader_transfer _pending_leader_transfer;
  // TODO(jyc): use a more delicate message queue
  util::buffering_queue<protocol::message> _received_messages;
  std::function<future<>(protocol::message)> _send;
  std::function<future<>(protocol::group_id, bool)> _report_snapshot_status;
  core::quiesce _quiesce;
  std::unique_ptr<core::peer> _peer;
  std::unique_ptr<rsm::snapshotter> _snapshotter;
  std::unique_ptr<rsm::statemachine_manager> _sm;
  protocol::snapshot_state _snapshot_state;
  uint64_t _current_tick = 0;
  uint64_t _gc_tick = 0;
  uint64_t _applied_index = 0;
  uint64_t _pushed_index = 0;
  uint64_t _confirmed_index = 0;
};

}  // namespace rafter
