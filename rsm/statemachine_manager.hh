//
// Created by jason on 2022/4/12.
//

#pragma once

#include <functional>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_mutex.hh>

#include "protocol/raft.hh"
#include "rafter/statemachine.hh"
#include "rsm/files.hh"
#include "rsm/membership.hh"
#include "server/snapshot_context.hh"
#include "util/seastarx.hh"
#include "util/worker.hh"

namespace rafter {

class node;

namespace rsm {

class session_manager;
class snapshotter;

class statemachine_manager {
 public:
  using rsm_result = protocol::rsm_result;

  statemachine_manager(
      node& node, snapshotter& snapshotter, statemachine::factory factory);
  future<> start();
  future<> stop();
  future<> push(protocol::rsm_task task);
  bool busy() const;

  protocol::state_machine_type type() { return _managed->type(); }
  const protocol::membership& get_membership() const { return _members.get(); }

  uint64_t last_applied_index() const { return _last_applied.index; }

  future<rsm_result> lookup(std::string_view cmd);
  future<> sync();
  future<server::snapshot> save(const protocol::snapshot_request& request);
  future<uint64_t> recover(const protocol::rsm_task& task);

 private:
  class managed {
   public:
    managed(bool& stooped, bool witness, std::unique_ptr<statemachine> sm)
      : _stopped(stooped), _witness(witness), _sm(std::move(sm)) {}
    future<rsm_result> open();
    future<rsm_result> update(uint64_t index, std::string_view cmd);
    future<rsm_result> lookup(std::string_view cmd);
    future<> sync();
    future<std::any> prepare();
    future<bool> save(std::any ctx, output_stream<char>& writer, files& fs);
    future<> recover(
        input_stream<char>& reader, const protocol::snapshot_files& fs);
    future<> close();
    protocol::state_machine_type type() { return _sm->type(); }

   private:
    bool& _stopped;
    bool _witness;
    shared_mutex _mtx;
    std::unique_ptr<statemachine> _sm;
  };

  future<> handle(std::vector<protocol::rsm_task>& tasks, bool& open);
  future<> handle_entry(const protocol::log_entry& entry, bool last);
  future<> handle_config_change(const protocol::log_entry& entry);
  future<> handle_update(const protocol::log_entry& entry, bool last);
  future<> handle_save(protocol::rsm_task task);
  future<> handle_recover(protocol::rsm_task task);
  future<> handle_stream(protocol::rsm_task task);

  future<protocol::snapshot_metadata> prepare(
      const protocol::snapshot_request& req);
  future<server::snapshot> do_save(protocol::snapshot_metadata meta);
  future<> do_recover(protocol::snapshot_ptr ss, bool init);

  protocol::snapshot_metadata get_snapshot_meta(
      std::any ctx, const protocol::snapshot_request& req);
  void apply(const protocol::snapshot& ss, bool init);
  void set_last_applied(const protocol::log_entry_vector& entries);
  void set_applied(protocol::log_id lid);

  node& _node;
  snapshotter& _snapshotter;
  bool _stopped = true;
  shared_mutex _mtx;
  statemachine::factory _factory;
  std::unique_ptr<managed> _managed;
  std::unique_ptr<session_manager> _sessions;
  util::worker<protocol::rsm_task> _applier;
  membership _members;
  protocol::log_id _last_applied;
  protocol::log_id _log_id;
  uint64_t _snapshot_index;
};

}  // namespace rsm

}  // namespace rafter
