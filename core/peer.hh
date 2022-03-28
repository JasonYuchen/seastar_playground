//
// Created by jason on 2021/12/15.
//

#pragma once

#include <memory>

#include "core/raft.hh"
#include "protocol/raft.hh"

namespace rafter::core {

class peer {
 public:
  peer(
      const raft_config& c,
      log_reader& lr,
      const std::map<uint64_t, std::string>& addresses,
      bool initial,
      bool new_node);
  // void gc(uint64_t now);
  seastar::future<> tick();
  seastar::future<> quiesced_tick();
  seastar::future<> request_leader_transfer(uint64_t target);
  seastar::future<> read_index(protocol::hint ctx);
  seastar::future<> propose_entries(protocol::log_entry_vector entries);
  seastar::future<> propose_config_change(
      protocol::config_change change, uint64_t key);
  seastar::future<> apply_config_change(protocol::config_change change);
  seastar::future<> reject_config_change();
  seastar::future<> restore_remotes(protocol::snapshot_ptr snapshot);
  seastar::future<> report_unreachable(uint64_t node);
  seastar::future<> report_snapshot_status(uint64_t node, bool reject);
  seastar::future<> handle(protocol::message m);
  bool has_entry_to_apply();
  bool has_update(bool more_to_apply);
  seastar::future<protocol::update> get_update(
      bool more_to_apply, uint64_t last_applied);
  void commit(const protocol::update& up);
  void notify_last_applied(uint64_t last_applied) noexcept;
  bool is_leader() const noexcept { return _raft.is_leader(); }
  bool rate_limited() const noexcept { return _raft._limiter.enabled(); }

 private:
  void bootstrap(const std::map<uint64_t, std::string>& addresses);

  protocol::hard_state _prev_state;
  raft _raft;
};

}  // namespace rafter::core
