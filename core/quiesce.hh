//
// Created by jason on 2022/3/15.
//

#pragma once

#include "protocol/raft.hh"

namespace rafter::core {

class quiesce {
 public:
  quiesce(protocol::group_id gid, bool enable, uint64_t election_tick);
  void set_new_quiesce() noexcept { _new_quiesce = true; }
  bool is_new_quiesce() const noexcept { return _new_quiesce; }
  uint64_t tick() noexcept;
  bool quiesced() const noexcept { return _enabled && _quiesced_since > 0; }
  void record(protocol::message_type type) noexcept;
  uint64_t threshold() const noexcept { return _election_tick * 10; }
  bool new_to_quiesce() const noexcept;
  bool just_exited_quiesce() const noexcept;
  void try_enter_quiesce() noexcept;
  void enter_quiesce() noexcept;
  void exit_quiesce() noexcept;

 private:
  const protocol::group_id _gid;
  bool _enabled = false;
  uint64_t _election_tick = 0;
  uint64_t _current_tick = 0;
  uint64_t _quiesced_since = 0;
  uint64_t _idle_since = 0;
  uint64_t _exit_quiesce_tick = 0;
  bool _new_quiesce = false;
};

}  // namespace rafter::core
