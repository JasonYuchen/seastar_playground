//
// Created by jason on 2022/3/15.
//

#include "quiesce.hh"

#include "core/logger.hh"

namespace rafter::core {

using namespace protocol;

quiesce::quiesce(group_id gid, bool enable, uint64_t election_tick)
  : _gid(gid), _enabled(enable), _election_tick(election_tick) {}

uint64_t quiesce::tick() noexcept {
  if (!_enabled) {
    return 0;
  }
  _current_tick++;
  if (!quiesced() && _current_tick - _idle_since > threshold()) {
    enter_quiesce();
  }
  return _current_tick;
}

void quiesce::record(message_type type) noexcept {
  if (!_enabled) {
    return;
  }
  if (type == message_type::heartbeat || type == message_type::heartbeat_resp) {
    if (!quiesced() || new_to_quiesce()) {
      return;
    }
  }
  _idle_since = _current_tick;
  if (quiesced()) {
    exit_quiesce();
    l.info("{} exited from quiesce due to {}", _gid, type);
  }
}

bool quiesce::new_to_quiesce() const noexcept {
  if (!quiesced()) {
    return false;
  }
  return _current_tick - _quiesced_since < _election_tick;
}

bool quiesce::just_exited_quiesce() const noexcept {
  if (quiesced()) {
    return false;
  }
  return _current_tick - _exit_quiesce_tick < threshold();
}

void quiesce::try_enter_quiesce() noexcept {
  if (!just_exited_quiesce() && !quiesced()) {
    l.info("{} entering quiesce due to quiesce message", _gid);
    enter_quiesce();
  }
}

void quiesce::enter_quiesce() noexcept {
  _quiesced_since = _current_tick;
  _idle_since = _current_tick;
  set_new_quiesce();
}

void quiesce::exit_quiesce() noexcept {
  _quiesced_since = 0;
  _exit_quiesce_tick = _current_tick;
}

}  // namespace rafter::core
