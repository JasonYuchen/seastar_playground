//
// Created by jason on 2021/12/15.
//

#include "remote.hh"

#include "util/error.hh"

namespace rafter::core {

bool remote::is_paused() const noexcept {
  return state == state::wait || state == state::snapshot;
}

void remote::become_retry() noexcept {
  if (state == state::snapshot) {
    next = std::max(match, snapshot_index) + 1;
  } else {
    next = match + 1;
  }
  snapshot_index = 0;
  state = state::retry;
}

void remote::become_wait() noexcept {
  become_retry();
  retry_to_wait();
}

void remote::become_replicate() noexcept {
  next = match + 1;
  snapshot_index = 0;
  state = state::replicate;
}

void remote::become_snapshot(uint64_t index) noexcept {
  snapshot_index = index;
  state = state::snapshot;
}

void remote::retry_to_wait() noexcept {
  if (state == state::retry) {
    state = state::wait;
  }
}

void remote::wait_to_retry() noexcept {
  if (state == state::wait) {
    state = state::retry;
  }
}

void remote::replicate_to_retry() noexcept {
  if (state == state::replicate) {
    become_retry();
  }
}

void remote::clear_pending_snapshot() noexcept { snapshot_index = 0; }

bool remote::try_update(uint64_t index) noexcept {
  if (next < index + 1) {
    next = index + 1;
  }
  if (match < index) {
    wait_to_retry();
    match = index;
    return true;
  }
  return false;
}

void remote::optimistic_update(uint64_t last_index) {
  if (state == state::replicate) {
    next = last_index + 1;
  } else if (state == state::retry) {
    // do not optimistically update the `next` since the remote is in Retry
    // state due to inflight snapshot or unreachable network
    retry_to_wait();
  } else {
    throw util::failed_precondition_error("unexpected optimistic_update");
  }
}

bool remote::try_decrease(
    uint64_t rejected_index, uint64_t peer_last_index) noexcept {
  if (state == state::replicate) {
    if (rejected_index <= match) {
      // The rejection must be stale if the progress has matched and
      // `rejected_index` is smaller than `match`
      return false;
    }
    // The `next` is set to `match` + 1 which is more conservative than the
    // `next` = `next` - 1 proposed in Raft thesis.
    next = match + 1;
    return true;
  }
  if (rejected_index != next - 1) {
    // The rejection must be stale if `rejected_index` does not match `next` - 1
    // because the log_index of the replication_resp is set to `next` - 1 in
    // `make_replicate_message`.
    return false;
  }
  wait_to_retry();
  next = std::max(uint64_t{1}, std::min(rejected_index, peer_last_index + 1));
  return true;
}

void remote::responded_to() noexcept {
  if (state == state::retry) {
    become_replicate();
  } else if (state == state::snapshot && match >= snapshot_index) {
    become_retry();
  }
}

void remote::set_snapshot_ack(uint64_t tick, bool rejected) {
  if (state == state::snapshot) {
    snapshot_tick = tick;
    snapshot_rejected = rejected;
  } else {
    throw util::failed_precondition_error(
        "setting snapshot ack in non-snapshot state");
  }
}

void remote::clear_snapshot_ack() {
  snapshot_tick = 0;
  snapshot_rejected = false;
}

bool remote::snapshot_ack_tick() {
  if (snapshot_tick > 0) {
    snapshot_tick--;
    return snapshot_tick == 0;
  }
  return false;
}

std::ostream& operator<<(std::ostream& os, enum remote::state s) {
  static std::string_view states[] = {"retry", "wait", "replicate", "snapshot"};
  return os << states[static_cast<uint8_t>(s)];
}

std::ostream& operator<<(std::ostream& os, const remote& r) {
  return os << "remote["
            << "mi:" << r.match << ",ni:" << r.next << ",st:" << r.state
            << ",si:" << r.snapshot_index << ",a:" << r.active << "]";
}

}  // namespace rafter::core
