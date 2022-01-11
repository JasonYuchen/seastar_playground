//
// Created by jason on 2021/12/15.
//

#pragma once

#include <stdint.h>

#include <string>

namespace rafter::core {

class remote {
 public:
  enum state : uint8_t {
    // Retry means leader could probe the remote after:
    // 1. The remote has been Wait and now leader receives a heartbeat response
    //    which means leader could probe the remote then, set remote as Retry.
    // 2. The remote has been Wait or Replicate and now leader receives a
    //    rejection of the replication, try to decrease the next index for the
    //    remote and set the remote as Retry.
    // 3. The remote has been Wait and now leader receives an acceptance of the
    //    replication, try to increase the next & match index for the remote
    //    and set the remote as Retry.
    // 4. The leader receives an acceptance of replication, if the remote is in
    //    Snapshot state and the match >= snapshotIndex, set the remote as Retry
    //    and do probing.
    // 5. The remote is unreachable, if it is Replicate then set it as Retry.
    // Retry is an active state and the leader can send messages.
    retry,
    // Wait means leader should stop replicating after:
    // 1. The remote has been Snapshot and leader receives the result, set the
    //    remote as Wait whether the result is successful or failed.
    //    If successful, just wait for the replication response which will be
    //    sent once the remote is restored (the remote will responds a
    //    replication response once restored the snapshot).
    //    If failed, just wait for another heartbeat interval before next probe
    //    (the ReportSnapshotStatus will be called when transport module failed
    //    to send the snapshot which eventually calls becomeWait).
    // 2. Leader usually optimistically updates the next for remotes, but if a
    //    remote has been in Retry, set it as Wait to reduce redundant probes.
    // Wait is an inactive state and the leader won't send messages.
    wait,
    // Replicate means the remote is eager for the replication after:
    // 1. The remote has been Retry and leader receives an acceptance of the
    //    replication, set the remote as Replicate.
    // Replicate is an active state and the leader can send messages.
    replicate,
    // Snapshot means the remote is far behind the leader and needs a snapshot:
    // 1. The remote is set as Snapshot if the required logs are compacted.
    // Snapshot is an inactive state and the leader won't send messages.
    snapshot,
    num_of_state,
  };

  bool is_paused() const noexcept;

  // The remote state is unknown, retry to find its index.
  // See probe state in etcd:
  // https://github.com/etcd-io/etcd/blob/master/raft/tracker/progress.go#L43
  void become_retry() noexcept;

  // The remote state is waiting due to inflight snapshot.
  // The etcd does not have this state (retry + wait here = probe in etcd).
  void become_wait() noexcept;

  // The remote state is clear, keep replicating logs.
  // See replicate state in etcd:
  // https://github.com/etcd-io/etcd/blob/master/raft/tracker/progress.go#L43
  void become_replicate() noexcept;

  // The snapshot is inflight, stop replicating logs.
  // See snapshot state in etcd:
  // https://github.com/etcd-io/etcd/blob/master/raft/tracker/progress.go#L43
  void become_snapshot(uint64_t index) noexcept;

  void retry_to_wait() noexcept;
  void wait_to_retry() noexcept;
  void replicate_to_retry() noexcept;
  void clear_pending_snapshot() noexcept;

  // Will be called when receives replicate_resp, try to update the log track of
  // the remote node, return true if updated.
  bool try_update(uint64_t index) noexcept;

  // Will be called when sends replicate messages, optimistically update the
  // "next" for a remote in replicate state.
  void optimistic_update(uint64_t last_index);

  // Will be called when receives replication_resp with rejection, try to
  // decrease the log track of the remote node, return true if decreased.
  bool try_decrease(uint64_t rejected_index, uint64_t peer_last_index) noexcept;

  void responded_to() noexcept;

  std::string debug_string() const;

  uint8_t match = 0;
  uint8_t next = 0;
  uint8_t snapshot_index = 0;
  bool active = false;
  enum state state = state::retry;
};

}  // namespace rafter::core
