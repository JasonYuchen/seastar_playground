//
// Created by jason on 2022/3/14.
//

#pragma once

#include <optional>
#include <random>
#include <seastar/core/future.hh>

#include "protocol/client.hh"
#include "protocol/raft.hh"
#include "rafter/config.hh"
#include "util/seastarx.hh"
#include "util/util.hh"

namespace rafter {

struct request_result {
  enum class code : uint8_t {
    timeout,
    committed,
    terminated,
    aborted,
    dropped,
    rejected,
    completed,
    num_of_code,
  };
  static std::string_view name(enum code c);
  friend std::ostream& operator<<(std::ostream& os, enum code c);

  static void timeout(promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::timeout});
  }

  static void commit(promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::committed});
  }

  static void terminate(promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::terminated});
  }

  static void abort(promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::aborted});
  }

  static void drop(promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::dropped});
  }

  static void reject(
      promise<request_result>& promise, protocol::rsm_result result) {
    promise.set_value(
        request_result{.code = code::rejected, .result = std::move(result)});
  }

  static void complete(
      promise<request_result>& promise, protocol::rsm_result result) {
    promise.set_value(
        request_result{.code = code::completed, .result = std::move(result)});
  }

  enum code code;
  protocol::rsm_result result;
};

struct request_state {
  // TODO(jyc): switch to std::chrono
  struct logical_clock {
    uint64_t tick = 0;
    uint64_t last_gc = 0;
    static constexpr uint64_t DEFAULT_GC_TICK = 2;
  };
  request_state() = default;
  explicit request_state(uint64_t deadline) : deadline(deadline) {}
  DEFAULT_MOVE_AND_ASSIGN(request_state);

  uint64_t deadline = UINT64_MAX;
  promise<request_result> result;
};

class pending_proposal {
 public:
  explicit pending_proposal(const raft_config& cfg);
  future<request_result> propose(
      const protocol::session& session, std::string_view cmd, uint64_t timeout);
  void close();
  void tick() { _clock.tick++; }
  void gc();

  void commit(uint64_t key);
  void drop(uint64_t key);
  void apply(uint64_t key, protocol::rsm_result result, bool rejected);

 private:
  friend class node;

  const raft_config& _config;
  bool _stopped = false;
  request_state::logical_clock _clock;
  uint64_t _next_key = 0;
  std::vector<protocol::log_entry_ptr> _proposal_queue;
  std::unordered_map<uint64_t, request_state> _pending;
};

class pending_read_index {
 public:
  explicit pending_read_index(const raft_config& cfg);
  future<request_result> read(uint64_t timeout);
  void close();
  void tick() { _clock.tick++; }
  void gc();

  std::optional<protocol::hint> pack();
  void add_ready(const protocol::ready_to_read_vector& readies);
  void drop(protocol::hint hint);
  void apply(uint64_t applied_index);

 private:
  friend class node;

  struct read_batch {
    uint64_t index = protocol::log_id::INVALID_INDEX;
    std::vector<std::optional<request_state>> requests;
  };
  const raft_config& _config;
  bool _stopped = false;
  request_state::logical_clock _clock;
  std::mt19937_64 _random_engine;
  uint64_t _next_key = 0;
  std::vector<std::optional<request_state>> _read_queue;
  std::unordered_map<protocol::hint, read_batch, util::pair_hasher> _pending;
};

class pending_config_change {
 public:
  explicit pending_config_change(const raft_config& cfg);
  future<request_result> request(protocol::config_change cc, uint64_t timeout);
  void close();
  void tick() { _clock.tick++; }
  void gc();

  void commit(uint64_t key);
  void drop(uint64_t key);
  void apply(uint64_t key, bool rejected);

 private:
  friend class node;

  void reset();

  const raft_config& _config;
  bool _stopped = false;
  request_state::logical_clock _clock;
  uint64_t _next_key = 0;
  std::optional<uint64_t> _key;
  std::optional<protocol::config_change> _request;
  std::optional<request_state> _pending;
};

class pending_snapshot {
 public:
  explicit pending_snapshot(const raft_config& cfg);
  future<request_result> request(
      protocol::snapshot_request request, uint64_t timeout);
  void close();
  void tick() { _clock.tick++; }
  void gc();

  void apply(uint64_t key, bool ignored, bool aborted, uint64_t index);

 private:
  friend class node;

  void reset();

  const raft_config& _config;
  bool _stopped = false;
  request_state::logical_clock _clock;
  uint64_t _next_key = 0;
  std::optional<uint64_t> _key;
  std::optional<protocol::snapshot_request> _request;
  std::optional<request_state> _pending;
};

class pending_leader_transfer {
  // TODO(jyc): timeout support, notify support
 public:
  explicit pending_leader_transfer(const raft_config& cfg);
  future<request_result> request(uint64_t target);
  void close();

  void notify(uint64_t leader_id);

 private:
  friend class node;

  const raft_config& _config;
  bool _stopped = false;
  std::optional<uint64_t> _request;
  std::optional<request_state> _pending;
};

}  // namespace rafter
