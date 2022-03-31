//
// Created by jason on 2022/3/14.
//

#pragma once

#include <optional>
#include <random>
#include <seastar/core/future.hh>

#include "protocol/client.hh"
#include "protocol/raft.hh"
#include "protocol/rsm.hh"
#include "rafter/config.hh"
#include "util/util.hh"

namespace rafter {

struct request_result {
  enum class code {
    timeout,  // not supported yet
    committed,
    terminated,
    aborted,
    dropped,
    rejected,
    completed,
  };

  static void timeout(seastar::promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::timeout});
  }

  static void commit(seastar::promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::committed});
  }

  static void terminate(seastar::promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::terminated});
  }

  static void abort(seastar::promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::aborted});
  }

  static void drop(seastar::promise<request_result>& promise) {
    promise.set_value(request_result{.code = code::dropped});
  }

  static void reject(
      seastar::promise<request_result>& promise, protocol::rsm_result result) {
    promise.set_value(
        request_result{.code = code::rejected, .result = std::move(result)});
  }

  static void complete(
      seastar::promise<request_result>& promise, protocol::rsm_result result) {
    promise.set_value(
        request_result{.code = code::completed, .result = std::move(result)});
  }

  enum code code;
  protocol::rsm_result result;
};

class pending_proposal {
 public:
  explicit pending_proposal(const raft_config& cfg);
  seastar::future<request_result> propose(
      const protocol::session& session, std::string_view cmd);
  void close();

  void commit(uint64_t key);
  void drop(uint64_t key);
  void apply(uint64_t key, protocol::rsm_result result, bool rejected);

 private:
  const raft_config& _config;
  bool _stopped = false;
  uint64_t _next_key = 0;
  std::vector<protocol::log_entry_ptr> _proposal_queue;
  std::unordered_map<uint64_t, seastar::promise<request_result>> _pending;
};

class pending_read_index {
 public:
  explicit pending_read_index(const raft_config& cfg);
  seastar::future<request_result> read();
  void close();

  std::optional<protocol::hint> pack();
  void add_ready(protocol::ready_to_read_vector readies);
  void drop(protocol::hint hint);
  void apply(uint64_t applied_index);

 private:
  struct read_batch {
    uint64_t index = protocol::log_id::INVALID_INDEX;
    std::vector<seastar::promise<request_result>> requests;
  };
  const raft_config& _config;
  bool _stopped = false;
  std::mt19937_64 _random_engine;
  uint64_t _next_key = 0;
  std::vector<seastar::promise<request_result>> _read_queue;
  std::unordered_map<protocol::hint, read_batch, util::pair_hasher> _pending;
};

class pending_config_change {
 public:
  explicit pending_config_change(const raft_config& cfg);
  seastar::future<request_result> request(protocol::config_change cc);
  void close();

  void commit(uint64_t key);
  void drop(uint64_t key);
  void apply(uint64_t key, bool rejected);

 private:
  const raft_config& _config;
  std::optional<uint64_t> _key;
  std::optional<protocol::config_change> _request;
  std::optional<seastar::promise<request_result>> _pending;
};

class pending_snapshot {
 public:
  explicit pending_snapshot(const raft_config& cfg);
  seastar::future<request_result> request(protocol::snapshot_request request);
  void close();

  void apply(uint64_t key, bool ignored, bool aborted, uint64_t index);

 private:
  const raft_config& _config;
  std::optional<uint64_t> _key;
  std::optional<protocol::snapshot_request> _request;
  std::optional<seastar::promise<request_result>> _pending;
};

class pending_leader_transfer {
 public:
  explicit pending_leader_transfer(const raft_config& cfg);
  void request(uint64_t target);

 private:
  const raft_config& _config;
  std::optional<uint64_t> _request;
  std::optional<seastar::promise<request_result>> _pending;
};

}  // namespace rafter