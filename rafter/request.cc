//
// Created by jason on 2022/3/14.
//

#include "request.hh"

#include "rafter/config.hh"
#include "util/error.hh"

namespace rafter {

pending_proposal::pending_proposal(const raft_config& cfg) : _config(cfg) {
  _proposal_queue.reserve(config::shard().incoming_proposal_queue_length);
}

future<request_result> pending_proposal::propose(
    const protocol::session& session, std::string_view cmd) {
  if (cmd.size() > config::shard().max_entry_size) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cmd", "too big"));
  }
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(
        util::closed_error("pending_proposal"));
  }
  if (_proposal_queue.size() >= config::shard().incoming_proposal_queue_length)
      [[unlikely]] {
    return make_exception_future<request_result>(util::system_busy());
  }
  if (_pending.contains(_next_key + 1)) [[unlikely]] {
    return make_exception_future<request_result>(util::system_busy());
  }
  promise<request_result> pr;
  auto fut = pr.get_future();
  auto& e = _proposal_queue.emplace_back(make_lw_shared<protocol::log_entry>());
  e->type = protocol::entry_type::encoded;
  e->key = _next_key++;
  e->client_id = session.client_id;
  e->series_id = session.series_id;
  e->responded_to = session.responded_to;
  e->payload = cmd;
  _pending.emplace(e->key, std::move(pr));
  return fut;
}

void pending_proposal::close() {
  _stopped = true;
  for (auto& [key, pr] : _pending) {
    // TODO(jyc): exception or abort/terminate state
    pr.set_exception(util::closed_error("pending_proposal"));
  }
  _pending.clear();
}

void pending_proposal::commit(uint64_t key) {
  auto it = _pending.find(key);
  if (it != _pending.end()) {
    request_result::commit(it->second);
    _pending.erase(it);
  }
}

void pending_proposal::drop(uint64_t key) {
  auto it = _pending.find(key);
  if (it != _pending.end()) {
    request_result::drop(it->second);
    _pending.erase(it);
  }
}

void pending_proposal::apply(
    uint64_t key, protocol::rsm_result result, bool rejected) {
  auto it = _pending.find(key);
  if (it != _pending.end()) {
    if (rejected) {
      request_result::reject(it->second, std::move(result));
    } else {
      request_result::complete(it->second, std::move(result));
    }
    _pending.erase(it);
  }
}

pending_read_index::pending_read_index(const raft_config& cfg)
  : _config(cfg)
  , _random_engine(
        std::chrono::system_clock::now().time_since_epoch().count()) {
  _read_queue.reserve(config::shard().incoming_read_index_queue_length);
}

future<request_result> pending_read_index::read() {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(
        util::closed_error("pending_read_index"));
  }
  if (_read_queue.size() >= config::shard().incoming_read_index_queue_length)
      [[unlikely]] {
    return make_exception_future<request_result>(util::system_busy());
  }
  return _read_queue.emplace_back().get_future();
}

void pending_read_index::close() {
  _stopped = true;
  for (auto& [hint, batch] : _pending) {
    // TODO(jyc): exception or abort/terminate state
    for (auto& pr : batch.requests) {
      pr.set_exception(util::closed_error("pending_read_index"));
    }
  }
  for (auto& pr : _read_queue) {
    pr.set_exception(util::closed_error("pending_read_index"));
  }
  _read_queue.clear();
  _pending.clear();
}

std::optional<protocol::hint> pending_read_index::pack() {
  if (_stopped) {
    return std::nullopt;
  }
  if (_read_queue.empty()) {
    return std::nullopt;
  }
  // TODO(jyc): make use of hint.high
  auto hint = protocol::hint{.low = _random_engine(), .high = _next_key++};
  if (_pending.contains(hint)) [[unlikely]] {
    // TODO(jyc): better handling
    return std::nullopt;
  }
  auto& batch = _pending[hint];
  batch.requests.reserve(_read_queue.capacity());
  std::swap(batch.requests, _read_queue);
  return hint;
}

void pending_read_index::add_ready(protocol::ready_to_read_vector readies) {
  if (readies.empty()) {
    return;
  }
  for (const auto& ready : readies) {
    auto it = _pending.find(ready.context);
    if (it != _pending.end()) {
      it->second.index = ready.index;
    }
  }
}

void pending_read_index::drop(protocol::hint hint) {
  if (_stopped) {
    return;
  }
  if (auto it = _pending.find(hint); it != _pending.end()) {
    for (auto& pr : it->second.requests) {
      request_result::drop(pr);
    }
    _pending.erase(it);
  }
}

void pending_read_index::apply(uint64_t applied_index) {
  if (_stopped || _pending.empty()) {
    return;
  }
  for (auto it = _pending.begin(); it != _pending.end();) {
    if (it->second.index != protocol::log_id::INVALID_INDEX &&
        it->second.index < applied_index) {
      for (auto& pr : it->second.requests) {
        request_result::complete(pr, {});
      }
      it = _pending.erase(it);
    } else {
      it++;
    }
  }
}

pending_config_change::pending_config_change(const raft_config& cfg)
  : _config(cfg) {}

future<request_result> pending_config_change::request(
    protocol::config_change cc) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(
        util::closed_error("pending_config_change"));
  }
  if (_key.has_value()) {
    return make_exception_future<request_result>(util::system_busy());
  }
  _key = _next_key++;
  _request = cc;
  _pending = promise<request_result>();
  return _pending->get_future();
}

void pending_config_change::close() {
  _stopped = true;
  if (_pending.has_value()) {
    _pending->set_exception(util::closed_error("pending_config_change"));
  }
  _key = std::nullopt;
  _request = std::nullopt;
  _pending = std::nullopt;
}

void pending_config_change::commit(uint64_t key) {
  if (_key.has_value() && _key.value() == key) {
    request_result::commit(_pending.value());
    _key = std::nullopt;
    _request = std::nullopt;
    _pending = std::nullopt;
  }
}

void pending_config_change::drop(uint64_t key) {
  if (_key.has_value() && _key.value() == key) {
    request_result::drop(_pending.value());
    _key = std::nullopt;
    _request = std::nullopt;
    _pending = std::nullopt;
  }
}

void pending_config_change::apply(uint64_t key, bool rejected) {
  if (_key.has_value() && _key.value() == key) {
    if (rejected) {
      request_result::reject(_pending.value(), {});
    } else {
      request_result::complete(_pending.value(), {});
    }
    _key = std::nullopt;
    _request = std::nullopt;
    _pending = std::nullopt;
  }
}

pending_snapshot::pending_snapshot(const raft_config& cfg) : _config(cfg) {}

future<request_result> pending_snapshot::request(
    protocol::snapshot_request request) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(
        util::closed_error("pending_snapshot"));
  }
  if (_key.has_value()) {
    return make_exception_future<request_result>(util::system_busy());
  }
  _key = _next_key++;
  _request = std::move(request);
  _pending = promise<request_result>();
  return _pending->get_future();
}

void pending_snapshot::close() {
  _stopped = true;
  if (_pending.has_value()) {
    _pending->set_exception(util::closed_error("pending_snapshot"));
  }
  _key = std::nullopt;
  _request = std::nullopt;
  _pending = std::nullopt;
}

void pending_snapshot::apply(
    uint64_t key, bool ignored, bool aborted, uint64_t index) {
  if (ignored && aborted) {
    throw util::failed_precondition_error("ignored and aborted snapshot");
  }
  if (_key.has_value() && _key.value() == key) {
    if (ignored) {
      request_result::reject(_pending.value(), {});
    } else if (aborted) {
      request_result::abort(_pending.value());
    } else {
      request_result::complete(_pending.value(), {.value = index});
    }
    _key = std::nullopt;
    _request = std::nullopt;
    _pending = std::nullopt;
  }
}

pending_leader_transfer::pending_leader_transfer(const raft_config& cfg)
  : _config(cfg) {}

future<request_result> pending_leader_transfer::request(uint64_t target) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(
        util::closed_error("pending_snapshot"));
  }
  if (_request.has_value()) {
    return make_exception_future<request_result>(util::system_busy());
  }
  _request = target;
  _pending = promise<request_result>();
  return _pending->get_future();
}

void pending_leader_transfer::close() {
  _stopped = true;
  if (_pending.has_value()) {
    _pending->set_exception(util::closed_error("pending_leader_transfer"));
  }
  _request = std::nullopt;
  _pending = std::nullopt;
}

void pending_leader_transfer::notify(uint64_t leader_id) {
  if (_pending.has_value()) {
    if (leader_id == _request.value()) {
      request_result::complete(_pending.value(), {.value = leader_id});
    } else {
      request_result::reject(_pending.value(), {.value = leader_id});
    }
    _request = std::nullopt;
    _pending = std::nullopt;
  }
}

}  // namespace rafter
