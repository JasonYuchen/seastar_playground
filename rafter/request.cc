//
// Created by jason on 2022/3/14.
//

#include "request.hh"

#include "rafter/config.hh"
#include "rafter/logger.hh"
#include "util/error.hh"

namespace rafter {

using namespace protocol;

std::string_view request_result::name(enum code c) {
  static std::string_view names[] = {
      "timeout",
      "committed",
      "terminated",
      "aborted",
      "dropped",
      "rejected",
      "completed",
  };
  static_assert(std::size(names) == static_cast<uint8_t>(code::num_of_code));
  assert(static_cast<uint8_t>(c) < static_cast<uint8_t>(code::num_of_code));
  return names[static_cast<uint8_t>(c)];
}

std::ostream& operator<<(std::ostream& os, enum request_result::code c) {
  return os << request_result::name(c);
}

pending_proposal::pending_proposal(const raft_config& cfg) : _config(cfg) {
  _proposal_queue.reserve(config::shard().incoming_proposal_queue_length);
}

future<request_result> pending_proposal::propose(
    const session& session, std::string_view cmd, uint64_t timeout) {
  if (cmd.size() + log_entry{}.in_memory_bytes() >
      config::shard().max_entry_bytes) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cmd", "too big"));
  }
  if (timeout == 0) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("timeout", "too small"));
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
  auto& e = _proposal_queue.emplace_back();
  e.type = cmd.empty() ? entry_type::application : entry_type::encoded;
  e.key = ++_next_key;
  e.client_id = session.client_id;
  e.series_id = session.series_id;
  e.responded_to = session.responded_to;
  // TODO(jyc): add compression support
  e.copy_of(cmd);
  auto deadline = _clock.tick + timeout;
  auto [it, inserted] = _pending.emplace(e.key, request_state{deadline});
  assert(inserted);
  return it->second.result.get_future();
}

void pending_proposal::close() {
  _stopped = true;
  for (auto& [key, st] : _pending) {
    // TODO(jyc): exception or abort/terminate state
    st.result.set_exception(util::closed_error("pending_proposal"));
  }
  _pending.clear();
}

void pending_proposal::gc() {
  if (_stopped) {
    return;
  }
  auto now = _clock.tick;
  if (now - _clock.last_gc < request_state::logical_clock::DEFAULT_GC_TICK) {
    return;
  }
  _clock.last_gc = now;
  for (auto it = _pending.begin(); it != _pending.end();) {
    if (it->second.deadline < now) {
      request_result::timeout(it->second.result);
      it = _pending.erase(it);
    } else {
      ++it;
    }
  }
}

void pending_proposal::commit(uint64_t key) {
  auto it = _pending.find(key);
  if (it != _pending.end()) {
    request_result::commit(it->second.result);
    _pending.erase(it);
  }
}

void pending_proposal::drop(uint64_t key) {
  auto it = _pending.find(key);
  if (it != _pending.end()) {
    request_result::drop(it->second.result);
    _pending.erase(it);
  }
}

void pending_proposal::apply(uint64_t key, rsm_result result, bool rejected) {
  auto it = _pending.find(key);
  if (it != _pending.end()) {
    if (rejected) {
      request_result::reject(it->second.result, std::move(result));
    } else {
      request_result::complete(it->second.result, std::move(result));
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

future<request_result> pending_read_index::read(uint64_t timeout) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(
        util::closed_error("pending_read_index"));
  }
  if (timeout == 0) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("timeout", "too small"));
  }
  if (_read_queue.size() >= config::shard().incoming_read_index_queue_length)
      [[unlikely]] {
    return make_exception_future<request_result>(util::system_busy());
  }
  auto deadline = _clock.tick + timeout;
  auto& st = _read_queue.emplace_back(request_state{deadline});
  return st->result.get_future();
}

void pending_read_index::close() {
  _stopped = true;
  for (auto& [hint, batch] : _pending) {
    // TODO(jyc): exception or abort/terminate state
    for (auto& st : batch.requests) {
      if (st.has_value()) {
        st->result.set_exception(util::closed_error("pending_read_index"));
      }
    }
  }
  for (auto& st : _read_queue) {
    if (st.has_value()) {
      st->result.set_exception(util::closed_error("pending_read_index"));
    }
  }
  _read_queue.clear();
  _pending.clear();
}

void pending_read_index::gc() {
  if (_stopped) {
    return;
  }
  auto now = _clock.tick;
  if (now - _clock.last_gc < request_state::logical_clock::DEFAULT_GC_TICK) {
    return;
  }
  _clock.last_gc = now;
  for (auto it = _pending.begin(); it != _pending.end();) {
    size_t count = 0;
    for (auto& st : it->second.requests) {
      if (st.has_value() && st->deadline < now) {
        request_result::timeout(st->result);
        st.reset();
      }
      if (st.has_value()) {
        count++;
      }
    }
    if (count == 0) {
      // all request in this batch have been notified
      it = _pending.erase(it);
    } else {
      ++it;
    }
  }
}

std::optional<hint> pending_read_index::pack() {
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

void pending_read_index::add_ready(const ready_to_read_vector& readies) {
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

void pending_read_index::drop(struct hint hint) {
  if (_stopped) {
    return;
  }
  if (auto it = _pending.find(hint); it != _pending.end()) {
    for (auto& st : it->second.requests) {
      if (st.has_value()) {
        request_result::drop(st->result);
        st.reset();
      }
    }
    _pending.erase(it);
  }
}

void pending_read_index::apply(uint64_t applied_index) {
  if (_stopped || _pending.empty()) {
    return;
  }
  auto now = _clock.tick;
  for (auto it = _pending.begin(); it != _pending.end();) {
    if (it->second.index != log_id::INVALID_INDEX &&
        it->second.index <= applied_index) {
      for (auto& st : it->second.requests) {
        if (st.has_value()) {
          if (st->deadline < now) {
            request_result::timeout(st->result);
          } else {
            request_result::complete(st->result, {});
          }
          st.reset();
        }
      }
      it = _pending.erase(it);
    } else {
      it++;
    }
  }
  // apply triggered gc
  gc();
}

pending_config_change::pending_config_change(const raft_config& cfg)
  : _config(cfg) {}

future<request_result> pending_config_change::request(
    config_change cc, uint64_t timeout) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(
        util::closed_error("pending_config_change"));
  }
  if (timeout == 0) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("timeout", "too small"));
  }
  if (_key.has_value()) [[unlikely]] {
    return make_exception_future<request_result>(util::system_busy());
  }
  _key = _next_key++;
  _request = std::move(cc);
  _pending = request_state{_clock.tick + timeout};
  return _pending->result.get_future();
}

void pending_config_change::close() {
  _stopped = true;
  if (_pending.has_value()) {
    _pending->result.set_exception(util::closed_error("pending_config_change"));
  }
  reset();
}

void pending_config_change::gc() {
  if (_stopped || !_pending.has_value()) {
    return;
  }
  auto now = _clock.tick;
  if (now - _clock.last_gc < request_state::logical_clock::DEFAULT_GC_TICK) {
    return;
  }
  _clock.last_gc = now;
  if (_pending->deadline < now) {
    request_result::timeout(_pending->result);
    reset();
  }
}

void pending_config_change::commit(uint64_t key) {
  if (_key.has_value() && _key.value() == key) {
    request_result::commit(_pending->result);
    reset();
  }
}

void pending_config_change::drop(uint64_t key) {
  if (_key.has_value() && _key.value() == key) {
    request_result::drop(_pending->result);
    reset();
  }
}

void pending_config_change::apply(uint64_t key, bool rejected) {
  if (_key.has_value() && _key.value() == key) {
    if (rejected) {
      request_result::reject(_pending->result, {});
    } else {
      request_result::complete(_pending->result, {});
    }
    reset();
  }
}

void pending_config_change::reset() {
  _key.reset();
  _request.reset();
  _pending.reset();
}

pending_snapshot::pending_snapshot(const raft_config& cfg) : _config(cfg) {}

future<request_result> pending_snapshot::request(
    snapshot_request request, uint64_t timeout) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(
        util::closed_error("pending_snapshot"));
  }
  if (timeout == 0) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("timeout", "too small"));
  }
  if (_key.has_value()) [[unlikely]] {
    return make_exception_future<request_result>(util::system_busy());
  }
  _key = _next_key++;
  _request = std::move(request);
  _pending = request_state{_clock.tick + timeout};
  return _pending->result.get_future();
}

void pending_snapshot::close() {
  _stopped = true;
  if (_pending.has_value()) {
    _pending->result.set_exception(util::closed_error("pending_snapshot"));
  }
  reset();
}

void pending_snapshot::gc() {
  if (_stopped || !_pending.has_value()) {
    return;
  }
  auto now = _clock.tick;
  if (now - _clock.last_gc < request_state::logical_clock::DEFAULT_GC_TICK) {
    return;
  }
  _clock.last_gc = now;
  if (_pending->deadline < now) {
    request_result::timeout(_pending->result);
    reset();
  }
}

void pending_snapshot::apply(
    uint64_t key, bool ignored, bool aborted, uint64_t index) {
  if (ignored && aborted) {
    throw util::failed_precondition_error("ignored and aborted snapshot");
  }
  if (_key.has_value() && _key.value() == key) {
    if (ignored) {
      request_result::reject(_pending->result, {});
    } else if (aborted) {
      request_result::abort(_pending->result);
    } else {
      request_result::complete(_pending->result, {.value = index});
    }
    reset();
  }
}

void pending_snapshot::reset() {
  _key.reset();
  _request.reset();
  _pending.reset();
}

pending_leader_transfer::pending_leader_transfer(const raft_config& cfg)
  : _config(cfg) {}

future<request_result> pending_leader_transfer::request(uint64_t target) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(
        util::closed_error("pending_snapshot"));
  }
  if (_request.has_value()) [[unlikely]] {
    return make_exception_future<request_result>(util::system_busy());
  }
  _request = target;
  _pending = request_state{};
  return _pending->result.get_future();
}

void pending_leader_transfer::close() {
  _stopped = true;
  if (_pending.has_value()) {
    _pending->result.set_exception(
        util::closed_error("pending_leader_transfer"));
  }
  _request = std::nullopt;
  _pending = std::nullopt;
}

void pending_leader_transfer::notify(uint64_t leader_id) {
  if (_pending.has_value()) {
    if (leader_id == _request.value()) {
      request_result::complete(_pending->result, {.value = leader_id});
    } else {
      request_result::reject(_pending->result, {.value = leader_id});
    }
    _request = std::nullopt;
    _pending = std::nullopt;
  }
}

}  // namespace rafter
