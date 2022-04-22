//
// Created by jason on 2022/3/15.
//

#include "node.hh"

#include "rafter/logger.hh"

namespace rafter {

using namespace protocol;

future<bool> node::start(
    raft_config cfg,
    const std::map<uint64_t, std::string>& peers,
    bool initial) {
  _config = std::move(cfg);
  bool new_node = co_await replay_log();
  _peer = std::make_unique<core::peer>(
      _config, _log_reader, peers, initial, new_node);
  _stopped = false;
  co_return new_node;
}

future<> node::stop() {
  if (_stopped) {
    co_return;
  }
  _stopped = true;
  // request_removal();
  co_await _received_messages.close();
  _pending_proposal.close();
  _pending_read_index.close();
  _pending_config_change.close();
  _pending_snapshot.close();
  _pending_leader_transfer.close();
}

future<request_result> node::propose_session(
    const session& s, uint64_t timeout) {
  // TODO(jyc): argument check
  return _pending_proposal.propose(s, {}, timeout);
}

future<request_result> node::propose(
    const session& s, std::string_view cmd, uint64_t timeout) {
  // TODO(jyc): argument check
  return _pending_proposal.propose(s, cmd, timeout);
}

future<request_result> node::read(uint64_t timeout) {
  // TODO(jyc): argument check
  return _pending_read_index.read(timeout);
}

future<request_result> node::request_snapshot(
    const snapshot_option& option, uint64_t timeout) {
  // TODO(jyc): argument check
  snapshot_request req{
      .type = snapshot_request::type::user_triggered,
      .path = option.export_path,
      .compaction_overhead = option.compaction_overhead};
  if (!option.export_path.empty()) {
    req.type = snapshot_request::type::exported;
    auto f = co_await file_type(option.export_path);
    if (!f.has_value()) {
      co_return coroutine::make_exception(
          util::request_error("invalid export path"));
    }
    if (f.value() != directory_entry_type::directory) {
      co_return coroutine::make_exception(
          util::request_error("export path is not a directory"));
    }
  }
  if (option.compaction_overhead == 0) {
    req.compaction_overhead = _config.compaction_overhead;
  }
  co_return co_await _pending_snapshot.request(std::move(req), timeout);
}

future<request_result> node::request_config_change(
    config_change cc, uint64_t timeout) {
  // TODO(jyc): argument check
  return _pending_config_change.request(std::move(cc), timeout);
}

future<request_result> node::request_leader_transfer(uint64_t target_id) {
  // TODO(jyc): argument check
  return _pending_leader_transfer.request(target_id);
}

future<> node::apply_update(
    const log_entry& e,
    protocol::rsm_result result,
    bool rejected,
    bool ignored,
    bool notify_read) {
  if (_config.witness) {
    return make_ready_future<>();
  }
  if (notify_read) {
    _pending_read_index.apply(e.lid.index);
  }
  if (!ignored) {
    _pending_proposal.apply(e.key, std::move(result), rejected);
  }
  return make_ready_future<>();
}

future<> node::apply_config_change(
    protocol::config_change cc, uint64_t key, bool rejected) {
  // unlike the algorithm in thesis, we follow etcd's way
  if (!rejected) {
    co_await _peer->apply_config_change(cc);
    switch (cc.type) {
      case config_change_type::add_node:
      case config_change_type::add_observer:
      case config_change_type::add_witness:
        _node_registry.update(id(), cc.address);
        break;
      case config_change_type::remove_node:
        if (cc.node == _config.node_id) {
          l.info("{}: removing self", id());
          _node_registry.remove_cluster(_config.cluster_id);
          // request removal
        } else {
          _node_registry.remove({_config.cluster_id, cc.node});
        }
        break;
      default:
        l.error("{}: unknown config change type {}", id(), cc.type);
    }
  }
  if (_config.witness) {
    co_return;
  }
  if (rejected) {
    co_await _peer->reject_config_change();
  } else {
    // TODO(jyc): notify config change
  }
  _pending_config_change.apply(key, rejected);
}

future<> node::restore_remotes(protocol::snapshot_ptr ss) {
  if (!ss || !ss->membership) [[unlikely]] {
    throw util::request_error("missing snapshot or membership");
  }
  if (ss->membership->config_change_id == 0) [[unlikely]] {
    throw util::request_error("invalid config change id");
  }
  for (auto& [node_id, address] : ss->membership->addresses) {
    _node_registry.update({_config.cluster_id, node_id}, address);
  }
  for (auto& [node_id, address] : ss->membership->observers) {
    _node_registry.update({_config.cluster_id, node_id}, address);
  }
  for (auto& [node_id, address] : ss->membership->witnesses) {
    _node_registry.update({_config.cluster_id, node_id}, address);
  }
  for (auto [node_id, _] : ss->membership->removed) {
    if (_config.node_id == node_id) {
      _node_registry.remove_cluster(_config.cluster_id);
      // request_removal();
    }
  }
  l.debug("{}: restoring remotes", id());
  co_await _peer->restore_remotes(std::move(ss));
  // TODO(jyc): notify config change
}

future<bool> node::replay_log() {
  l.info("{}: replaying raft logs", id());
  auto snapshot = co_await _logdb.query_snapshot(id());
  if (snapshot && !snapshot->empty()) {
    _log_reader.apply_snapshot(snapshot);
  }
  auto state = co_await _logdb.query_raft_state(id(), snapshot->log_id.index);
  if (state.empty()) {
    co_return true;
  }
  l.info(
      "{}: {}, first index:{}, length:{}",
      state.hard_state,
      state.first_index,
      state.entry_count);
  _log_reader.set_state(state.hard_state);
  _log_reader.set_range(
      {state.first_index, state.first_index + state.entry_count});
  co_return false;
}

future<bool> node::handle_events() {
  bool has_event = false;

  auto last_applied = update_applied_index();
  bool event = last_applied != _confirmed_index;
  has_event = has_event || event;

  event = _peer->has_entry_to_apply();
  has_event = has_event || event;

  event = co_await handle_read_index();
  has_event = has_event || event;

  event = co_await handle_received_messages();
  has_event = has_event || event;

  event = co_await handle_config_change();
  has_event = has_event || event;

  event = co_await handle_proposals();
  has_event = has_event || event;

  event = co_await handle_leader_transfer();
  has_event = has_event || event;

  event = co_await handle_snapshot(last_applied);
  has_event = has_event || event;

  event = co_await handle_compaction();
  has_event = has_event || event;

  gc();
  if (has_event) {
    _pending_read_index.apply(last_applied);
  }

  co_return has_event;
}

future<bool> node::handle_read_index() {
  auto hint = _pending_read_index.pack();
  if (hint.has_value()) {
    _quiesce.record(message_type::read_index);
    co_await _peer->read_index(*hint);
    co_return true;
  }
  co_return false;
}

future<bool> node::handle_received_messages() {
  uint64_t count = 0;
  // TODO(jyc): busy
  auto busy = false;
  auto has_message = _received_messages.size() > 0;
  co_await _received_messages.consume(
      [this, busy, &count](
          std::vector<message>& messages, bool& open) -> future<> {
        for (auto& m : messages) {
          if (m.type == message_type::replicate && busy) {
            continue;
          }
          switch (m.type) {
            case message_type::local_tick: {
              count++;
              co_await tick(m.hint.low);
              break;
            }
            case message_type::quiesce: {
              _quiesce.try_enter_quiesce();
              break;
            }
            case message_type::snapshot_status: {
              l.debug(
                  "{}: received report snapshot status from {}, rejected:{}",
                  id(),
                  m.from,
                  m.reject);
              co_await _peer->report_snapshot_status(m.from, m.reject);
              break;
            }
            case message_type::unreachable: {
              co_await _peer->report_unreachable(m.from);
              break;
            }
            default:
              record_message(m);
              co_await _peer->handle(std::move(m));
              break;
          }
        }
        co_return;
      });
  if (count > _config.election_rtt / 2) {
    l.warn("{}: had {} local tick messages in one batch", id(), count);
  }
  co_return has_message;
}

future<bool> node::handle_config_change() {
  if (_pending_config_change._request.has_value()) {
    _quiesce.record(message_type::config_change);
    co_await _peer->propose_config_change(
        *_pending_config_change._request, *_pending_config_change._key);
    co_return true;
  }
  co_return false;
}

future<bool> node::handle_proposals() {
  auto rate_limited = _peer->rate_limited();
  if (_rate_limited != rate_limited) {
    _rate_limited = rate_limited;
    l.info("{} new rate limit state:{}", id(), rate_limited);
  }
  // TODO(jyc): db busy
  auto paused = rate_limited;
  if (!_pending_proposal._proposal_queue.empty()) {
    auto copy = _pending_proposal._proposal_queue;
    _pending_proposal._proposal_queue.clear();
    co_await _peer->propose_entries(std::move(copy));
    co_return true;
  }
  co_return false;
}

future<bool> node::handle_leader_transfer() {
  if (_pending_leader_transfer._request.has_value()) {
    auto target = *_pending_leader_transfer._request;
    co_await _peer->request_leader_transfer(target);
    co_return true;
  }
  co_return false;
}

future<bool> node::handle_snapshot(uint64_t last_applied) {
  if (_pending_snapshot._request.has_value()) {
    auto req = std::move(_pending_snapshot._request.value());
    if (!req.exported() &&
        last_applied == _snapshot_state.request_snapshot_index) {
      // TODO(jyc): report_ignored_snapshot_status
      co_return false;
    }
    _snapshot_state.request_snapshot_index = last_applied;
    co_await _sm->push(rsm_task{.ss_request = std::move(req), .save = true});
    co_return true;
  }
  co_return false;
}

future<bool> node::handle_compaction() {
  return make_ready_future<bool>(
      _snapshot_state.compacted_to > 0 || _snapshot_state.compact_log_to > 0);
}

void node::gc() {
  if (_gc_tick != _current_tick) {
    _pending_proposal.gc();
    _pending_config_change.gc();
    _pending_snapshot.gc();
    _gc_tick = _current_tick;
  }
}

future<> node::tick(uint64_t tick) {
  _current_tick++;
  _quiesce.tick();
  if (_quiesce.quiesced()) {
    co_await _peer->quiesced_tick();
  } else {
    co_await _peer->tick();
  }
  _pending_proposal.tick(tick);
  _pending_read_index.tick(tick);
  _pending_config_change.tick(tick);
  _pending_snapshot.tick(tick);
  co_return;
}

void node::record_message(const message& m) {
  if ((m.type == message_type::heartbeat ||
       m.type == message_type::heartbeat_resp) &&
      m.hint.low > 0) {
    _quiesce.record(message_type::read_index);
  } else {
    _quiesce.record(m.type);
  }
}

uint64_t node::update_applied_index() {
  _applied_index = _sm->last_applied_index();
  _peer->notify_last_applied(_applied_index);
  return _applied_index;
}

}  // namespace rafter
