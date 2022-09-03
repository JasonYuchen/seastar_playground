//
// Created by jason on 2022/3/15.
//

#include "node.hh"

#include <seastar/core/coroutine.hh>

#include "rafter/logger.hh"
#include "rafter/nodehost.hh"
#include "rsm/session_manager.hh"
#include "rsm/snapshotter.hh"

namespace rafter {

using namespace protocol;

node::node(
    raft_config cfg,
    nodehost& host,
    transport::registry& registry,
    storage::logdb& logdb,
    std::unique_ptr<rsm::snapshotter> snapshotter,
    statemachine::factory&& sm_factory,
    std::function<future<>(protocol::message)>&& sender,
    std::function<future<>(protocol::group_id, bool)>&& snapshot_notifier)
  : _config(std::move(cfg))
  , _nodehost(host)
  , _node_registry(registry)
  , _logdb(logdb)
  , _log_reader(id(), _logdb)
  , _pending_proposal(_config)
  , _pending_read_index(_config)
  , _pending_snapshot(_config)
  , _pending_config_change(_config)
  , _pending_leader_transfer(_config)
  , _received_messages("q", 100 /*FIXME*/)
  , _send(std::move(sender))
  , _report_snapshot_status(std::move(snapshot_notifier))
  , _quiesce(id(), _config.quiesce, _config.election_rtt * 2)
  , _snapshotter(std::move(snapshotter))
  , _sm(std::make_unique<rsm::statemachine_manager>(
        *this, *_snapshotter, std::move(sm_factory))) {}

future<> node::start(const member_map& peers, bool initial) {
  co_await _sm->start();
  bool new_node = co_await replay_log();
  _peer = std::make_unique<core::peer>(
      _config, _log_reader, peers, initial, new_node);
  _stopped = false;
  _new_node = new_node;
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
  co_await _sm->stop();
}

future<std::optional<update>> node::step() {
  if (!_initialized) {
    co_return std::nullopt;
  }
  bool has_event = co_await handle_events();
  if (!has_event) {
    co_return std::nullopt;
  }
  if (_quiesce.is_new_quiesce()) {
    co_await send_enter_quiesce_messages();
  }
  co_return co_await get_update();
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
      co_await coroutine::return_exception(
          util::request_error("invalid export path"));
    }
    if (f.value() != directory_entry_type::directory) {
      co_await coroutine::return_exception(
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

future<> node::apply_entry(
    const log_entry& e,
    protocol::rsm_result result,
    bool rejected,
    bool ignored,
    bool notify_read) {
  if (is_witness()) {
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
  if (is_witness()) {
    co_return;
  }
  if (rejected) {
    co_await _peer->reject_config_change();
  } else {
    // TODO(jyc): notify config change
  }
  _pending_config_change.apply(key, rejected);
}

future<> node::apply_snapshot(
    uint64_t key, bool ignored, bool aborted, uint64_t index) {
  _pending_snapshot.apply(key, ignored, aborted, index);
}

future<> node::restore_remotes(protocol::snapshot_ptr ss) {
  if (!ss || !ss->membership) [[unlikely]] {
    co_await coroutine::return_exception(
        util::request_error("missing snapshot or membership"));
  }
  if (ss->membership->config_change_id == 0) [[unlikely]] {
    co_await coroutine::return_exception(
        util::request_error("invalid config change id"));
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

future<> node::apply_raft_update(protocol::update& up) {
  auto entries = utils::entries_to_apply(up.committed_entries, _pushed_index);
  if (entries.empty()) {
    co_return;
  }
  auto last_index = entries.back().lid.index;
  co_await _sm->push(rsm_task{.entries = std::move(entries)});
  _pushed_index = last_index;
}

future<> node::commit_raft_update(const protocol::update& up) {
  _peer->commit(up);
  return make_ready_future<>();
}

future<> node::process_raft_update(protocol::update& up) {
  _log_reader.apply_entries(up.entries_to_save);
  co_await send_messages(up.messages);
  co_await remove_log();
  // on disk sm run sync task
  if (save_snapshot_required(up.last_applied)) {
    co_await _sm->push(rsm_task{.save = true});
  }
}

future<> node::process_dropped_entries(const protocol::update& up) {
  for (const auto& e : up.dropped_entries) {
    if (e.is_proposal()) {
      _pending_proposal.drop(e.key);
    } else if (e.type == entry_type::config_change) {
      _pending_config_change.drop(e.key);
    } else {
      return make_exception_future<>(util::failed_precondition_error(
          fmt::format("{}: unknown entry type:{}", id(), e.type)));
    }
  }
  return make_ready_future<>();
}

future<> node::process_dropped_read_indexes(const protocol::update& up) {
  for (auto hint : up.dropped_read_indexes) {
    _pending_read_index.drop(hint);
  }
  return make_ready_future<>();
}

future<> node::process_ready_to_read(const protocol::update& up) {
  if (!up.ready_to_reads.empty()) {
    _pending_read_index.add_ready(up.ready_to_reads);
    _pending_read_index.apply(up.last_applied);
  }
  return make_ready_future<>();
}

future<> node::process_snapshot(const protocol::update& up) {
  if (up.snapshot && !up.snapshot->empty()) {
    _log_reader.apply_snapshot(up.snapshot);
    auto ssi = up.snapshot->log_id.index;
    if (ssi < _pushed_index || ssi < _snapshot_state.snapshot_index ||
        ssi < up.last_applied) {
      l.error(
          "{}: out of date snapshot, index:{}, pushed:{}, applied:{}",
          id(),
          ssi,
          _pushed_index,
          _snapshot_state.snapshot_index);
      co_await coroutine::return_exception(
          util::failed_precondition_error("invalid snapshot"));
    }
    co_await _sm->push(rsm_task{.index = ssi, .recover = true});
    _snapshot_state.snapshot_index = ssi;
    _pushed_index = ssi;
    //    _engine.apply_ready(_config.cluster_id);
  }
}

bool node::is_busy_snapshotting() const {
  bool snapshotting = _snapshot_state.recovering || _snapshot_state.saving;
  return snapshotting && _sm->busy();
}

future<> node::handle_snapshot_task(protocol::rsm_task task) {
  if (_snapshot_state.recovering) {
    l.error("{}: recovering again", id());
    co_await coroutine::return_exception(
        util::failed_precondition_error("TODO"));
  }
  if (task.recover) {
    report_recover_snapshot(std::move(task));
  } else if (task.save) {
    if (_snapshot_state.saving) {
      l.warn("{}: taking snapshot, ignored new snapshot request", id());
      report_ignored_snapshot_request(task.ss_request.key);
      co_return;
    }
    report_save_snapshot(std::move(task));
  } else if (task.stream) {
    // TODO(jyc): support stream snapshot
    report_stream_snapshot(std::move(task));
  } else {
    l.error("{}: unknown task type", id());
  }
  co_return;
}

bool node::process_status_transition() {
  if (process_save_status()) {
    return true;
  }
  if (process_stream_status()) {
    return true;
  }
  if (process_recover_status()) {
    return true;
  }
  if (process_unintialized_status()) {
    return true;
  }
  return false;
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

future<> node::remove_log() {
  auto compact_to = _snapshot_state.compact_log_to;
  if (compact_to == 0) {
    co_return;
  }
  co_await _log_reader.apply_compaction(compact_to)
      .handle_exception_type([](util::compacted_error& err) {
        (void)err;  // compacted is ok
      });
  co_await _logdb.remove(id(), compact_to);
  l.debug("{}: compact log up to index:{}", id(), compact_to);
  _snapshot_state.compacted_to = compact_to;
}

future<> node::compact_log(const rsm_task& task, uint64_t index) {
  if (task.ss_request.compaction_overhead > 0 &&
      index > task.ss_request.compaction_overhead) {
    _snapshot_state.compact_log_to =
        index - task.ss_request.compaction_overhead;
  }
  return _snapshotter->compact(index);
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
              co_await tick();
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
    auto copy = log_entry::share(_pending_proposal._proposal_queue);
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

bool node::process_save_status() {
  if (_snapshot_state.saving) {
    if (!_snapshot_state.save_completed.has_value()) {
      return true;  // TODO(jyc): concurrent snapshot
    }
    if (_snapshot_state.save_completed->save && !_initialized) {
      throw util::panic("taking snapshot when uninitialized");
    }
    _snapshot_state.save_completed.reset();
    _snapshot_state.saving = false;
  }
  return false;
}

bool node::process_stream_status() {
  if (_snapshot_state.streaming) {
    if (true /*TODO(jyc): on disk state machine*/) {
      throw util::panic("non-on disk statemachine is streaming snapshot");
    }
    if (!_snapshot_state.stream_completed.has_value()) {
      return false;
    }
    _snapshot_state.stream_completed.reset();
    _snapshot_state.streaming = false;
  }
  return false;
}

bool node::process_recover_status() {
  if (_snapshot_state.recovering) {
    if (!_snapshot_state.recover_completed.has_value()) {
      return true;
    }
    if (_snapshot_state.recover_completed->save) {
      throw util::panic("completed");
    }
    if (_snapshot_state.recover_completed->initial) {
      // TODO(jyc): set initial state
    }
    _snapshot_state.recover_completed.reset();
    _snapshot_state.recovering = false;
  }
  return false;
}

bool node::process_unintialized_status() {
  if (!_initialized) {
    l.debug("{}: checking initial snapshot", id());
    _snapshot_state.recovering = true;
    report_recover_snapshot(
        {.new_node = _new_node, .recover = true, .initial = true});
    return true;
  }
  return false;
}

future<> node::send_enter_quiesce_messages() {
  // the members should not change during loop sending
  const auto& members = _sm->get_membership();
  for (auto& [node_id, _] : members.addresses) {
    if (_config.node_id != node_id) {
      co_await _send(message{
          .type = message_type::quiesce,
          .cluster = _config.cluster_id,
          .from = _config.node_id,
          .to = node_id});
    }
  }
  co_return;
}

future<> node::send_replicate_messages(protocol::message_vector& msgs) {
  for (auto& msg : msgs) {
    if (msg.type == message_type::replicate) {
      co_await _send(std::move(msg));
    }
  }
  co_return;
}

future<> node::send_messages(protocol::message_vector& msgs) {
  for (auto& msg : msgs) {
    if (msg.type != message_type::replicate) {
      co_await _send(std::move(msg));
    }
  }
  co_return;
}

void node::node_ready() { _nodehost.node_ready(id().cluster); }

void node::gc() {
  if (_gc_tick != _current_tick) {
    _pending_proposal.gc();
    _pending_config_change.gc();
    _pending_snapshot.gc();
    _gc_tick = _current_tick;
  }
}

future<> node::tick() {
  _current_tick++;
  _quiesce.tick();
  if (_quiesce.quiesced()) {
    co_await _peer->quiesced_tick();
  } else {
    co_await _peer->tick();
  }
  _pending_proposal.tick();
  _pending_read_index.tick();
  _pending_config_change.tick();
  _pending_snapshot.tick();
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

bool node::save_snapshot_required(uint64_t applied) {
  auto interval = _config.snapshot_interval;
  if (interval == 0) {
    return false;
  }
  auto si = _snapshot_state.snapshot_index;
  if (_pushed_index <= interval + si || applied <= interval + si ||
      applied <= interval + _snapshot_state.request_snapshot_index) {
    return false;
  }
  if (is_busy_snapshotting()) {
    return false;
  }
  l.debug("{}: requested to create snapshot, applied:{}", id(), applied);
  _snapshot_state.request_snapshot_index = applied;
  return true;
}

future<std::optional<update>> node::get_update() {
  bool not_busy = !_sm->busy();
  if (_peer->has_update(not_busy) || _confirmed_index != _applied_index ||
      _snapshot_state.compact_log_to > 0 || _snapshot_state.compacted_to > 0) {
    if (_applied_index < _confirmed_index) [[unlikely]] {
      l.error(
          "{}: applied index moving backward from {} to {}",
          id(),
          _confirmed_index,
          _applied_index);
      co_await coroutine::return_exception(util::failed_precondition_error());
    }
    auto update = co_await _peer->get_update(not_busy, _applied_index);
    _confirmed_index = _applied_index;
    co_return std::optional<protocol::update>{std::move(update)};
  }
  co_return std::nullopt;
}

void node::report_ignored_snapshot_request(uint64_t key) {
  _pending_snapshot.apply(key, true, false, log_id::INVALID_INDEX);
}

void node::report_save_snapshot(protocol::rsm_task task) {
  _snapshot_state.saving = true;
  _snapshot_state.save_ready = std::move(task);
  //  _engine.save_ready(_config.cluster_id);
}

void node::report_stream_snapshot(protocol::rsm_task task) {
  _snapshot_state.streaming = true;
  _snapshot_state.stream_ready = std::move(task);
  //  _engine.stream_ready(_config.cluster_id);
}

void node::report_recover_snapshot(protocol::rsm_task task) {
  _snapshot_state.recovering = true;
  _snapshot_state.recover_ready = std::move(task);
  //  _engine.recover_ready(_config.cluster_id);
}

}  // namespace rafter
