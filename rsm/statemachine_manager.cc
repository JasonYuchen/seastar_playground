//
// Created by jason on 2022/4/12.
//

#include "statemachine_manager.hh"

#include <seastar/core/coroutine.hh>

#include "protocol/serializer.hh"
#include "rafter/config.hh"
#include "rafter/node.hh"
#include "rsm/logger.hh"
#include "rsm/session_manager.hh"
#include "rsm/snapshotter.hh"

namespace rafter::rsm {

using namespace protocol;

statemachine_manager::statemachine_manager(
    node& node, snapshotter& snapshotter, statemachine::factory factory)
  : _node(node)
  , _snapshotter(snapshotter)
  , _factory(std::move(factory))
  , _sessions(std::make_unique<session_manager>())
  , _applier(
        fmt::format("{}-apply-queue", node.id()),
        config::shard().task_queue_capacity,
        l)
  , _members(_node.id(), true /* FIXME */)
  , _snapshot_index(log_id::INVALID_INDEX) {}

future<> statemachine_manager::start() {
  auto sm = co_await _factory(_node.id());
  _managed = make_unique<managed>(_stopped, _node.is_witness(), std::move(sm));
  _applier.start(
      [this](auto& tasks, bool& open) { return handle(tasks, open); });
  _stopped = false;
}

future<> statemachine_manager::stop() {
  if (!_stopped) {
    _stopped = true;
    co_await _applier.close();
    co_await _managed->close();
  }
}

future<> statemachine_manager::push(rsm_task task) {
  if (busy()) {
    // TODO(jyc): set this rsm busy to limit further tasks
  }
  return _applier.push_eventually(std::move(task));
}

bool statemachine_manager::busy() const {
  // TODO(jyc): busy threshold
  return _applier.size() * 2 >= config::shard().task_queue_capacity;
}

future<rsm_result> statemachine_manager::lookup(std::string_view cmd) {
  return with_shared(_mtx, [=] { return _managed->lookup(cmd); });
}

future<> statemachine_manager::sync() {
  // TODO(jyc): support on disk sm
  return make_ready_future<>();
}

future<server::snapshot> statemachine_manager::save(
    const snapshot_request& request) {
  // TODO(jyc): stream, witness, concurrent
  return with_shared(_mtx, [this, &request]() mutable {
    return prepare(request).then(
        [=](snapshot_metadata meta) { return do_save(std::move(meta)); });
  });
}

future<uint64_t> statemachine_manager::recover(const protocol::rsm_task& task) {
  auto ss = co_await _snapshotter.get_snapshot();
  if (!ss) {
    co_return log_id::INVALID_INDEX;  // TODO(jyc): switch to other error?
  }
  l.debug("{}: recover from snapshot, index:{}", _node.id(), ss->log_id.index);
  co_await do_recover(ss, task.initial);
  apply(*ss, task.initial);
  co_await _node.restore_remotes(ss);
  co_return ss->log_id.index;
}

future<> statemachine_manager::handle(
    std::vector<rsm_task>& tasks, bool& open) {
  for (auto& task : tasks) {
    if (task.save) {
      co_await handle_save(std::move(task));
      continue;
    }
    if (task.recover) {
      co_await handle_recover(std::move(task));
      continue;
    }
    for (const auto& e : task.entries) {
      if (e.type == entry_type::config_change) {
        co_await handle_config_change(e);
      } else {
        // TODO(jyc): figure out the last entry
        co_await handle_entry(e, true);
      }
    }
    set_last_applied(task.entries);
  }
  // give the node a change to run when state machine complete a bunch of tasks
  _node.node_ready();
}

future<> statemachine_manager::handle_entry(
    const protocol::log_entry& entry, bool last) {
  // TODO(jyc): support on disk statemachine
  if (!entry.is_session_managed()) {
    if (entry.is_noop()) {
      co_await _node.apply_entry(entry, {}, false, true, last);
    } else {
      co_await coroutine::return_exception(
          util::panic("not session manged, not empty"));
    }
  } else if (entry.is_new_session_request()) {
    bool registered = _sessions->register_client(entry.client_id);
    co_await _node.apply_entry(entry, {}, !registered, false, last);
  } else if (entry.is_end_session_request()) {
    bool unregistered = _sessions->unregister_client(entry.client_id);
    co_await _node.apply_entry(entry, {}, !unregistered, false, last);
  } else {
    co_await handle_update(entry, last);
  }
  set_applied(entry.lid);
}

future<> statemachine_manager::handle_config_change(
    const protocol::log_entry& entry) {
  auto cc = util::read_from_string(
      {entry.payload.get(), entry.payload.size()}, util::type<config_change>());
  bool rejected = !_members.handle(cc, entry.lid.index);
  set_applied(entry.lid);
  co_await _node.apply_config_change(std::move(cc), entry.key, rejected);
}

future<> statemachine_manager::handle_update(
    const protocol::log_entry& entry, bool last) {
  if (entry.is_noop_session()) {
    auto result = co_await _managed->update(
        entry.lid.index, {entry.payload.get(), entry.payload.size()});
    co_await _node.apply_entry(entry, std::move(result), false, false, last);
  } else {
    auto* s = _sessions->registered_client(entry.client_id);
    if (s == nullptr) {
      co_return co_await _node.apply_entry(entry, {}, true, false, last);
    }
    s->clear_to(entry.responded_to);
    if (s->has_responded(entry.series_id)) {
      co_return co_await _node.apply_entry(entry, {}, false, true, last);
    }
    auto response = s->response(entry.series_id);
    if (response.has_value()) {
      co_return co_await _node.apply_entry(
          entry, std::move(response.value()), false, false, last);
    }
    auto result = co_await _managed->update(
        entry.lid.index, {entry.payload.get(), entry.payload.size()});
    s->response(entry.series_id, result);
    co_await _node.apply_entry(entry, std::move(result), false, false, last);
  }
}

future<> statemachine_manager::handle_save(protocol::rsm_task task) {
  if (!task.ss_request.exported() &&
      last_applied_index() <= _node._snapshot_state.snapshot_index) {
    // a snapshot has been pushed to the sm but not applied yet
    // or the snapshot has been applied and there is no further progress
    co_await _node.apply_snapshot(task.ss_request.key, true, false, 0);
    co_return;
  }
  uint64_t index = 0;
  try {
    auto snap = co_await save(task.ss_request);
    l.info(
        "{} saved snapshot with {} and {} files",
        _node.id(),
        snap.ss->log_id,
        snap.ss->files.size());
    co_await _snapshotter.commit(snap.ss, task.ss_request);
    if (task.ss_request.exported()) {
      co_await _node.apply_snapshot(
          task.ss_request.key, false, false, snap.ss->log_id.index);
      co_return;
    }
    _node._log_reader.create_snapshot(snap.ss);
    _node.compact_log(snap.ss->log_id.index);
    _node._snapshot_state.snapshot_index = snap.ss->log_id.index;
    index = snap.ss->log_id.index;
  } catch (util::snapshot_error& e) {
    l.warn("failed to save snapshot due to {}", e);
  }
  if (index == 0) {
    auto ctx = _snapshotter.get_snapshot_context(
        _log_id.index, task.ss_request.exported(), task.ss_request.path);
    // TODO(jyc): must remove remaining tmp dir when snapshotting failed
    co_await ctx.remove_tmp_dir();
  }
  co_await _node.apply_snapshot(task.ss_request.key, index == 0, false, index);
  co_return;
}

future<> statemachine_manager::handle_recover(protocol::rsm_task task) {
  // TODO(jyc): if on disk statemachine
  uint64_t index = 0;
  try {
    index = co_await recover(task);
    l.info("{} recovered from snapshot with index:{}", _node.id(), index);
    _node.compact_log(index);
  } catch (util::snapshot_error& e) {
    l.warn("failed to recover snapshot due to {}", e);
  }
  co_return;
}

future<snapshot_metadata> statemachine_manager::prepare(
    const snapshot_request& req) {
  // TODO(jyc): check snapshot status
  if (_stopped) {
    return make_exception_future<snapshot_metadata>(util::closed_error());
  }
  if (last_applied_index() < _snapshot_index) {
    return make_exception_future<snapshot_metadata>(
        util::panic("last_applied < snapshot_index"));
  }
  if (!req.exported() && last_applied_index() > 0 &&
      last_applied_index() == _snapshot_index) {
    return make_exception_future<snapshot_metadata>(
        util::snapshot_out_of_date());
  }
  return _managed->prepare().then([this, &req](std::any ctx) mutable {
    return make_ready_future<snapshot_metadata>(
        get_snapshot_meta(std::move(ctx), req));
  });
}

future<server::snapshot> statemachine_manager::do_save(snapshot_metadata meta) {
  auto index = meta.lid.index;
  auto ss = co_await _snapshotter.save(
      [this](std::any ctx, output_stream<char>& os, files& fs) -> future<bool> {
        auto session_data = write_to_string(*_sessions);
        size_t size = util::htole(session_data.size());
        co_await os.write(reinterpret_cast<const char*>(&size), sizeof(size));
        co_await os.write(session_data);
        co_return co_await _managed->save(std::move(ctx), os, fs);
      },
      std::move(meta));
  _snapshot_index = index;
  co_return ss;
}

future<> statemachine_manager::do_recover(snapshot_ptr ss, bool init) {
  if (last_applied_index() >= ss->log_id.index) {
    return make_exception_future<>(util::snapshot_out_of_date());
  }
  if (_stopped) {
    return make_exception_future<>(util::closed_error());
  }
  // TODO(jyc): on disk statemachine is shrunk
  return _snapshotter.load(
      [this](input_stream<char>& is, const snapshot_files& fs) -> future<> {
        auto session_data =
            co_await is.read_exactly(sizeof(size_t))
                .then([&is](temporary_buffer<char> size) {
                  return is.read_exactly(util::read_le<size_t>(size.get()));
                });
        if (!_sessions) {
          _sessions = std::make_unique<session_manager>();
        }
        *_sessions = read_from_string(
            {session_data.get(), session_data.size()},
            util::type<session_manager>());
        co_await _managed->recover(is, fs);
      },
      std::move(ss));
}

snapshot_metadata statemachine_manager::get_snapshot_meta(
    std::any ctx, const snapshot_request& req) {
  if (_members.empty()) {
    throw util::panic("empty membership");
  }
  const auto& m = _members.get();
  l.debug("{}: snapshot {} has members:{}", _node.id(), _log_id, m.addresses);
  return snapshot_metadata{
      .from = _node.id().node,
      .lid = _log_id,
      .request = req,
      .membership = make_lw_shared<protocol::membership>(m),
      .smtype = state_machine_type::regular,
      .comptype = compression_type::no_compression,
      .ctx = std::move(ctx)};
}
void statemachine_manager::apply(const snapshot& ss, bool init) {
  _members.set(*ss.membership);
  _last_applied = ss.log_id;
  _log_id = ss.log_id;
  l.debug(
      "{}: recover from snapshot {}, init:{}, members:{}, observers:{}, "
      "witnesses:{}",
      _node.id(),
      ss.log_id,
      init,
      ss.membership->addresses,
      ss.membership->observers,
      ss.membership->witnesses);
}

void statemachine_manager::set_last_applied(const log_entry_vector& entries) {
  if (!entries.empty()) {
    // TODO(jyc): various log continuity check
    _last_applied = entries.back().lid;
  }
}

void statemachine_manager::set_applied(protocol::log_id lid) {
  if (_log_id.index + 1 != lid.index) {
    throw util::panic("applied index gap");
  }
  if (_log_id.term > lid.term) {
    throw util::panic("applied smaller term");
  }
  _log_id = lid;
}

future<rsm_result> statemachine_manager::managed::open() { return _sm->open(); }

future<rsm_result> statemachine_manager::managed::update(
    uint64_t index, std::string_view cmd) {
  if (_stopped) {
    return make_exception_future<rsm_result>(util::closed_error("sm"));
  }
  return with_lock(_mtx, [=] { return _sm->update(index, cmd); });
}

future<rsm_result> statemachine_manager::managed::lookup(std::string_view cmd) {
  if (_stopped) {
    return make_exception_future<rsm_result>(util::closed_error("sm"));
  }
  return with_shared(_mtx, [=] { return _sm->lookup(cmd); });
}

future<> statemachine_manager::managed::sync() { return _sm->sync(); }

future<std::any> statemachine_manager::managed::prepare() {
  return _sm->prepare();
}

future<bool> statemachine_manager::managed::save(
    std::any ctx, output_stream<char>& writer, files& fs) {
  if (_witness) {
    return make_ready_future<bool>(true);
  }
  return _sm->save_snapshot(std::move(ctx), writer, fs, _stopped)
      .then([](statemachine::snapshot_status s) {
        if (s != statemachine::snapshot_status::done) {
          return make_exception_future<bool>(util::snapshot_aborted());
        }
        return make_ready_future<bool>(false);
      });
}

future<> statemachine_manager::managed::recover(
    input_stream<char>& reader, const snapshot_files& fs) {
  return _sm->recover_from_snapshot(reader, fs, _stopped)
      .then([](statemachine::snapshot_status s) {
        if (s != statemachine::snapshot_status::done) {
          return make_exception_future<>(util::snapshot_aborted());
        }
        return make_ready_future<>();
      });
}

future<> statemachine_manager::managed::close() { return _sm->close(); }

}  // namespace rafter::rsm
