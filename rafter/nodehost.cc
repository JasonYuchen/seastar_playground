//
// Created by jason on 2021/12/8.
//

#include "nodehost.hh"

#include <seastar/core/coroutine.hh>

#include "rafter/logger.hh"
#include "rafter/node.hh"
#include "rsm/session_manager.hh"
#include "rsm/snapshotter.hh"
#include "server/environment.hh"
#include "util/error.hh"
#include "util/file.hh"

namespace rafter {

using namespace protocol;

// TODO(jyc): refine some APIs' flow to avoid redundant shard/stop check

nodehost::nodehost(
    struct config cfg,
    storage::logdb& logdb,
    transport::registry& registry,
    transport::rpc& rpc)
  : _config(std::move(cfg))
  , _logdb(logdb)
  , _registry(registry)
  , _rpc(rpc)
  , _persister("persister", 100, l)
  , _sender("sender", 100, l)
  , _receiver("receiver", 100, l)
  , _partitioner(server::environment::get_partition_func()) {}

future<> nodehost::start() {
  l.info("nodehost starting...");
  _persister.start(
      [this](std::vector<storage::update_pack>& packs, bool& stopped) {
        return _logdb.save(packs);
      });
  // FIXME(jyc): this starting procedure is just for demo
  auto groups = co_await _logdb.list_nodes();
  for (auto gid : groups) {
    l.info("restarting {}", gid);
    // TODO(jyc): optimize
    std::stringstream is;
    auto dir = std::filesystem::path(_config.data_dir).append("config");
    auto name = fmt::format("{:020d}_{:020d}.yaml", gid.cluster, gid.node);
    if (!co_await rafter::util::exist_file(dir.string(), name)) {
      l.warn("{} config file {}/{} not found", gid, dir.string(), name);
      continue;
    }
    auto content = co_await rafter::util::read_file(dir.string(), name);
    is << std::string_view{content.get(), content.size()};
    auto cfg = raft_config::read_from(is);
    l.info("{}", cfg);
    co_await start_cluster(
        cfg,
        {},
        false,
        state_machine_type::regular,
        /* TODO(jyc): different statemachine */
        std::make_unique<kv_statemachine_factory>());
  }
  _ticker.set_callback([this] {
    if (_stopped) [[unlikely]] {
      return;
    }
    for (auto [cluster_id, node] : _clusters) {
      message m{
          .type = message_type::local_tick,
          .from = node->n->id().node,
          .to = node->n->id().node};
      // FIXME(jyc): tolerate tick message lost?
      bool pushed = node->n->_received_messages.push(m);
      if (!pushed) {
        l.debug("{} missed a tick", node->n->id());
      }
      node_ready(cluster_id);
    }
  });
  _ticker.arm_periodic(std::chrono::milliseconds(_config.rtt_ms));
  initialize_handlers();
}

future<> nodehost::stop() {
  l.info("nodehost:{} stopping...", _id);
  _stopped = true;
  _ticker.cancel();
  auto it = _clusters.begin();
  while (it != _clusters.end()) {
    co_await stop_cluster(it->first);
    it = _clusters.begin();
  }
  co_await _persister.close();
  uninitialize_handlers();
}

future<> nodehost::start_cluster(
    raft_config cfg,
    member_map initial_members,
    bool join,
    state_machine_type type,
    std::unique_ptr<statemachine_factory> factory) {
  if (_stopped) [[unlikely]] {
    co_await coroutine::return_exception(util::closed_error());
  }
  auto gid = group_id{cfg.cluster_id, cfg.node_id};
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    co_return co_await container().invoke_on(
        shard,
        &nodehost::start_cluster,
        cfg,
        std::move(initial_members),
        join,
        type,
        std::move(factory));
  }
  if (_clusters.contains(gid.cluster)) {
    co_await coroutine::return_exception(
        util::invalid_argument("cluster_id", "already exist"));
  }
  if (join && !initial_members.empty()) {
    co_await coroutine::return_exception(
        util::invalid_argument("join", "initial_members not empty"));
  }
  l.trace("nodehost::start_cluster: {}", group_id{cfg.cluster_id, cfg.node_id});
  auto [peers, im] =
      co_await bootstrap_cluster(cfg, initial_members, join, type);
  for (const auto& [id, address] : peers) {
    if (id != gid.node) {
      _registry.update(group_id{gid.cluster, id}, address);
    }
  }
  // TODO(jyc): consider a double-sharding snapshot dir func
  auto snapshot_dir_func =
      server::environment::get_snapshot_dir_func(_config.data_dir);
  auto snapshotter =
      std::make_unique<rsm::snapshotter>(gid, _logdb, snapshot_dir_func);
  co_await snapshotter->process_orphans();
  auto sender = [s = shared_from_this()](message m) {
    return s->send(std::move(m));
  };
  auto snapshot_notifier = [s = shared_from_this()](group_id gid, bool failed) {
    return s->handle_snapshot_status(gid, failed);
  };
  auto n = make_lw_shared<node>(
      cfg,
      container().local(),
      _registry,
      _logdb,
      std::move(snapshotter),
      std::move(factory),
      std::move(sender),
      std::move(snapshot_notifier));
  co_await n->start(peers, im);
  auto r = make_lw_shared<ready>();
  _clusters[gid.cluster] = r;
  r->n = std::move(n);
  r->main = node_main(r->n);
  // TODO(jyc): cluster change ready
  node_ready(gid.cluster);
  auto dir = std::filesystem::path(_config.data_dir).append("config");
  auto name = fmt::format("{:020d}_{:020d}.yaml", gid.cluster, gid.node);
  std::stringstream os;
  cfg.write_to(os);
  co_await rafter::util::create_file(dir.string(), name, os.str());
}

future<> nodehost::stop_cluster(uint64_t cluster_id) {
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::stop_cluster, cluster_id);
  }
  l.trace("nodehost::stop_cluster: cluster_id:{}", cluster_id);
  return stop_node({cluster_id, group_id::INVALID_NODE});
}

future<membership> nodehost::get_membership(uint64_t cluster_id) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<membership>(util::closed_error());
  }
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::get_membership, cluster_id);
  }
  l.trace("nodehost::get_membership: cluster_id:{}", cluster_id);
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<membership>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  return read_index(cluster_id).then([n = it->second](request_result r) {
    if (r.code == request_result::code::completed) {
      return make_ready_future<membership>(n->n->_sm->get_membership());
    }
    l.error("failed to read index, code:{}", r.code);
    return make_exception_future<membership>(util::request_error("read index"));
  });
}

future<uint64_t> nodehost::get_leader(uint64_t cluster_id) {
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::get_leader, cluster_id);
  }
  l.trace("nodehost::get_leader: cluster_id:{}", cluster_id);
  // TODO(jyc): add raft event
  return make_exception_future<uint64_t>(util::panic("not implemented"));
}

future<session> nodehost::get_session(uint64_t cluster_id) {
  if (_stopped) [[unlikely]] {
    co_await coroutine::return_exception(util::closed_error());
  }
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    co_return co_await container().invoke_on(
        shard, &nodehost::get_session, cluster_id);
  }
  l.trace("nodehost::get_session: cluster_id:{}", cluster_id);
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    co_await coroutine::return_exception(
        util::invalid_argument("cluster_id", "not_found"));
  }
  session s{cluster_id};
  s.prepare_for_register();
  auto r = co_await propose_session(s);
  if (r.result.value != s.client_id) [[unlikely]] {
    co_await coroutine::return_exception(util::panic("unexpected result"));
  }
  s.prepare_for_propose();
  co_return s;
}

future<session> nodehost::get_noop_session(uint64_t cluster_id) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<session>(util::closed_error());
  }
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard, &nodehost::get_noop_session, cluster_id);
  }
  l.trace("nodehost::get_noop_session: cluster_id:{}", cluster_id);
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<session>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  return make_ready_future<session>(cluster_id, true);
}

future<> nodehost::close_session(session& s) {
  if (_stopped) [[unlikely]] {
    co_await coroutine::return_exception(util::closed_error());
  }
  auto shard = _partitioner(s.cluster_id);
  if (shard != this_shard_id()) {
    co_return co_await container().invoke_on(
        shard, &nodehost::close_session, std::ref(s));
  }
  l.trace("nodehost::close_session: cluster_id:{}", s.cluster_id);
  auto it = _clusters.find(s.cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    co_await coroutine::return_exception(
        util::invalid_argument("cluster_id", "not_found"));
  }
  if (s.is_noop()) {
    co_return;
  }
  s.prepare_for_unregister();
  auto r = co_await propose_session(s);
  if (r.result.value != s.client_id) [[unlikely]] {
    co_await coroutine::return_exception(util::panic("unexpected result"));
  }
}

future<request_result> nodehost::propose(session& s, std::string_view cmd) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(s.cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::propose, std::ref(s), cmd);
  }
  l.trace("nodehost::propose: cluster_id:{} cmd:{}", s.cluster_id, cmd);
  auto it = _clusters.find(s.cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->n->propose(s, cmd, 86400000);
  node_ready(s.cluster_id);
  return r;
}

future<request_result> nodehost::propose_session(session& s) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(s.cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard, &nodehost::propose_session, std::ref(s));
  }
  l.trace("nodehost::propose_session: cluster_id:{}", s.cluster_id);
  auto it = _clusters.find(s.cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  if (it->second->n->is_witness()) {
    return make_exception_future<request_result>(
        util::invalid_argument("witness", "invalid operation on witness"));
  }
  auto r = it->second->n->propose_session(s, UINT64_MAX);
  node_ready(s.cluster_id);
  return r;
}

future<request_result> nodehost::read_index(uint64_t cluster_id) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::read_index, cluster_id);
  }
  l.trace("nodehost::read_index: cluster_id:{}", cluster_id);
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto ret = it->second->n->read(86400000);
  node_ready(cluster_id);
  return ret;
}

future<request_result> nodehost::linearizable_read(
    uint64_t cluster_id, std::string_view query) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard, &nodehost::linearizable_read, cluster_id, query);
  }
  l.trace(
      "nodehost::linearizable_read: cluster_id:{} query:{}", cluster_id, query);
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  return read_index(cluster_id).then([query, n = it->second](request_result r) {
    if (r.code == request_result::code::completed) {
      return n->n->_sm->lookup(query).then([](rsm_result result) {
        return make_ready_future<request_result>(
            request_result{request_result::code::completed, std::move(result)});
      });
    }
    return make_ready_future<request_result>(std::move(r));
  });
}

future<request_result> nodehost::stale_read(
    uint64_t cluster_id, std::string_view query) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard, &nodehost::stale_read, cluster_id, query);
  }
  l.trace("nodehost::stale_read: cluster_id:{} query:{}", cluster_id, query);
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  if (it->second->n->is_witness()) {
    return make_exception_future<request_result>(
        util::invalid_argument("witness", "invalid operation on witness"));
  }
  return it->second->n->_sm->lookup(query).then([](rsm_result r) {
    return make_ready_future<request_result>(
        request_result{request_result::code::completed, std::move(r)});
  });
}

future<request_result> nodehost::request_snapshot(
    uint64_t cluster_id, const snapshot_option& option) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard, &nodehost::request_snapshot, cluster_id, std::cref(option));
  }
  l.trace("nodehost::reqeust_snapshot: cluster_id:{}", cluster_id);
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->n->request_snapshot(option, UINT64_MAX);
  node_ready(cluster_id);
  return r;
}

future<request_result> nodehost::request_compaction(group_id gid) {
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::request_compaction, gid);
  }
  l.trace("nodehost::request_compaction: {}", gid);
  return make_exception_future<request_result>(util::panic("not implemented"));
}

future<request_result> nodehost::request_add_node(
    group_id gid, std::string target, uint64_t config_change_index) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard, &nodehost::request_add_node, gid, target, config_change_index);
  }
  l.trace("nodehost::request_add_node: {}", gid);
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->n->request_config_change(
      config_change{
          .config_change_id = config_change_index,
          .type = config_change_type::add_node,
          .node = gid.node,
          .address = target},
      UINT64_MAX);
  node_ready(gid.cluster);
  return r;
}

future<request_result> nodehost::request_add_observer(
    group_id gid, std::string target, uint64_t config_change_index) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard,
        &nodehost::request_add_observer,
        gid,
        target,
        config_change_index);
  }
  l.trace("nodehost::request_add_observer: {}", gid);
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->n->request_config_change(
      config_change{
          .config_change_id = config_change_index,
          .type = config_change_type::add_observer,
          .node = gid.node,
          .address = target},
      UINT64_MAX);
  node_ready(gid.cluster);
  return r;
}

future<request_result> nodehost::request_add_witness(
    group_id gid, std::string target, uint64_t config_change_index) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard,
        &nodehost::request_add_witness,
        gid,
        target,
        config_change_index);
  }
  l.trace("nodehost::request_add_witness: {}", gid);
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->n->request_config_change(
      config_change{
          .config_change_id = config_change_index,
          .type = config_change_type::add_witness,
          .node = gid.node,
          .address = target},
      UINT64_MAX);
  node_ready(gid.cluster);
  return r;
}

future<request_result> nodehost::request_delete_node(
    group_id gid, uint64_t config_change_index) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard, &nodehost::request_delete_node, gid, config_change_index);
  }
  l.trace("nodehost::request_delete_node: {}", gid);
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->n->request_config_change(
      config_change{
          .config_change_id = config_change_index,
          .type = config_change_type::remove_node,
          .node = gid.node},
      UINT64_MAX);
  node_ready(gid.cluster);
  return r;
}

future<request_result> nodehost::request_leader_transfer(group_id gid) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<request_result>(util::closed_error());
  }
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(
        shard, &nodehost::request_leader_transfer, gid);
  }
  l.trace("nodehost::request_leader_transfer: {}", gid);
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->n->request_leader_transfer(gid.node);
  node_ready(gid.cluster);
  // TODO(jyc): when to apply the leader transfer?
  return r;
}

future<request_result> nodehost::request_remove_data(group_id gid) {
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::request_remove_data, gid);
  }
  l.trace("nodehost::request_remove_data: {}", gid);
  return make_exception_future<request_result>(util::panic("not implemented"));
}

void nodehost::initialize_handlers() {
  _rpc.register_unreachable_handler(
      [this](auto gid) { return handle_unreachable(gid); });
  _rpc.register_message_handler(
      [this](auto m) { return handle_message(std::move(m)); });
  _rpc.register_snapshot_handler(
      [this](auto gid, auto from) { return handle_snapshot(gid, from); });
  _rpc.register_snapshot_status_handler([this](auto gid, auto failed) {
    return handle_snapshot_status(gid, failed);
  });
}

void nodehost::uninitialize_handlers() {
  _rpc.register_unreachable_handler(
      [](auto gid) { return make_ready_future<>(); });
  _rpc.register_message_handler([](auto m) { return make_ready_future<>(); });
  _rpc.register_snapshot_handler(
      [](auto gid, auto from) { return make_ready_future<>(); });
  _rpc.register_snapshot_status_handler(
      [](auto gid, auto failed) { return make_ready_future<>(); });
}

void nodehost::node_ready(uint64_t cluster_id) {
  if (auto it = _clusters.find(cluster_id); it != _clusters.end()) {
    if (it->second->event == 0 && it->second->gate.has_value()) {
      it->second->gate->set_value();
    }
    it->second->event = 1;
  }
}

future<> nodehost::stop_node(group_id gid) {
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto ready_node = it->second;
  auto ret = ready_node->n->stop().then([ready_node, s = shared_from_this()] {
    return ready_node->main.value().then(
        [ready_node, s] { s->_clusters.erase(ready_node->n->id().cluster); });
  });
  node_ready(gid.cluster);
  return ret;
}

future<> nodehost::node_main(lw_shared_ptr<node> n) {
  auto self = shared_from_this();
  while (!n->_stopped) {
    {
      assert(_clusters.contains(n->id().cluster));
      auto ready_node = _clusters.find(n->id().cluster)->second;
      if (ready_node->event == 0) {
        ready_node->gate = promise<>();
        co_await ready_node->gate->get_future();
        ready_node->gate.reset();
      }
      ready_node->event = 0;
    }
    if (n->_stopped) {
      break;
    }
    auto up = co_await n->step();
    if (!up.has_value()) {
      continue;
    }
    if (up->fast_apply) {
      co_await n->process_snapshot(*up);
      co_await n->apply_raft_update(*up);
      node_ready(n->id().cluster);
    }
    co_await n->send_replicate_messages(up->messages);
    co_await n->process_ready_to_read(*up);
    co_await n->process_dropped_entries(*up);
    co_await n->process_dropped_read_indexes(*up);
    // TODO(jyc): add `bool update::need_persistent() const;`
    if (up->snapshot || !up->entries_to_save.empty() || !up->state.empty()) {
      storage::update_pack pack{*up};
      auto fut = pack.done.get_future();
      co_await _persister.push_eventually(std::move(pack));
      co_await fut.discard_result();
    }
    // TODO(jyc): co_await engine.onSnapshotSaved
    if (!up->fast_apply) {
      co_await n->process_snapshot(*up);
      co_await n->apply_raft_update(*up);
      node_ready(n->id().cluster);
    }
    co_await n->process_raft_update(*up);
    if (up->has_more_committed_entries) {
      node_ready(n->id().cluster);
    }
    co_await n->commit_raft_update(*up);
  }
}

future<> nodehost::handle_unreachable(group_id gid) {
  l.debug("unreachable called on {}", gid);
  if (_stopped) [[unlikely]] {
    return make_exception_future<>(util::closed_error());
  }
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    (void)container().invoke_on(shard, &nodehost::handle_unreachable, gid);
    return make_ready_future<>();
  }
  if (auto it = _clusters.find(gid.cluster); it != _clusters.end()) {
    (void)it->second->n->_received_messages
        .push_eventually(message{
            .type = message_type::unreachable,
            .from = gid.node,
            .to = it->second->n->id().node})
        .handle_exception([](std::exception_ptr ex) {
          l.error("failed to push unreachable to node: {}", ex);
        });
    node_ready(gid.cluster);
  }
  return make_ready_future<>();
}

future<> nodehost::handle_message(message m) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<>(util::closed_error());
  }
  if (m.to == group_id::INVALID_NODE) [[unlikely]] {
    return make_exception_future<>(util::panic("invalid to"));
  }
  auto shard = _partitioner(m.cluster);
  if (shard != this_shard_id()) {
    (void)container().invoke_on(shard, &nodehost::handle_message, std::move(m));
    return make_ready_future<>();
  }
  if (auto it = _clusters.find(m.cluster); it != _clusters.end()) {
    if (it->second->n->id().node != m.to) {
      l.warn(
          "nodehost::handle_message: ignored a {} message sent to {} but "
          "received by {}",
          m.type,
          group_id{m.cluster, m.to},
          group_id{m.cluster, it->second->n->id().node});
      return make_ready_future<>();
    }
    // TODO(jyc): since we are blindly push_eventually for snapshot related
    //  messages, shall we check for the number of existing awaiters?
    if (m.type == message_type::install_snapshot) {
      (void)it->second->n->_received_messages.push_eventually(std::move(m))
          .handle_exception([](std::exception_ptr ex) {
            l.error("failed to push install snapshot to node: {}", ex);
          });
    } else if (m.type == message_type::snapshot_received) {
      (void)it->second->n->_received_messages
          .push_eventually(
              message{.type = message_type::snapshot_status, .from = m.from})
          .handle_exception([](std::exception_ptr ex) {
            l.error("failed to push snapshot status to node: {}", ex);
          });
    } else {
      if (!it->second->n->_received_messages.push(m)) {
        l.warn("nodehost::handle_message: dropped a {} message", m.type);
      }
    }
    node_ready(m.cluster);
  }
  return make_ready_future<>();
}

future<> nodehost::handle_snapshot(group_id gid, uint64_t from) {
  l.debug("snapshot called on {}, from:{}", gid, from);
  if (_stopped) [[unlikely]] {
    return make_exception_future<>(util::closed_error());
  }
  (void)send(message{
      .type = message_type::snapshot_received,
      .cluster = gid.cluster,
      .from = gid.node,
      .to = from});
  // TODO(jyc): event
  return make_ready_future<>();
}

future<> nodehost::handle_snapshot_status(group_id gid, bool failed) {
  l.debug("snapshot status called on {}, failed:{}", gid, failed);
  if (_stopped) [[unlikely]] {
    return make_exception_future<>(util::closed_error());
  }
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    (void)container().invoke_on(
        shard, &nodehost::handle_snapshot_status, gid, failed);
    return make_ready_future<>();
  }
  if (auto it = _clusters.find(gid.cluster); it != _clusters.end()) {
    // TODO(jyc): delay snapshot status
    (void)it->second->n->_received_messages
        .push_eventually(message{
            .type = message_type::snapshot_status,
            .from = gid.node,
            .reject = failed})
        .handle_exception([](std::exception_ptr ex) {
          l.error("failed to push snapshot status to node: {}", ex);
        });
    node_ready(gid.cluster);
  }
  return make_ready_future<>();
}

future<> nodehost::send(protocol::message m) {
  l.debug("nodehost::send message type {}", m.type);
  if (_stopped) [[unlikely]] {
    return make_exception_future<>(util::closed_error());
  }
  if (m.type != message_type::install_snapshot) {
    (void)_rpc.send_message(std::move(m))
        .handle_exception([](std::exception_ptr ex) {
          l.error("nodehost::send: send message exception:{}", ex);
        });
  } else {
    bool witness = m.snapshot->witness;
    l.info(
        "nodehost::send snapshot from:{} to:{}, {}",
        group_id{m.cluster, m.from},
        group_id{m.cluster, m.to},
        m.snapshot->log_id);
    if (auto it = _clusters.find(m.cluster); it != _clusters.end()) {
      if (witness /*on disk statemachine*/) {
        (void)_rpc.send_snapshot(std::move(m))
            .handle_exception([](std::exception_ptr ex) {
              l.error("nodehost::send: send snapshot exception:{}", ex);
            });
      } else {
        // push stream snapshot
      }
    }
    // TODO(jyc): event
  }
  return make_ready_future<>();
}

future<std::pair<member_map, bool>> nodehost::bootstrap_cluster(
    const raft_config& cfg,
    const member_map& initial_members,
    bool join,
    state_machine_type type) {
  auto info = co_await _logdb.load_bootstrap({cfg.cluster_id, cfg.node_id});
  if (!info.has_value()) {
    if (!join && initial_members.empty()) {
      co_await coroutine::return_exception(util::panic("cluster not booted"));
    }
    member_map members;
    if (!join) {
      members = initial_members;
    }
    // TODO(jyc): clean addresses in initial_members
    info = {.addresses = initial_members, .join = join, .smtype = type};
    co_await _logdb.save_bootstrap({cfg.cluster_id, cfg.node_id}, *info);
    co_return std::pair{std::move(members), !join};
  }
  // TODO(jyc): validate the given information and corresponding bootstrap
  co_return std::pair{std::move(info->addresses), !info->join};
}

}  // namespace rafter
