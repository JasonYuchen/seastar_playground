//
// Created by jason on 2021/12/8.
//

#include "nodehost.hh"

#include <seastar/core/coroutine.hh>

#include "rafter/engine.hh"
#include "rafter/logger.hh"
#include "rafter/node.hh"
#include "rsm/session_manager.hh"
#include "rsm/snapshotter.hh"
#include "server/environment.hh"
#include "util/error.hh"

namespace rafter {

using namespace protocol;

// TODO(jyc): refine some APIs' flow to avoid redundant shard/stop check

nodehost::nodehost(
    struct config cfg,
    engine& ng,
    storage::logdb& logdb,
    transport::registry& registry,
    transport::rpc& rpc)
  : _config(std::move(cfg))
  , _engine(ng)
  , _logdb(logdb)
  , _registry(registry)
  , _rpc(rpc)
  , _sender("sender", 100, l)
  , _receiver("receiver", 100, l)
  , _partitioner(server::environment::get_partition_func()) {}

future<> nodehost::start() {
  // FIXME(jyc): this starting procedure is just for demo
  auto groups = co_await _logdb.list_nodes();
  for (auto gid : groups) {
    l.info("restarting {}", gid);
    co_await start_cluster(
        /* TODO(jyc): save the raft config somewhere ? say bootstrap, hard code
         *  for now */
        raft_config{
            .cluster_id = gid.cluster,
            .node_id = gid.node,
            .election_rtt = 50,
            .heartbeat_rtt = 5,
            .snapshot_interval = 10,
        },
        {},
        false,
        state_machine_type::regular,
        /* TODO(jyc): different statemachine */
        [](group_id gid) {
          return make_ready_future<std::unique_ptr<statemachine>>(
              new kv_statemachine(gid));
        });
  }
  _ticker.set_callback([self = shared_from_this()] {
    if (self->_stopped) [[unlikely]] {
      return;
    }
    for (auto [cluster_id, node] : self->_clusters) {
      message m{
          .type = message_type::local_tick,
          .from = node->id().node,
          .to = node->id().node};
      // FIXME(jyc): tolerate tick message lost?
      bool pushed = node->_received_messages.push(m);
      if (!pushed) {
        l.debug("{} missed a tick", node->id());
      }
      self->_engine.step_ready(cluster_id);
    }
  });
  _ticker.arm_periodic(std::chrono::milliseconds(_config.rtt_ms));
  throw util::panic("not implemented");
}

future<> nodehost::stop() {
  _ticker.cancel();
  return make_exception_future<>(util::panic("not implemented"));
}

future<> nodehost::start_cluster(
    raft_config cfg,
    member_map initial_members,
    bool join,
    state_machine_type type,
    statemachine::factory&& factory) {
  if (_stopped) [[unlikely]] {
    throw util::closed_error();
  }
  auto gid = group_id{cfg.cluster_id, cfg.node_id};
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    co_return co_await container().invoke_on(
        shard,
        &nodehost::start_cluster,
        std::move(cfg),
        std::move(initial_members),
        join,
        type,
        std::move(factory));
  }
  if (_clusters.contains(gid.cluster)) {
    throw util::invalid_argument("cluster_id", "already exist");
  }
  if (join && !initial_members.empty()) {
    throw util::invalid_argument("join", "initial_members not empty");
  }
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
      std::move(cfg),
      _engine,
      _registry,
      _logdb,
      std::move(snapshotter),
      std::move(factory),
      std::move(sender),
      std::move(snapshot_notifier));
  co_await n->start(peers, im);
  _clusters[gid.cluster] = std::move(n);
  // TODO(jyc): cluster change ready
  _engine.apply_ready(gid.cluster);
}

future<> nodehost::stop_cluster(uint64_t cluster_id) {
  return stop_node({cluster_id, group_id::INVALID_NODE});
}

future<> nodehost::stop_node(group_id gid) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<>(util::closed_error());
  }
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::stop_node, gid);
  }
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  lw_shared_ptr<node> n{it->second};
  _clusters.erase(it);
  auto ret = n->stop();
  // TODO(jyc): cluster change ready
  _engine.step_ready(gid.cluster);
  _engine.commit_ready(gid.cluster);
  _engine.apply_ready(gid.cluster);
  _engine.recover_ready(gid.cluster);
  return ret;
}

future<membership> nodehost::get_membership(uint64_t cluster_id) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<membership>(util::closed_error());
  }
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::get_membership, cluster_id);
  }
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<membership>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  return read_index(cluster_id).then([n = it->second](request_result r) {
    if (r.code == request_result::code::completed) {
      return make_ready_future<membership>(n->_sm->get_membership());
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
  // TODO(jyc): add raft event
  return make_exception_future<uint64_t>(util::panic("not implemented"));
}

future<session> nodehost::get_session(uint64_t cluster_id) {
  if (_stopped) [[unlikely]] {
    co_return coroutine::make_exception(util::closed_error());
  }
  auto shard = _partitioner(cluster_id);
  if (shard != this_shard_id()) {
    co_return co_await container().invoke_on(
        shard, &nodehost::get_session, cluster_id);
  }
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    co_return coroutine::make_exception(
        util::invalid_argument("cluster_id", "not_found"));
  }
  session s{cluster_id};
  s.prepare_for_register();
  auto r = co_await propose_session(s);
  if (r.result.value != s.client_id) [[unlikely]] {
    co_return coroutine::make_exception(util::panic("unexpected result"));
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
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<session>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  return make_ready_future<session>(cluster_id, true);
}

future<> nodehost::close_session(session& s) {
  if (_stopped) [[unlikely]] {
    throw util::closed_error();
  }
  auto shard = _partitioner(s.cluster_id);
  if (shard != this_shard_id()) {
    co_return co_await container().invoke_on(
        shard, &nodehost::close_session, std::ref(s));
  }
  auto it = _clusters.find(s.cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    throw util::invalid_argument("cluster_id", "not_found");
  }
  if (s.is_noop()) {
    co_return;
  }
  s.prepare_for_unregister();
  auto r = co_await propose_session(s);
  if (r.result.value != s.client_id) [[unlikely]] {
    throw util::panic("unexpected result");
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
  auto it = _clusters.find(s.cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->propose(s, cmd, UINT64_MAX);
  _engine.step_ready(s.cluster_id);
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
  auto it = _clusters.find(s.cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  if (it->second->is_witness()) {
    return make_exception_future<request_result>(
        util::invalid_argument("witness", "invalid operation on witness"));
  }
  auto r = it->second->propose_session(s, UINT64_MAX);
  _engine.step_ready(s.cluster_id);
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
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto ret = it->second->read(UINT64_MAX);
  _engine.step_ready(cluster_id);
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
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  return read_index(cluster_id).then([query, n = it->second](request_result r) {
    if (r.code == request_result::code::completed) {
      return n->_sm->lookup(query).then([](rsm_result result) {
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
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  if (it->second->is_witness()) {
    return make_exception_future<request_result>(
        util::invalid_argument("witness", "invalid operation on witness"));
  }
  return it->second->_sm->lookup(query).then([](rsm_result r) {
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
  auto it = _clusters.find(cluster_id);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->request_snapshot(option, UINT64_MAX);
  _engine.step_ready(cluster_id);
  return r;
}

future<request_result> nodehost::request_compaction(group_id gid) {
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::request_compaction, gid);
  }
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
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->request_config_change(
      config_change{
          .config_change_id = config_change_index,
          .type = config_change_type::add_node,
          .node = gid.node,
          .address = target},
      UINT64_MAX);
  _engine.step_ready(gid.cluster);
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
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->request_config_change(
      config_change{
          .config_change_id = config_change_index,
          .type = config_change_type::add_observer,
          .node = gid.node,
          .address = target},
      UINT64_MAX);
  _engine.step_ready(gid.cluster);
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
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->request_config_change(
      config_change{
          .config_change_id = config_change_index,
          .type = config_change_type::add_witness,
          .node = gid.node,
          .address = target},
      UINT64_MAX);
  _engine.step_ready(gid.cluster);
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
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->request_config_change(
      config_change{
          .config_change_id = config_change_index,
          .type = config_change_type::remove_node,
          .node = gid.node},
      UINT64_MAX);
  _engine.step_ready(gid.cluster);
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
  auto it = _clusters.find(gid.cluster);
  if (it == _clusters.end()) [[unlikely]] {
    return make_exception_future<request_result>(
        util::invalid_argument("cluster_id", "not_found"));
  }
  auto r = it->second->request_leader_transfer(gid.node);
  _engine.step_ready(gid.cluster);
  // TODO(jyc): when to apply the leader transfer?
  return r;
}

future<request_result> nodehost::request_remove_data(group_id gid) {
  auto shard = _partitioner(gid.cluster);
  if (shard != this_shard_id()) {
    return container().invoke_on(shard, &nodehost::request_remove_data, gid);
  }
  return make_exception_future<request_result>(util::panic("not implemented"));
}

void nodehost::initialize_handlers() {
  _rpc.register_unreachable_handler([s = shared_from_this()](auto gid) {
    return s->handle_unreachable(gid);
  });
  _rpc.register_message_handler([s = shared_from_this()](auto m) {
    return s->handle_message(std::move(m));
  });
  _rpc.register_snapshot_handler([s = shared_from_this()](auto gid, auto from) {
    return s->handle_snapshot(gid, from);
  });
}

void nodehost::uninitialize_handlers() {
  _rpc.register_unreachable_handler(
      [](auto gid) { return make_ready_future<>(); });
  _rpc.register_message_handler([](auto m) { return make_ready_future<>(); });
  _rpc.register_snapshot_handler(
      [](auto gid, auto from) { return make_ready_future<>(); });
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
    (void)it->second->_received_messages
        .push_eventually(message{
            .type = message_type::unreachable,
            .from = gid.node,
            .to = it->second->id().node})
        .handle_exception([](std::exception_ptr ex) {
          l.error("failed to push unreachable to node: {}", ex);
        });
    it->second->_engine.step_ready(gid.cluster);
  }
  return make_ready_future<>();
}

future<> nodehost::handle_message(message m) {
  if (_stopped) [[unlikely]] {
    return make_exception_future<>(util::closed_error());
  }
  if (m.to == group_id::INVALID_NODE) [[unlikely]] {
    throw util::panic("invalid to");
  }
  auto shard = _partitioner(m.cluster);
  if (shard != this_shard_id()) {
    (void)container().invoke_on(shard, &nodehost::handle_message, std::move(m));
    return make_ready_future<>();
  }
  if (auto it = _clusters.find(m.cluster); it != _clusters.end()) {
    if (it->second->id().node != m.to) {
      l.warn(
          "nodehost::handle_message: ignored a {} message sent to {} but "
          "received by {}",
          m.type,
          group_id{m.cluster, m.to},
          group_id{m.cluster, it->second->id().node});
      return make_ready_future<>();
    }
    // TODO(jyc): since we are blindly push_eventually for snapshot related
    //  messages, shall we check for the number of existing awaiters?
    if (m.type == message_type::install_snapshot) {
      (void)it->second->_received_messages.push_eventually(std::move(m))
          .handle_exception([](std::exception_ptr ex) {
            l.error("failed to push install snapshot to node: {}", ex);
          });
    } else if (m.type == message_type::snapshot_received) {
      (void)it->second->_received_messages
          .push_eventually(
              message{.type = message_type::snapshot_status, .from = m.from})
          .handle_exception([](std::exception_ptr ex) {
            l.error("failed to push snapshot status to node: {}", ex);
          });
    } else {
      if (!it->second->_received_messages.push(m)) {
        l.warn("nodehost::handle_message: dropped a {} message", m.type);
      }
    }
    it->second->_engine.step_ready(m.cluster);
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
    (void)it->second->_received_messages
        .push_eventually(message{
            .type = message_type::snapshot_status,
            .from = gid.node,
            .reject = failed})
        .handle_exception([](std::exception_ptr ex) {
          l.error("failed to push snapshot status to node: {}", ex);
        });
    it->second->_engine.step_ready(gid.cluster);
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
      co_return coroutine::make_exception(util::panic("cluster not booted"));
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
