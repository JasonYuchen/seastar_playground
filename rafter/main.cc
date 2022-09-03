//
// Created by jason on 2022/5/22.
//

#include <seastar/core/app-template.hh>

#include "rafter/api_server.hh"
#include "rafter/logger.hh"
#include "rafter/nodehost.hh"
#include "rsm/session_manager.hh"
#include "rsm/snapshotter.hh"
#include "server/environment.hh"
#include "storage/segment_manager.hh"
#include "transport/exchanger.hh"
#include "transport/registry.hh"
#include "util/signal.hh"

using namespace seastar;

static int rafter_main(int argc, char** argv, char** env) {
  app_template::config app_cfg;
  app_cfg.name = "rafter";
  app_cfg.description = "FIXME";
  app_cfg.auto_handle_sigint_sigterm = false;
  app_template app{std::move(app_cfg)};
  // TODO(jyc): program options here

  static sharded<rafter::storage::segment_manager> logdb;
  static sharded<rafter::transport::registry> registry;
  static sharded<rafter::transport::exchanger> rpc;
  static sharded<rafter::nodehost> nodehost;
  static sharded<rafter::api_server> server;

  return app.run(argc, argv, [&]() -> future<int> {
    rafter::l.info("rafter initializing...");
    auto&& opts = app.configuration();
    rafter::util::stop_signal stop_signal;
    // TODO(jyc): construct nodehost config via yaml
    rafter::config config;
    config.data_dir = "testdata";
    config.listen_port = 40615;
    rafter::config::initialize(config);
    co_await rafter::config::broadcast();

    // TODO(jyc): starting procedure (draft)
    //  1. constructs all sharded instances
    //  2. start all rafter services except for messaging service
    //  3. start messaging service, make sure all other services are up when
    //     accepting peer's messages
    //  4. reload all existing clusters (be careful that a message belonging to
    //     cluster A may arrive before the initialization of cluster A)
    //  5. start web service and accept normal requests

    using rafter::server::environment;
    auto partitioner = environment::get_partition_func();
    auto snapshot_dir = environment::get_snapshot_dir_func(config.data_dir);

    co_await logdb.start(partitioner);
    co_await registry.start();
    co_await rpc.start(std::ref(registry), std::move(snapshot_dir));
    co_await nodehost.start(
        std::move(config), std::ref(logdb), std::ref(registry), std::ref(rpc));
    co_await server.start(std::ref(nodehost));

    co_await logdb.invoke_on_all(&rafter::storage::segment_manager::start);
    co_await registry.invoke_on_all(&rafter::transport::registry::start);
    // TODO(jyc): list and reload existing clusters in nodehost::start
    co_await nodehost.invoke_on_all(&rafter::nodehost::start);
    co_await rpc.invoke_on_all(&rafter::transport::exchanger::start);
    co_await server.invoke_on_all(&rafter::api_server::initialize_handlers);
    socket_address l_addr{uint16_t{30615}};
    listen_options l_opt{.reuse_address = true};
    co_await server.invoke_on_all(&rafter::api_server::listen, l_addr, l_opt);

    rafter::l.info("rafter is up now");
    auto signum = co_await stop_signal.wait();
    rafter::l.info("rafter exiting... with {}:{}", signum, ::strsignal(signum));
    // TODO(jyc): stop and close all existing clusters in nodehost::stop
    co_await server.stop();
    co_await nodehost.stop();
    co_await rpc.stop();
    co_await registry.stop();
    co_await logdb.stop();
    co_return 0;
  });
}

int main(int argc, char** argv, char** env) {
  return rafter_main(argc, argv, env);
}
