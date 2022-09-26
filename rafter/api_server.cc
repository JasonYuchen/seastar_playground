//
// Created by jason on 2022/5/28.
//

#include "api_server.hh"

#include "api/api.hh"
#include "rafter/logger.hh"
#include "rafter/nodehost.hh"
#include "simdjson.h"
#include "util/seastarx.hh"

namespace rafter {

using namespace seastar;
using namespace httpd;

class echo_handler : public handler_base {
 public:
  explicit echo_handler(std::string_view tag, api_server* server)
    : _tag(tag), _server(server) {}
  future<std::unique_ptr<reply>> handle(
      const sstring& path,
      std::unique_ptr<request> req,
      std::unique_ptr<reply> resp) override {
    resp->_content = fmt::format("{}, echo:\n{}", _tag, req->content);
    resp->done("html");
    return make_ready_future<std::unique_ptr<reply>>(std::move(resp));
  }

 protected:
  nodehost& nh() { return _server->_nodehost; }

  std::string _tag;
  api_server* _server;
};

class list_cluster_handler : public echo_handler {
 public:
  using echo_handler::echo_handler;

 private:
};

class start_cluster_handler : public echo_handler {
 public:
  using echo_handler::echo_handler;
  future<std::unique_ptr<reply>> handle(
      const sstring& path,
      std::unique_ptr<request> req,
      std::unique_ptr<reply> resp) override {
    simdjson::ondemand::parser parser;
    auto j = simdjson::padded_string(std::string_view{req->content});
    simdjson::ondemand::document doc = parser.iterate(j);
    raft_config cfg;
    protocol::member_map peers;
    try {
      cfg.cluster_id = doc["clusterId"].get_uint64().value();
      cfg.node_id = doc["nodeId"].get_uint64().value();
      cfg.election_rtt = doc["electionRTT"].get_uint64().value();
      cfg.heartbeat_rtt = doc["heartbeatRTT"].get_uint64().value();
      cfg.snapshot_interval = doc["snapshotInterval"].get_uint64().value();
      cfg.compaction_overhead = doc["compactionOverhead"].get_uint64().value();
      for (auto&& item : doc["peers"].get_array()) {
        peers.emplace(
            item["nodeId"].get_uint64().value(),
            item["address"].get_string().value());
      }
      // TODO(jyc): omit other parameters
    } catch (const simdjson::simdjson_error& ex) {
      resp->set_status(
              httpd::reply::status_type::bad_request,
              fmt::format("Error: {}", ex.what()))
          .done("html");
      co_return std::move(resp);
    }
    std::stringstream ss;
    ss << cfg;
    co_await nh().start_cluster(
        std::move(cfg),
        std::move(peers),
        false,
        protocol::state_machine_type::regular,
        std::make_unique<kv_statemachine_factory>());
    resp->set_status(httpd::reply::status_type::ok, ss.str()).done("html");
    co_return std::move(resp);
  }

 private:
};

class single_get_handler : public echo_handler {
 public:
  using echo_handler::echo_handler;
  future<std::unique_ptr<reply>> handle(
      const sstring& path,
      std::unique_ptr<request> req,
      std::unique_ptr<reply> resp) override {
    const auto& cid = req->param.at("clusterId");
    uint64_t cluster_id = 0;
    auto ec = std::from_chars(cid.begin() + 1, cid.end(), cluster_id);
    if (ec.ec != std::errc{}) {
      resp->set_status(
              httpd::reply::status_type::bad_request,
              fmt::format("invalid cluster id: {}", cid))
          .done("html");
    } else {
      auto result = co_await nh().linearizable_read(
          cluster_id, req->query_parameters.at("key"));
      resp->set_status(
              httpd::reply::status_type::ok,
              fmt::format("{}:{}", result.code, result.result.data))
          .done("html");
    }
    co_return std::move(resp);
  }

 private:
};

class single_put_handler : public echo_handler {
 public:
  using echo_handler::echo_handler;
  future<std::unique_ptr<reply>> handle(
      const sstring& path,
      std::unique_ptr<request> req,
      std::unique_ptr<reply> resp) override {
    const auto& cid = req->param.at("clusterId");
    uint64_t cluster_id = 0;
    auto ec = std::from_chars(cid.begin() + 1, cid.end(), cluster_id);
    if (ec.ec != std::errc{}) {
      resp->set_status(
              httpd::reply::status_type::bad_request,
              fmt::format("invalid cluster id: {}", cid))
          .done("html");
    } else {
      auto session = co_await nh().get_noop_session(cluster_id);
      auto cmd = fmt::format(
          "{}={}",
          req->query_parameters.at("key"),
          req->query_parameters.at("value"));
      auto result = co_await nh().propose(session, cmd);
      resp->set_status(
              httpd::reply::status_type::ok,
              fmt::format("{}:{}", result.code, result.result.data))
          .done("html");
    }
    co_return std::move(resp);
  }

 private:
};

class single_delete_handler : public echo_handler {
 public:
  using echo_handler::echo_handler;

 private:
};

api_server::api_server(nodehost& nh, socket_address addr, listen_options lo)
  : _nodehost(nh), _address(addr), _options(lo), _server("rafter") {
  initialize_handlers();
}

future<> api_server::start() {
  l.info("api_server starting...");
  return _server.listen(_address, _options);
}

future<> api_server::stop() {
  l.info("api_server stopping...");
  return _server.stop();
}

void api_server::initialize_handlers() {
  // TODO(jyc): nodehost apis
  httpd::api_json::listClusters.set(
      _server._routes, new list_cluster_handler{"listClusters", this});
  httpd::api_json::startCluster.set(
      _server._routes, new start_cluster_handler{"startCluster", this});
  httpd::api_json::singleGet.set(
      _server._routes, new single_get_handler{"singleGet", this});
  httpd::api_json::singlePut.set(
      _server._routes, new single_put_handler{"singlePut", this});
  httpd::api_json::singleDelete.set(
      _server._routes, new single_delete_handler{"singleDelete", this});
}

}  // namespace rafter
