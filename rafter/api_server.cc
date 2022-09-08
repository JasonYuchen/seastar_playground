//
// Created by jason on 2022/5/28.
//

#include "api_server.hh"

#include "api/api.hh"
#include "rafter/logger.hh"
#include "rafter/nodehost.hh"
#include "util/seastarx.hh"

namespace {

using namespace seastar;
using namespace httpd;

class echo_handler : public handler_base {
 public:
  explicit echo_handler(std::string_view tag) : tag_(tag) {}
  future<std::unique_ptr<reply>> handle(
      const sstring& path,
      std::unique_ptr<request> req,
      std::unique_ptr<reply> resp) override {
    resp->_content = fmt::format("{}, echo:\n{}", tag_, req->content);
    resp->done("html");
    return make_ready_future<std::unique_ptr<reply>>(std::move(resp));
  }

 private:
  std::string tag_;
};

}  // namespace

namespace rafter {

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
      _server._routes, new echo_handler{"listClusters"});
  httpd::api_json::startCluster.set(
      _server._routes, new echo_handler{"startCluster"});
  httpd::api_json::singleGet.set(
      _server._routes, new echo_handler{"singleGet"});
  httpd::api_json::singlePut.set(
      _server._routes, new echo_handler{"singlePut"});
  httpd::api_json::singleDelete.set(
      _server._routes, new echo_handler{"singleDelete"});
}

}  // namespace rafter
