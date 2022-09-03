//
// Created by jason on 2022/5/28.
//

#include "api_server.hh"

#include "rafter/logger.hh"
#include "rafter/nodehost.hh"

namespace rafter {

api_server::api_server(nodehost& nh) : _server("test"), _nodehost(nh) {}

future<> api_server::stop() {
  l.info("api_server stopping...");
  return _server.stop();
}

void api_server::initialize_handlers() {
  // TODO(jyc): nodehost apis
}

void api_server::set_routes(std::function<void(routes&)> fun) {
  fun(_server._routes);
}

future<> api_server::listen(socket_address addr, listen_options lo) {
  return _server.listen(std::move(addr), std::move(lo));
}

}  // namespace rafter
