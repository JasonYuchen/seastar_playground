//
// Created by jason on 2022/5/28.
//

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/http/httpd.hh>

#include "util/seastarx.hh"

namespace rafter {

class nodehost;

class api_server
  : public async_sharded_service<api_server>
  , public peering_sharded_service<api_server> {
 public:
  explicit api_server(nodehost& nh);
  future<> stop();
  void initialize_handlers();
  void set_routes(std::function<void(routes& r)> fun);
  future<> listen(socket_address addr, listen_options lo);

 private:
  http_server _server;
  nodehost& _nodehost;
};

}  // namespace rafter
