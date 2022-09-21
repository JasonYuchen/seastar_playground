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
  explicit api_server(nodehost& nh, socket_address addr, listen_options lo);
  future<> start();
  future<> stop();
  void initialize_handlers();

 private:
  friend class echo_handler;
  nodehost& _nodehost;
  socket_address _address;
  listen_options _options;
  http_server _server;
};

}  // namespace rafter
