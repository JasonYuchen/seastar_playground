//
// Created by jason on 2022/5/13.
//

#pragma once

#include <functional>
#include <seastar/core/future.hh>

#include "protocol/raft.hh"
#include "util/seastarx.hh"

namespace rafter::transport {

class rpc {
 public:
  virtual ~rpc() = default;

  virtual future<> send_message(protocol::message m) = 0;

  virtual future<> send_snapshot(protocol::message m) = 0;

  using unreachable_handler = std::function<future<>(protocol::group_id)>;
  virtual void register_unreachable_handler(unreachable_handler&& func) = 0;

  using message_handler = std::function<future<>(protocol::message)>;
  virtual void register_message_handler(message_handler&& func) = 0;

  using snapshot_handler =
      std::function<future<>(protocol::group_id, uint64_t from)>;
  virtual void register_snapshot_handler(snapshot_handler&& func) = 0;

  using snapshot_status_handler =
      std::function<future<>(protocol::group_id, bool rejected)>;
  virtual void register_snapshot_status_handler(
      snapshot_status_handler&& func) = 0;
};

}  // namespace rafter::transport
