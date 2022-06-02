//
// Created by jason on 2022/5/22.
//

#pragma once

#include <functional>
#include <string>

#include "protocol/raft.hh"

namespace rafter::server {

class environment {
 public:
  static std::function<std::string(protocol::group_id)> get_snapshot_dir_func(
      std::string root_dir);
  // TODO(jyc): naive partitioner
  static std::function<unsigned(uint64_t)> get_partition_func();
};

}  // namespace rafter::server
