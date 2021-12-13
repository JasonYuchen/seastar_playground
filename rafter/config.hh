//
// Created by jason on 2021/12/8.
//

#pragma once

#include <stdint.h>

#include <string>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/units.hh>

namespace rafter {

using seastar::KB;
using seastar::MB;
using seastar::GB;

struct config {
  // TODO: add scheduling group configuration here
  // the absolute path of a directory for data (WAL, snapshot, etc) storage
  std::string data_dir;
  // the rolling threshold size in bytes for segment files
  uint64_t wal_rolling_size = 1UL * GB;
  // the max capacity of the queue holding out-dated segment files
  uint64_t wal_gc_queue_capacity = 100;

  void validate() const;
  // TODO: read from yaml?
};

// config file is shared among all shards
using config_ptr = seastar::shared_ptr<config>;

}  // namespace rafter