//
// Created by jason on 2022/3/14.
//

#pragma once

#include <string>

namespace rafter::protocol {

struct rsm_result {
  uint64_t value;
  std::string data;
};

struct snapshot_request {
  enum class type {
    periodic,
    user_triggered,
    exported,
    streaming,
  };
  std::string path;
  enum type type = type::periodic;
  uint64_t key = 0;
  uint64_t compaction_overhead = 0;
  bool override_compaction = false;
};

}  // namespace rafter::protocol
