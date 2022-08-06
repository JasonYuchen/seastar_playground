//
// Created by jason on 2021/12/11.
//

#pragma once

#include <memory>
#include <seastar/core/shared_ptr.hh>

#include "protocol/raft.hh"
#include "rafter/config.hh"

namespace rafter::test {

class util {
 public:
  static config default_config();
  static std::function<unsigned(uint64_t)> partition_func();
  static std::function<std::string(protocol::group_id)> snapshot_dir_func(
      std::string root);
  static std::vector<protocol::update> make_updates(
      protocol::group_id gid,
      size_t num,                 // total number of updates
      size_t entry_interval,      // fill entries per entry_interval
      size_t state_interval,      // fill state per state_interval
      size_t snapshot_interval);  // fill snapshot per snapshot interval
  static size_t extract_entries(
      protocol::update& up, protocol::log_entry_vector& entries);
  static bool compare(
      const protocol::update& lhs, const protocol::update& rhs) noexcept;
  static bool compare(
      const protocol::snapshot& lhs, const protocol::snapshot& rhs) noexcept;
  // generate entry vector with continuous log entries
  // first index = first term = range.low
  // last index = last term = range.high - 1
  static protocol::log_entry_vector new_entries(protocol::hint range);
  static protocol::log_entry_vector new_entries(
      const std::vector<protocol::log_id>& lids);
  static protocol::snapshot_ptr new_snapshot(protocol::log_id lid);
};

}  // namespace rafter::test
