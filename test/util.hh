//
// Created by jason on 2021/12/11.
//

#pragma once

#include <memory>

#include <seastar/core/shared_ptr.hh>

#include "storage/segment.hh"
#include "storage/segment_manager.hh"

namespace rafter::test {

struct segment_package {
  std::vector<protocol::group_id> _gids;
  seastar::lw_shared_ptr<storage::segment> _segment;
  std::vector<protocol::update> _updates;
  std::vector<storage::index::entry> _indexes;
};

class util {
 public:
  static config default_config();
  static std::vector<protocol::update> make_updates(
      protocol::group_id gid,
      size_t num,                 // total number of updates
      size_t entry_interval,      // fill entries per entry_interval
      size_t state_interval,      // fill state per state_interval
      size_t snapshot_interval);  // fill snapshot per snapshot interval
  static bool compare(
      const protocol::update& lhs, const protocol::update& rhs) noexcept;
  static bool compare(
      const protocol::snapshot& lhs, const protocol::snapshot& rhs) noexcept;
};

}  // namespace rafter::test
