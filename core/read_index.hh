//
// Created by jason on 2021/12/21.
//

#pragma once

#include <deque>
#include <unordered_set>

#include "protocol/raft.hh"
#include "util/util.hh"

namespace rafter::test {

class core_helper;

}  // namespace rafter::test

namespace rafter::core {

class read_index {
 public:
  struct status {
    uint64_t index;
    uint64_t from;
    protocol::hint ctx;
    std::unordered_set<uint64_t> confirmed;
  };

  void add_request(uint64_t index, protocol::hint ctx, uint64_t from);
  void confirm(
      protocol::hint ctx,
      uint64_t from,
      uint64_t quorum,
      std::function<void(const status&)>&& handler);
  bool has_pending_request() const { return !_queue.empty(); }
  protocol::hint peep() const { return _queue.back(); }
  void clear() {
    _pending.clear();
    _queue.clear();
  }

 private:
  friend class test::core_helper;

  std::unordered_map<protocol::hint, status, util::pair_hasher> _pending;
  std::deque<protocol::hint> _queue;
};

}  // namespace rafter::core
