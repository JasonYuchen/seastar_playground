//
// Created by jason on 2021/12/21.
//

#include "read_index.hh"

#include "util/error.hh"

namespace rafter::core {

using namespace protocol;

void read_index::add_request(uint64_t index, hint ctx, uint64_t from) {
  if (_pending.contains(ctx)) {
    return;
  }
  if (!_queue.empty()) {
    // make sure the queue and the pending are consistent
    auto s = _pending.find(peep());
    if (s == _pending.end()) [[unlikely]] {
      throw util::invalid_raft_state("inconsistent read_index");
    }
    if (index < s->second.index) [[unlikely]] {
      throw util::invalid_raft_state("index moved backward in read_index");
    }
  }
  _queue.push_back(ctx);
  _pending[ctx] = {.index = index, .from = from, .ctx = ctx};
}

void read_index::confirm(
    hint ctx,
    uint64_t from,
    uint64_t quorum,
    std::function<void(const status &)> &&handler) {
  auto it = _pending.find(ctx);
  if (it == _pending.end()) {
    return;
  }
  it->second.confirmed.insert(from);
  if (it->second.confirmed.size() + 1 < quorum) {
    return;
  }
  auto qit = find(_queue.begin(), _queue.end(), ctx);
  if (qit == _queue.end()) {
    return;
  }
  auto cut = _pending.find(*qit);
  if (cut == _pending.end()) [[unlikely]] {
    throw util::invalid_raft_state("inconsistent read_index");
  }
  while (!_queue.empty()) {
    auto cur_ctx = _queue.front();
    _queue.pop_front();
    auto s = _pending.find(cur_ctx);
    if (s == _pending.end()) [[unlikely]] {
      throw util::invalid_raft_state("inconsistent read_index");
    }
    if (cut->second.index < s->second.index) [[unlikely]] {
      throw util::invalid_raft_state("unexpected index in read_index");
    }
    s->second.index = cut->second.index;
    handler(s->second);
    _pending.erase(s);
    if (cur_ctx == ctx) {
      return;
    }
  }
}

}  // namespace rafter::core
