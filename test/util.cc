//
// Created by jason on 2021/12/11.
//

#include "util.hh"

#include <random>

namespace rafter::test {

using namespace rafter::protocol;
using namespace seastar;
using namespace std;

config util::default_config() {
  return config{
      .data_dir = "test_data/wal",
      .wal_rolling_size = 100UL * KB,  // a smaller size to make more segments
  };
}

vector<update> util::make_updates(
    group_id gid,
    size_t num,
    size_t entry_interval,
    size_t state_interval,
    size_t snapshot_interval) {
  std::random_device rd;
  std::mt19937 g(rd());
  std::uniform_int_distribution<int> r(0, 100);
  std::vector<update> updates;
  hard_state prev_state {.term = 1, .vote = 2, .commit = 0};
  uint64_t prev_index = r(g) + 1;
  for (size_t i = 1; i <= num; ++i) {
    update up{.gid = gid};
    if (i % state_interval == 0) {
      up.state.term = prev_state.term + r(g) % 3;
      up.state.commit = prev_index - 1;
      up.state.vote = 2;
      prev_state = up.state;
    }
    if (i % snapshot_interval == 0) {
      up.snapshot = make_lw_shared<snapshot>();
      up.snapshot->group_id = gid;
      up.snapshot->log_id = {.term = prev_state.term, .index = prev_index};
      up.snapshot->file_path = "test snapshot for " + gid.to_string();
    }
    if (i % entry_interval == 0) {
      up.first_index = prev_index;
      up.last_index = prev_index + r(g);
      prev_index = up.last_index + 1;
      for (size_t j = up.first_index; j <= up.last_index; ++j) {
        auto e = up.entries_to_save.emplace_back(make_lw_shared<log_entry>());
        e->lid = {.term = prev_state.term, .index = j};
        e->payload = fmt::format("test_payload for {} with {}",
                                 gid.to_string(), e->lid.to_string());
      }
    }
    up.fill_meta();
    if (up.has_update()) {
      updates.emplace_back(std::move(up));
    }
  }
  return updates;
}

}  // namespace rafter::test
