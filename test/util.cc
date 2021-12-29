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
      .data_dir = "test_data",
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
  hard_state prev_state{.term = 1, .vote = 2, .commit = 0};
  uint64_t prev_index = r(g) + 1;
  for (size_t i = 1; i <= num; ++i) {
    update up{.gid = gid};
    if ((state_interval > 0) && (i % state_interval == 0)) {
      up.state.term = prev_state.term + r(g) % 3;
      up.state.commit = prev_index - 1;
      up.state.vote = 2;
      prev_state = up.state;
    }
    if ((snapshot_interval > 0) && (i % snapshot_interval == 0)) {
      up.snapshot = make_lw_shared<snapshot>();
      up.snapshot->group_id = gid;
      up.snapshot->log_id = {.term = prev_state.term, .index = prev_index};
      up.snapshot->file_path = fmt::format("test snapshot for {}", gid);
    }
    if ((entry_interval > 0) && (i % entry_interval == 0)) {
      up.first_index = prev_index;
      up.last_index = prev_index + r(g);
      prev_index = up.last_index + 1;
      for (size_t j = up.first_index; j <= up.last_index; ++j) {
        auto e = up.entries_to_save.emplace_back(make_lw_shared<log_entry>());
        e->lid = {.term = prev_state.term, .index = j};
        e->payload = fmt::format("test_payload for {} with {}", gid, e->lid);
      }
    }
    up.fill_meta();
    if (up.has_update()) {
      updates.emplace_back(std::move(up));
    }
  }
  return updates;
}

size_t util::extract_entries(const update& up, log_entry_vector& entries) {
  size_t size = 0;
  std::for_each(
      up.entries_to_save.begin(), up.entries_to_save.end(), [&](auto e) {
        entries.push_back(e);
        size += e->bytes();
      });
  return size;
}

bool util::compare(const update& lhs, const update& rhs) noexcept {
  if (lhs.gid != rhs.gid) {
    return false;
  }
  if (lhs.state != rhs.state) {
    return false;
  }
  if (lhs.first_index != rhs.first_index) {
    return false;
  }
  if (lhs.last_index != rhs.last_index) {
    return false;
  }
  if (lhs.snapshot_index != rhs.snapshot_index) {
    return false;
  }
  if (lhs.entries_to_save.size() != rhs.entries_to_save.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.entries_to_save.size(); ++i) {
    if (lhs.entries_to_save[i].operator bool() ^
        rhs.entries_to_save[i].operator bool()) {
      return false;
    }
    if (lhs.entries_to_save[i] &&
        *lhs.entries_to_save[i] != *rhs.entries_to_save[i]) {
      return false;
    }
  }
  if (lhs.snapshot.operator bool() ^ rhs.snapshot.operator bool()) {
    return false;
  }
  if (lhs.snapshot) {
    return compare(*lhs.snapshot, *rhs.snapshot);
  }
  return true;
}

bool util::compare(
    const protocol::snapshot& lhs, const protocol::snapshot& rhs) noexcept {
  if (lhs.group_id != rhs.group_id) {
    return false;
  }
  if (lhs.log_id != rhs.log_id) {
    return false;
  }
  if (lhs.file_path != rhs.file_path) {
    return false;
  }
  if (lhs.file_size != rhs.file_size) {
    return false;
  }
  if (lhs.membership.operator bool() ^ rhs.membership.operator bool()) {
    return false;
  }
  if (lhs.membership && *lhs.membership != *rhs.membership) {
    return false;
  }
  if (lhs.files.size() != rhs.files.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.files.size(); ++i) {
    if (lhs.files[i].operator bool() ^ rhs.files[i].operator bool()) {
      return false;
    }
    if (lhs.files[i] && *lhs.files[i] != *rhs.files[i]) {
      return false;
    }
  }
  if (lhs.smtype != rhs.smtype) {
    return false;
  }
  if (lhs.imported != rhs.imported) {
    return false;
  }
  if (lhs.witness != rhs.witness) {
    return false;
  }
  if (lhs.dummy != rhs.dummy) {
    return false;
  }
  if (lhs.on_disk_index != rhs.on_disk_index) {
    return false;
  }
  return true;
}

}  // namespace rafter::test
