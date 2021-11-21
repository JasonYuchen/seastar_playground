//
// Created by jason on 2021/10/13.
//

#include "segment_manager.hh"

#include <seastar/core/reactor.hh>

namespace rafter::storage {

using namespace protocol;
using namespace seastar;
using namespace std;

segment_manager::segment_manager(string log_dir)
    : _log_dir(std::move(log_dir)) {
  // TODO: setup WAL module
  //  1. validate data_dir
  //  2. parse existing segments
  //  3. create active segment
  file dir = co_await open_directory(_log_dir);
  co_await dir.list_directory(parse_existing_segments).done();
  if (_segments.empty()) {
    co_await rolling();
  }
  co_await dir.close();
}

future<bool> segment_manager::append(const protocol::update& u) {
  if (!u.snapshot && u.entries_to_save.empty() && u.state.empty()) {
    co_return false;
  }
  auto ni = _index_group.get_node_index(u.group_id);
  auto [file_id, segment] = *_segments.rbegin();
  auto pos = segment->bytes();
  auto new_pos = co_await segment->append(u);
  if (!u.entries_to_save.empty()) {
    ni->update_entry({
        .id = u.group_id,
        .first_index = u.first_index,
        .last_index = u.last_index,
        .filename = file_id,
        .offset = pos,
        .length = new_pos - pos,
        .type = index::entry::type::normal});
  }
  if (!u.state.empty()) {
    ni->update_state({
        .id = u.group_id,
        .first_index = log_id::invalid_index,
        .last_index = log_id::invalid_index,
        .filename = file_id,
        .offset = pos,
        .length = new_pos - pos,
        .type = index::entry::type::state});
  }
  if (u.snapshot) {
    ni->update_snapshot({
        .id = u.group_id,
        .first_index = u.snapshot->log_id.index,
        .last_index = log_id::invalid_index,
        .filename = file_id,
        .offset = pos,
        .length = new_pos - pos,
        .type = index::entry::type::snapshot});
  }
  if (new_pos >= _rolling_size) {
    co_await rolling();
    co_return false;
  }
  co_return
      u.snapshot || !u.entries_to_save.empty() || u.state.term || u.state.vote;
}

seastar::future<> segment_manager::rolling() {
  _next_filename++;
  if (!_segments.empty()) {
    co_await _segments.rbegin()->second->sync();
    co_await _segments.rbegin()->second->close();
  }
  filesystem::path p = fmt::format(
      "{}/{:05d}_{:020d}.{}",
      _log_dir, this_shard_id(), _next_filename, LOG_SUFFIX);
  auto s = co_await segment::open(std::move(p));
  _segments.emplace_hint(_segments.end(), std::move(s));
  co_await sync_directory(_log_dir);
  co_return;
}

}  // namespace rafter::storage
