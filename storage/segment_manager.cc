//
// Created by jason on 2021/10/13.
//

#include "segment_manager.hh"

#include <charconv>

#include <seastar/core/reactor.hh>

namespace rafter::storage {

using namespace protocol;
using namespace seastar;
using namespace std;

segment_manager::segment_manager(string log_dir)
    : _log_dir(std::move(log_dir)), _obsolete_queue(_gc_queue_length) {
  file dir = co_await recursive_touch_directory(_log_dir);
  co_await dir.list_directory(parse_existing_segments).done();
  // always write to a new file after bootstrap
  _next_filename = _segments.empty() ? 0 : _segments.rbegin()->first;
  co_await rolling();
  co_await dir.close();
  _open = true;
  _gc_service = segement_gc_service();
}

segment_manager::~segment_manager() {
  if (_open) {
    co_await shutdown();
  }
}

future<> segment_manager::shutdown() {
  _open = false;
  // TODO(jason):
  co_return co_await _gc_service.discard_result();
}

future<bool> segment_manager::append(const protocol::update& u) {
  if (!_open) {
    // TODO
    co_return coroutine::make_exception(std::runtime_error("closed"));
  }
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

future<> segment_manager::parse_existing_segments(directory_entry s) {
  auto [shard_id, filename_id] = parse_segment_name(s.name);
  if (shard_id != this_shard_id()) {
    co_return;
  }
  auto seg = co_await segment::open(s.name, true);
  co_await seg->list_index([this](const index::entry& e) -> future<> {
    _index_group.update(e);
    co_return;
  });
  _segments.emplace(filename_id, std::move(seg));
  co_return;
}

future<> segment_manager::rolling() {
  _next_filename++;
  if (!_segments.empty()) {
    co_await _segments.rbegin()->second->sync();
    co_await _segments.rbegin()->second->close();
  }
  string p = fmt::format("{}/{:05d}_{:020d}.{}",
                         _log_dir, this_shard_id(), _next_filename, LOG_SUFFIX);
  auto s = co_await segment::open(std::move(p));
  _segments.emplace_hint(_segments.end(), std::move(s));
  co_await sync_directory(_log_dir);
  co_return;
}

}  // namespace rafter::storage
