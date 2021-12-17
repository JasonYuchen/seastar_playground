//
// Created by jason on 2021/10/13.
//

#include "segment_manager.hh"

#include <charconv>

#include <seastar/core/reactor.hh>

#include "storage/logger.hh"
#include "util/error.hh"

namespace rafter::storage {

using namespace protocol;
using namespace seastar;
using namespace std;
using util::code;

segment_manager::segment_manager(const config& config)
    : _config(config) {
}

future<> segment_manager::start() {
  _log_dir = filesystem::path(_config.data_dir).append("wal");
  co_await recursive_touch_directory(_log_dir);
  _obsolete_queue = std::make_unique<seastar::queue<uint64_t>>(
      _config.wal_gc_queue_capacity);
  l.info("segment_manager::start: dir:{}, rolling_size:{}, gc_queue_cap:{}",
         _log_dir, _config.wal_rolling_size, _config.wal_gc_queue_capacity);
  file dir = co_await open_directory(_log_dir);
  co_await dir.list_directory(
                  [this](auto e){ return parse_existing_segments(e); }).done();
  // always write to a new file after bootstrap
  _next_filename = _segments.empty() ? 1 : _segments.rbegin()->first + 1;
  co_await rolling();
  co_await dir.close();
  _open = true;
  _gc_service = gc_service();
}

future<> segment_manager::stop() {
  _open = false;
  _obsolete_queue->abort(std::make_exception_ptr(
      util::runtime_error(code::closed)));
  co_await _gc_service->discard_result();
  l.info("segment_manager::stop: stopped");
}

stats segment_manager::stats() const noexcept {
  return _stats;
}

future<bool> segment_manager::append(const protocol::update& up) {
  must_open();
  if (!up.snapshot && up.entries_to_save.empty() && up.state.empty()) {
    co_return false;
  }
  _stats._append++;
  _stats._append_entry += up.entries_to_save.size();
  _stats._append_state += !up.state.empty();
  _stats._append_snap += up.snapshot.operator bool();

  auto prev_state = _index_group.get_hard_state(up.gid);
  bool need_sync =
      up.snapshot ||
      !up.entries_to_save.empty() ||
      prev_state.term != up.state.term ||
      prev_state.vote != up.state.vote;
  auto e = co_await with_lock(_mutex, [&, this]() -> future<index::entry> {
    auto it = _segments.rbegin();
    auto filename = it->first;
    auto segment = it->second.get();
    auto offset = segment->bytes();
    auto new_offset = co_await segment->append(up);
    index::entry e {
        .filename = filename,
        .offset = offset,
        .length = new_offset - offset,
    };
    if (new_offset >= _config.wal_rolling_size) {
      co_await rolling();
      _need_sync = need_sync = false;
    }
    _need_sync = _need_sync || need_sync;
    co_return e;
  });
  co_await update_index(up, e);
  co_return need_sync;
}

future<> segment_manager::remove(group_id id, uint64_t index) {
  _stats._remove++;
  update comp {
      .gid = id,
      .state = {
          .commit = index,
      },
  };
  co_await append(comp);
  co_await compaction(id);
}

future<snapshot_ptr> segment_manager::query_snapshot(group_id id) {
  static const char* err = "{gid} segment_manager::query_snapshot: {msg}";
  _stats._query_snap++;

  auto i = _index_group.query_snapshot(id);
  if (i.empty()) {
    co_return snapshot_ptr{};
  }
  auto it = _segments.find(i.filename);
  if (it == _segments.end()) {
    co_return coroutine::make_exception(util::runtime_error(
        l, code::failed_precondition, err, id.to_string(), "segment lost"));
  }
  auto segment = it->second.get();
  auto up = co_await segment->query(i);
  if (up.snapshot->log_id.index == log_id::invalid_index) {
    co_return coroutine::make_exception(util::io_error(
        l, code::corruption, err, id.to_string(), "empty snapshot"));
  }
  co_return std::move(up.snapshot);
}

future<raft_state> segment_manager::query_raft_state(
    protocol::group_id id, uint64_t last_index) {
  static const char* err = "{gid} segment_manager::query_raft_state: {msg}";
  _stats._query_state++;

  // TODO: read from segment or read from _index_group?
  auto st = _index_group.get_hard_state(id);
  if (st.empty()) {
    co_return coroutine::make_exception(util::runtime_error(
        l, code::no_data, err, id.to_string(), "no saved data"));
  }
  raft_state r;
  r.hard_state = st;
  auto ie = _index_group.query(id, {.low = last_index + 1, .high = UINT64_MAX});
  if (!ie.empty()) {
    uint64_t prev_idx = ie.front().first_index - 1;
    for (auto e : ie) {
      if (prev_idx + 1 != e.first_index) {
        co_return coroutine::make_exception(util::logic_error(
            l, code::failed_precondition, err, "gap found in indexes"));
      }
      prev_idx = e.last_index;
    }
    r.first_index = last_index + 1;
    r.entry_count = ie.back().last_index - r.first_index;
  }
  co_return r;
}

future<protocol::log_entry_vector> segment_manager::query_entries(
    protocol::group_id id, protocol::hint range, uint64_t max_size) {
  _stats._query_entry++;

  auto ni = _index_group.get_node_index(id);
  auto compacted_to = ni->compacted_to();
  if (range.low <= compacted_to) {
    co_return protocol::log_entry_vector{};
  }
  auto indexes = ni->query(range);
  if (indexes.empty()) {
    co_return protocol::log_entry_vector{};
  }
  protocol::log_entry_vector entries;
  if (max_size == 0) {
    max_size = UINT64_MAX;
  }
  size_t start = 0;
  size_t count = 1;
  uint64_t prev_filename = indexes.front().filename;
  for (size_t i = 1; i < indexes.size(); ++i) {
    if (indexes[i].filename != prev_filename) {
      // TODO: check the continuity of entry's index
      max_size = co_await _segments[prev_filename]->query(
          indexes.subspan(start, count), entries, max_size);
      if (max_size == 0) {
        break;
      }
      prev_filename = indexes[i].filename;
      count = 1;
      start = i;
    } else {
      count++;
    }
  }
  if (max_size != 0) {
    co_await _segments[prev_filename]->query(
        indexes.subspan(start, count), entries, max_size);
  }
  co_return std::move(entries);
}

seastar::future<> segment_manager::sync() {
  co_await with_lock(_mutex, [this]() ->future<> {
    // sometimes when a worker try to sync a segment, it maybe already synced by
    // another worker due to coroutine scheduling, and this worker's update is
    // included in that sync, skip
    if (_need_sync && !_segments.empty()) {
      _stats._sync++;
      co_await _segments.rbegin()->second->sync();
    }
    _need_sync = false;
    co_return;
  });
}

void segment_manager::must_open() const {
  if (!_open) [[unlikely]] {
    throw util::runtime_error(code::closed);
  }
}

future<> segment_manager::parse_existing_segments(directory_entry s) {
  auto [shard_id, filename] = segment::parse_name(s.name);
  if (filename == segment::INVALID_FILENAME) {
    l.warn("segment_manager::parse_existing_segments: invalid {}", s.name);
    co_return;
  }
  if (shard_id != this_shard_id()) {
    co_return;
  }
  auto path = fmt::format("{}/{}", _log_dir, s.name);
  auto seg = co_await segment::open(filename, std::move(path), true);
  _stats._new_segment++;
  co_await seg->list_update([this] (const auto& up, auto e) {
    return update_index(up, e);
  });
  _segments.emplace(filename, std::move(seg));
}

future<> segment_manager::update_index(
    const protocol::update& up, index::entry e) {
  if (_index_group.update(up, e)) {
    _segments_ref_count[e.filename]++;
  }
  co_return;
}

future<> segment_manager::rolling() {
  _stats._new_segment++;
  if (!_segments.empty()) {
    co_await _segments.rbegin()->second->sync();
    // some nodes maybe still reading from this segment, leave it to compaction
    // co_await _segments.rbegin()->second->close();
  }
  auto p = segment::form_path(_log_dir, _next_filename);
  l.info("segment_manager::rolling: start new segment {}", p);
  auto s = co_await segment::open(_next_filename, std::move(p));
  _segments.emplace_hint(_segments.end(), _next_filename, std::move(s));
  co_await sync_directory(_log_dir);
  _next_filename++;
}

future<> segment_manager::gc_service() {
  try {
    l.info("segment_manager::gc_service: online");
    while (_open) {
      auto filename = co_await _obsolete_queue->pop_eventually();
      _stats._del_segment++;
      co_await _segments[filename]->close();
      co_await _segments[filename]->remove();
      _segments.erase(filename);
      // TODO: sync_directory once for multiple segments
      co_await sync_directory(_log_dir);
    }
  } catch (util::runtime_error& e) {
    if (e.error_code() != code::closed) {
      l.error("segment_manager::gc_service: unexpected exception:{}", e.what());
    }
  }
  l.info("segment_manager::gc_service: offline");
}

future<> segment_manager::compaction(group_id id) {
  auto ni = _index_group.get_node_index(id);
  auto obsoletes = ni->compaction();
  for (auto file : obsoletes) {
    assert(_segments_ref_count.contains(file));
    if (--_segments_ref_count[file] == 0) {
      _segments_ref_count.erase(file);
      co_await _obsolete_queue->push_eventually(std::move(file));
    }
  }
}

}  // namespace rafter::storage
