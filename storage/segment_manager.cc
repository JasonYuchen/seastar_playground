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

segment_manager::segment_manager(nodehost::config_ptr config)
    : _config(std::move(config)) {
}

future<> segment_manager::start() {
  _log_dir = filesystem::path(_config->data_dir).append("wal");
  _obsolete_queue = std::make_unique<seastar::queue<uint64_t>>(
      _config->wal_gc_queue_capacity);
  l.info("segment_manager::start: dir:{}, rolling_size:{}, gc_queue_cap:{}",
         _log_dir, _config->wal_rolling_size, _config->wal_gc_queue_capacity);
  file dir = co_await open_directory(_log_dir);
  co_await dir.list_directory(
                  [this](auto e){ return parse_existing_segments(e); }).done();
  // always write to a new file after bootstrap
  _next_filename = _segments.empty() ? 1 : _segments.rbegin()->first + 1;
  co_await rolling();
  co_await dir.close();
  _gc_service = gc_service();
  _open = true;
}

future<> segment_manager::stop() {
  _open = false;
  _obsolete_queue->abort(std::make_exception_ptr(
      util::runtime_error(code::closed)));
  co_await _gc_service->discard_result();
  l.info("segment_manager::stop: stopped");
  co_return;
}

future<bool> segment_manager::append(const protocol::update& up) {
  must_open();
  if (!up.snapshot && up.entries_to_save.empty() && up.state.empty()) {
    co_return false;
  }
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
    if (new_offset >= _config->wal_rolling_size) {
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
  update compaction {
      .gid = id,
      .state = {
          .commit = index,
      },
  };
  co_await append(compaction);
  auto ni = _index_group.get_node_index(id);
  auto obsoletes = ni->compaction();
  for (auto file : obsoletes) {
    if (--_segments_ref_count[file] == 0) {
      co_await _segments[file]->close();
      _segments.erase(file);
      _segments_ref_count.erase(file);
      co_await _obsolete_queue->push_eventually(std::move(file));
    }
  }
  co_return;
}

future<snapshot_ptr> segment_manager::query_snapshot(group_id id) {
  static const char* err = "{gid} segment_manager::query_snapshot: {msg}";
  auto ni = _index_group.get_node_index(id);
  auto i = ni->query_snapshot();
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
  auto ni = _index_group.get_node_index(id);
  auto st = _index_group.get_hard_state(id);
  if (st.empty()) {
    co_return coroutine::make_exception(util::runtime_error(
        l, code::no_data, err, id.to_string(), "no saved data"));
  }
  // TODO
  co_return raft_state{};
}

future<protocol::log_entry_vector> segment_manager::query_entries(
    protocol::group_id id, protocol::hint range, uint64_t max_size) {
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
  co_return co_await with_lock(_mutex, [this]() ->future<> {
    // sometimes when a worker try to sync a segment, it maybe already synced by
    // another worker due to coroutine scheduling, and this worker's update is
    // included in that sync, skip
    if (_need_sync && !_segments.empty()) {
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
  auto [shard_id, filename] = parse_segment_name(s.name);
  if (filename == INVALID_FILENAME) {
    l.warn("segment_manager::parse_existing_segments: invalid {}", s.name);
    co_return;
  }
  if (shard_id != this_shard_id()) {
    co_return;
  }
  auto path = fmt::format("{}/{}", _log_dir, s.name);
  auto seg = co_await segment::open(filename, std::move(path), true);
  co_await seg->list_update([this] (const auto& up, auto e) {
    return update_index(up, e);
  });

  _segments.emplace(filename, std::move(seg));
  co_return;
}

future<> segment_manager::update_index(
    const protocol::update& up, index::entry e) {
  if (_index_group.update(up, e)) {
    _segments_ref_count[e.filename]++;
  }
  co_return;
}

future<> segment_manager::rolling() {
  if (!_segments.empty()) {
    co_await _segments.rbegin()->second->sync();
    co_await _segments.rbegin()->second->close();
  }
  auto p = fmt::format("{}/{:05d}_{:020d}.{}",
                       _log_dir, this_shard_id(), _next_filename, LOG_SUFFIX);
  l.info("segment_manager::rolling: start new segment {}", p);
  auto s = co_await segment::open(_next_filename, std::move(p));
  _segments.emplace_hint(_segments.end(), _next_filename, std::move(s));
  co_await sync_directory(_log_dir);
  _next_filename++;
  co_return;
}

future<> segment_manager::gc_service() {
  try {
    l.info("segment_manager::gc_service: online");
    while (_open) {
      auto filename = co_await _obsolete_queue->pop_eventually();
      auto path = fmt::format("{}/{:05d}_{:020d}.{}",
                              _log_dir, this_shard_id(), filename, LOG_SUFFIX);
      co_await remove_file(path);
      co_await sync_directory(_log_dir);
    }
  } catch (util::runtime_error& e) {
    l.warn("segment_manager::gc_service: caught exception:{}", e.what());
  }
  l.info("segment_manager::gc_service: offline");
  co_return;
}

pair<unsigned, uint64_t> segment_manager::parse_segment_name(string_view name) {
  auto sep_pos = name.find('_');
  auto suffix_pos = name.find('.');
  if (sep_pos == string::npos || suffix_pos == string::npos) {
    return {0, INVALID_FILENAME};
  }
  name.remove_suffix(name.size() - suffix_pos);
  unsigned shard_id = 0;
  uint64_t filename = 0;
  auto r = from_chars(name.data(), name.data() + sep_pos, shard_id);
  if (r.ec != errc()) {
    return {0, INVALID_FILENAME};
  }
  name.remove_prefix(sep_pos + 1);
  r = from_chars(name.data(), name.data() + name.size(), filename);
  if (r.ec != errc()) {
    return {0, INVALID_FILENAME};
  }
  return {shard_id, filename};
}


}  // namespace rafter::storage
