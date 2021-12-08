//
// Created by jason on 2021/10/13.
//

#include "segment_manager.hh"

#include <charconv>

#include <seastar/core/reactor.hh>

#include "storage/storage.hh"

namespace rafter::storage {

using namespace protocol;
using namespace seastar;
using namespace std;

segment_manager::segment_manager(string log_dir)
    : _log_dir(std::move(log_dir))
    , _obsolete_queue(make_unique<seastar::queue<uint64_t>>(_gc_queue_length)) {
}

segment_manager::~segment_manager() {
  if (_open) {
    // TODO: warning unclosed manager
  }
}

future<> segment_manager::start() {
  l.info("touching segment data dir:{}", _log_dir);
  co_await recursive_touch_directory(_log_dir);
  file dir = co_await open_directory(_log_dir);
  co_await dir.list_directory([this](auto e) { return parse_existing_segments(e); }).done();
  // always write to a new file after bootstrap
  _next_filename = _segments.empty() ? 0 : _segments.rbegin()->first;
  co_await rolling();
  co_await dir.close();
  _open = true;
  _gc_service = gc_service();
}

future<> segment_manager::stop() {
  _open = false;
  _obsolete_queue->abort(std::make_exception_ptr(std::runtime_error("closed")));
  // TODO(jason):
  co_await _gc_service->discard_result();
  l.info("segment manager closed");
  co_return;
}

future<bool> segment_manager::append(const protocol::update& up) {
  if (!_open) {
    // TODO
    co_return coroutine::make_exception(std::runtime_error("closed"));
  }
  if (!up.snapshot && up.entries_to_save.empty() && up.state.empty()) {
    co_return false;
  }

  uint64_t curr_filename = _next_filename;
  auto e = co_await with_lock(_mutex, [this, &up]() -> future<index::entry> {
    auto it = _segments.rbegin();
    auto filename = it->first;
    auto segment = it->second.get();
    auto offset = segment->bytes();
    auto new_offset = co_await segment->append(up);
    index::entry e {
        .id = up.group_id,
        .filename = filename,
        .offset = offset,
        .length = new_offset - offset,
    };
    if (new_offset >= _rolling_size) {
      co_await rolling();
    }
    co_return e;
  });
  co_await update_index(up, e);
  if (_next_filename > curr_filename) {
    // already fsync-ed when rolling
    co_return false;
  }
  co_return up.snapshot ||
      !up.entries_to_save.empty() || up.state.term || up.state.vote;;
}

future<> segment_manager::remove(group_id id, uint64_t index) {
  update compaction {
      .group_id = id,
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

future<snapshot> segment_manager::query_snapshot(group_id id) {
  auto ni = _index_group.get_node_index(id);
  auto i = ni->query_snapshot();
  if (i.empty()) {
    co_return snapshot{};
  }
  // TODO
  co_return snapshot{};
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
      co_await _segments[prev_filename]->query(
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

future<> segment_manager::parse_existing_segments(directory_entry s) {
  auto [shard_id, filename] = parse_segment_name(s.name);
  if (filename == INVALID_FILENAME) {
    l.warn("invalid segment found, name:{}", s.name);
    co_return;
  }
  if (shard_id != this_shard_id()) {
    co_return;
  }
  std::string path = fmt::format("{}/{}", _log_dir, s.name);
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
  _next_filename++;
  if (!_segments.empty()) {
    co_await _segments.rbegin()->second->sync();
    co_await _segments.rbegin()->second->close();
  }
  string p = fmt::format("{}/{:05d}_{:020d}.{}",
                         _log_dir, this_shard_id(), _next_filename, LOG_SUFFIX);
  auto s = co_await segment::open(_next_filename, std::move(p));
  _segments.emplace_hint(_segments.end(), _next_filename, std::move(s));
  co_await sync_directory(_log_dir);
  co_return;
}

future<> segment_manager::gc_service() {
  while (_open) {
    l.info("segment gc service online");
    auto filename = co_await _obsolete_queue->pop_eventually();
    auto path = fmt::format("{}/{:05d}_{:020d}.{}",
                            _log_dir, this_shard_id(), filename, LOG_SUFFIX);
    co_await seastar::remove_file(path);
  }
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
