//
// Created by jason on 2021/10/13.
//

#include "segment_manager.hh"

#include <seastar/core/reactor.hh>
#include <span>

#include "protocol/serializer.hh"
#include "rafter/config.hh"
#include "storage/logger.hh"
#include "util/error.hh"
#include "util/file.hh"
#include "util/util.hh"

namespace rafter::storage {

using namespace protocol;
using namespace std;

segment_manager::segment_manager(function<unsigned(uint64_t)> func)
  : _partitioner(std::move(func))
  , _gc_worker("segment_gc", config::shard().wal_gc_queue_capacity, l) {}

future<> segment_manager::start() {
  _log_dir = filesystem::path(config::shard().data_dir).append("wal");
  _boot_dir = filesystem::path(config::shard().data_dir).append("bootstrap");
  co_await recursive_touch_directory(_log_dir);
  co_await recursive_touch_directory(_boot_dir);
  l.info(
      "segment_manager::start: dir:{}, rolling_size:{}, gc_queue_cap:{}",
      _log_dir,
      config::shard().wal_rolling_size,
      config::shard().wal_gc_queue_capacity);
  file dir = co_await open_directory(_log_dir);
  co_await dir
      .list_directory([this](auto e) { return parse_existing_segments(e); })
      .done();
  // always write to a new file after bootstrap
  _next_filename = _segments.empty() ? 1 : _segments.rbegin()->first + 1;
  co_await rolling();
  co_await dir.close();
  co_await recovery_compaction();
  // TODO(jyc): run obsolete segments deleter on shard 0 only, shard X passes
  // the
  //  path to shard 0's deleter
  _gc_worker.start(
      [this](auto& t, bool& open) { return this->gc_service(t, open); });
}

future<> segment_manager::stop() {
  co_await _gc_worker.close();
  l.info("segment_manager::stop: stopped");
}

stats segment_manager::stats() const noexcept { return _stats; }

future<vector<group_id>> segment_manager::list_nodes() {
  vector<group_id> nodes;
  co_await with_file(open_directory(_boot_dir), [this, &nodes](file f) {
    return f
        .list_directory([this, &nodes](directory_entry entry) {
          if (entry.type && entry.type != directory_entry_type::regular) {
            l.warn("invalid bootstrap file type {}", entry.name);
            return make_ready_future<>();
          }
          group_id id;
          auto c = util::parse_file_name(entry.name, '_', id.cluster, id.node);
          if (c != 2) {
            l.warn("invalid bootstrap file format {}", entry.name);
            return make_ready_future<>();
          }
          if (this_shard_id() == _partitioner(id.cluster)) {
            nodes.emplace_back(id);
          }
          return make_ready_future<>();
        })
        .done();
  });
  co_return std::move(nodes);
}

future<> segment_manager::save_bootstrap(group_id id, const bootstrap& info) {
  auto name = fmt::format("{:020d}_{:020d}", id.cluster, id.node);
  co_await util::create_file(_boot_dir, name + ".tmp", write_to_string(info));
  co_await rename_file(
      filesystem::path(_boot_dir).append(name + ".tmp").string(),
      filesystem::path(_boot_dir).append(name).string());
  co_await sync_directory(_boot_dir);
}

future<std::optional<bootstrap>> segment_manager::load_bootstrap(group_id id) {
  auto name = fmt::format("{:020d}_{:020d}", id.cluster, id.node);
  auto exist =
      co_await file_exists(filesystem::path(_boot_dir).append(name).string());
  if (!exist) {
    co_return std::optional<bootstrap>{};
  }
  co_return co_await util::read_file(_boot_dir, name)
      .then([](temporary_buffer<char> buf) {
        return read_from_string(
            {buf.get(), buf.size()}, util::type<bootstrap>());
      });
}

future<> segment_manager::save(std::span<update_pack> updates) {
  return with_lock(_mtx, [=]() -> future<> {
    // FIXME(jyc): must explicitly copy the span, compiler issue? clang++-12.0.1
    std::span<update_pack> ups = updates;
    bool need_sync = false;
    for (const auto& up : ups) {
      bool sync = co_await append(up.update);
      need_sync = need_sync || sync;
    }
    if (need_sync) {
      co_await sync();
    }
    for (auto& up : ups) {
      up.done.set_value();
    }
  });
}

future<size_t> segment_manager::query_entries(
    group_id id, hint range, log_entry_vector& entries, uint64_t max_bytes) {
  _stats._query_entry++;
  if (max_bytes == 0) {
    co_return max_bytes;
  }
  auto ni = _index_group.get_node_index(id);
  auto compacted_to = ni->compacted_to();
  if (range.low <= compacted_to) {
    co_return max_bytes;
  }
  auto indexes_vec = ni->query(range);
  auto indexes = std::span<const index::entry>(indexes_vec);
  if (indexes.empty()) {
    co_return max_bytes;
  }
  reference_segments(indexes);
  size_t start = 0;
  size_t count = 1;
  uint64_t prev_filename = indexes.front().filename;
  for (size_t i = 1; i < indexes.size(); ++i) {
    if (indexes[i].filename != prev_filename) {
      // TODO(jyc): check the continuity of entry's index
      max_bytes = co_await _segments[prev_filename]->query(
          indexes.subspan(start, count), entries, max_bytes);
      if (max_bytes == 0) {
        break;
      }
      prev_filename = indexes[i].filename;
      count = 1;
      start = i;
    } else {
      count++;
    }
  }
  if (max_bytes != 0) {
    max_bytes = co_await _segments[prev_filename]->query(
        indexes.subspan(start, count), entries, max_bytes);
  }
  co_await unreference_segments(indexes);
  co_return max_bytes;
}

future<raft_state> segment_manager::query_raft_state(
    group_id id, uint64_t last_index) {
  _stats._query_state++;

  // TODO(jyc): read from segment or read from _index_group?
  auto st = _index_group.get_hard_state(id);
  raft_state r;
  if (st.empty()) {
    l.warn("{} segment_manager::query_raft_state: no data", id);
    co_return r;
  }
  r.hard_state = st;
  auto ie = _index_group.query(id, {.low = last_index + 1, .high = UINT64_MAX});
  if (!ie.empty()) {
    uint64_t prev_idx = ie.front().first_index - 1;
    for (auto e : ie) {
      if (prev_idx + 1 != e.first_index) {
        l.error(
            "{} segment_manager::query_raft_state: missing index:{}",
            id,
            prev_idx + 1);
        co_return coroutine::make_exception(util::corruption_error());
      }
      prev_idx = e.last_index;
    }
    r.first_index = last_index + 1;
    r.entry_count = ie.back().last_index - r.first_index;
  }
  co_return r;
}

future<snapshot_ptr> segment_manager::query_snapshot(group_id id) {
  _stats._query_snap++;

  auto i = _index_group.query_snapshot(id);
  if (i.empty()) {
    l.warn("{} snapshot index not found", id);
    co_return snapshot_ptr{};
  }
  auto it = _segments.find(i.filename);
  if (it == _segments.end()) {
    l.error(
        "{} segment_manager::query_snapshot: segment:{} not found",
        id,
        i.filename);
    co_return coroutine::make_exception(util::corruption_error());
  }
  reference_segments({&i, 1});
  auto* segment = it->second.get();
  auto up = co_await segment->query(i);
  co_await unreference_segments({&i, 1});
  if (up.snapshot->log_id.index == log_id::INVALID_INDEX) {
    l.error("{} segment_manager::query_snapshot: empty", id);
    co_return coroutine::make_exception(util::corruption_error());
  }
  co_return std::move(up.snapshot);
}

future<> segment_manager::remove(group_id id, uint64_t index) {
  return with_lock(
             _mtx,
             [=]() -> future<> {
               _stats._remove++;
               update comp{
                   .gid = id,
                   .state =
                       {
                           .commit = index,
                       },
               };
               co_await append(comp);
               co_await sync();
             })
      .then([=] { return compaction(id); });
}

future<> segment_manager::remove_node(protocol::group_id id) {
  auto ni = _index_group.get_node_index(id);
  auto files = ni->files();
  _index_group.remove(id);
  for (auto file : files) {
    assert(_segments_ref_count.contains(file));
    if (--_segments_ref_count[file] == 0) {
      _segments_ref_count.erase(file);
      co_await _gc_worker.push_eventually(std::move(file));
    }
  }
  std::string name = filesystem::path(_log_dir).append(
      fmt::format("BOOT-{}-{}", id.cluster, id.node));
  co_await remove_file(name);
  co_await sync_directory(_log_dir);
}

future<> segment_manager::import_snapshot(protocol::snapshot_ptr snapshot) {
  return with_lock(_mtx, [=]() -> future<> {
    auto id = snapshot->group_id;
    auto info = bootstrap{.join = true, .smtype = snapshot->smtype};
    co_await remove_node(id);
    co_await save_bootstrap(id, info);
    auto up = update{
        .gid = id,
        .state =
            {.term = snapshot->log_id.term, .commit = snapshot->log_id.index},
        .snapshot = snapshot};
    bool need_sync = co_await append(up);
    if (need_sync) {
      co_await sync();
    }
  });
}

future<bool> segment_manager::append(const update& up) {
  if (up.snapshot && up.snapshot->group_id != up.gid) [[unlikely]] {
    l.warn(
        "snapshot:{} does not match update:{}", up.snapshot->group_id, up.gid);
  }
  if (!up.snapshot && up.entries_to_save.empty() && up.state.empty()) {
    co_return false;
  }
  _stats._append++;
  _stats._append_entry += up.entries_to_save.size();
  _stats._append_state += up.state.empty() ? 0 : 1;
  _stats._append_snap += up.snapshot.operator bool();

  auto prev_state = _index_group.get_hard_state(up.gid);
  bool need_sync = up.snapshot || !up.entries_to_save.empty() ||
                   prev_state.term != up.state.term ||
                   prev_state.vote != up.state.vote;
  auto it = _segments.rbegin();
  auto filename = it->first;
  auto* segment = it->second.get();
  auto offset = segment->bytes();
  auto new_offset = co_await segment->append(up);
  index::entry e{
      .filename = filename,
      .offset = offset,
      .length = new_offset - offset,
  };
  if (new_offset >= config::shard().wal_rolling_size) {
    co_await rolling();
    need_sync = false;
  }
  co_await update_index(up, e);
  co_return need_sync;
}

future<> segment_manager::sync() {
  if (!_segments.empty()) {
    _stats._sync++;
    co_await _segments.rbegin()->second->sync();
  }
  co_return;
}

string segment_manager::debug_string() const noexcept {
  stringstream ss;
  for_each(_segments.begin(), _segments.end(), [this, &ss](const auto& seg) {
    size_t ref = 0;
    if (_segments_ref_count.contains(seg.first)) {
      ref = _segments_ref_count.at(seg.first);
    }
    ss << seg.first << "." << ref << ";";
  });
  return fmt::format(
      "segment_manager[shard:{}, dir:{}, next:{}, managed_segments:{}]",
      this_shard_id(),
      _log_dir,
      _next_filename,
      ss.str());
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
  co_await seg->list_update(
      [this](const auto& up, auto e) { return update_index(up, e); });
  _segments.emplace(filename, std::move(seg));
}

future<> segment_manager::recovery_compaction() {
  // TODO(jyc): use callback and avoid this allocation
  auto groups = _index_group.managed_groups();
  for (auto gid : groups) {
    co_await compaction(gid);
  }
  l.info("segment_manager::recovery_compaction: done");
  co_return;
}

future<> segment_manager::update_index(const update& up, index::entry e) {
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

future<> segment_manager::gc_service(std::vector<uint64_t>& segs, bool& open) {
  for (auto seg : segs) {
    if (!open) {
      // exit ASAP when marked not open
      break;
    }
    try {
      auto s = std::move(_segments[seg]);
      _segments.erase(seg);
      co_await s->close();
      co_await s->remove();
      _stats._del_segment++;
    } catch (std::runtime_error& e) {
      l.warn(
          "segment_manager::gc_service: failed to remove segment:{}, {}",
          segment::form_path(_log_dir, seg),
          e.what());
    }
  }
  co_await sync_directory(_log_dir);
  segs.clear();
}

future<> segment_manager::compaction(group_id id) {
  auto ni = _index_group.get_node_index(id);
  auto obsoletes = ni->compaction();
  for (auto file : obsoletes) {
    assert(_segments_ref_count.contains(file));
    if (--_segments_ref_count[file] == 0) {
      _segments_ref_count.erase(file);
      co_await _gc_worker.push_eventually(std::move(file));
    }
  }
}

void segment_manager::reference_segments(
    std::span<const index::entry> indexes) {
  for (const auto& idx : indexes) {
    _segments_ref_count[idx.filename]++;
  }
}

future<> segment_manager::unreference_segments(
    std::span<const index::entry> indexes) {
  for (const auto& idx : indexes) {
    auto file = idx.filename;
    assert(_segments_ref_count.contains(file));
    if (--_segments_ref_count[file] == 0) {
      _segments_ref_count.erase(file);
      co_await _gc_worker.push_eventually(std::move(file));
    }
  }
}

}  // namespace rafter::storage
