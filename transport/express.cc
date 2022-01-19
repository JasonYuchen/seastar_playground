//
// Created by jason on 2022/1/12.
//

#include "express.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/util/defer.hh>

#include "transport/exchanger.hh"
#include "transport/logger.hh"

namespace rafter::transport {

using namespace seastar;

future<> express::start() {
  _worker.start([this](auto& t, bool& open) { return this->main(t, open); });
  co_return;
}

future<> express::close() {
  _open = false;
  co_await _worker.close();
  if (_split_task) {
    co_await _split_task->discard_result();
  }
  co_await _sink.flush();
  co_await _sink.close();
  co_return;
}

future<> express::send(protocol::snapshot_ptr snapshot) {
  // TODO(jyc): handle split failure
  _split_task = split(std::move(snapshot));
  co_return;
}

future<> express::split(
    const protocol::snapshot& snapshot,
    const std::string& file_path,
    uint64_t start_chunk_id,
    protocol::snapshot_file_ptr file,
    std::vector<protocol::snapshot_chunk_ptr>& chunks) {
  auto f = co_await open_file_dma(file_path, open_flags::ro);
  auto defer_close = seastar::defer([&f] { (void)f.close().discard_result(); });
  uint64_t file_size = co_await f.size();
  if (file_size == 0) {
    throw util::out_of_range_error(fmt::format("empty file:{}", file_path));
  }
  auto fstream = make_file_input_stream(f);
  uint64_t snapshot_chunk_size = _exchanger.config().snapshot_chunk_size;
  uint64_t file_chunk_count = (file_size - 1) / snapshot_chunk_size + 1;
  for (uint64_t i = 0; i < file_chunk_count; ++i) {
    auto& c = chunks.emplace_back(make_lw_shared<protocol::snapshot_chunk>());
    c->group_id = snapshot.group_id;
    c->log_id = snapshot.log_id;
    c->from = _local.node;
    c->id = start_chunk_id + i;
    c->size = (i == file_chunk_count - 1) ? file_size % snapshot_chunk_size
                                          : snapshot_chunk_size;
    auto buf = co_await fstream.read_exactly(c->size);
    c->data = std::string(buf.get(), buf.size());
    c->membership = snapshot.membership;
    c->file_path = file_path;
    c->file_size = file_size;
    c->file_chunk_id = i;
    c->file_chunk_count = file_chunk_count;
    c->file_info = file;
    c->on_disk_index = snapshot.on_disk_index;
    c->witness = snapshot.witness;
  }
  co_return;
}

future<> express::split(protocol::snapshot_ptr snapshot) {
  // TODO(jyc): split the snapshot into chunks and push to sink
  uint64_t chunk_id = 0;
  std::vector<protocol::snapshot_chunk_ptr> chunks;
  co_await split(*snapshot, snapshot->file_path, chunk_id, {}, chunks);
  chunk_id += chunks.size();
  for (auto file : snapshot->files) {
    // TODO(jyc): validation size?
    co_await split(*snapshot, file->file_path, chunk_id, file, chunks);
    chunk_id += chunks.size();
  }
  // TODO(jyc): use file size to determine the total number of chunks at first
  std::for_each(chunks.begin(), chunks.end(), [n = chunks.size()](auto& chunk) {
    chunk->count = n;
  });
}

future<> express::main(
    std::vector<protocol::snapshot_chunk_ptr>& chunks, bool& open) {
  if (!_open) {
    co_return;
  }
  try {
    for (const auto& chunk : chunks) {
      if (!open) {
        break;
      }
      co_await _sink(chunk);
      if (chunk->id == chunk->count) {
        (void)_exchanger.notify_successful(_target).discard_result();
      }
    }
  } catch (... /*TODO(jyc): rpc error, report unreachable*/) {
    l.error("express::main: {} {}", _target, std::current_exception());
    (void)_exchanger.notify_unreachable(_target).discard_result();
  }
  co_return;
}

}  // namespace rafter::transport
