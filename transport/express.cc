//
// Created by jason on 2022/1/12.
//

#include "express.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/util/defer.hh>

#include "transport/exchanger.hh"
#include "transport/logger.hh"
#include "util/error.hh"

namespace rafter::transport {

using namespace seastar;
using namespace protocol;

future<> express::stop() {
  auto s = _senders.begin();
  while (s != _senders.end()) {
    s->second->_close = true;
    co_await s->second->stop();
    s = _senders.begin();
  }
  auto r = _receivers.begin();
  while (r != _receivers.end()) {
    s->second->_close = true;
    co_await r->second->stop();
    r = _receivers.begin();
  }
  co_return;
}

future<> express::send(message_ptr message) {
  auto key = pair{message->cluster, message->from, message->to};
  if (_senders.contains(key)) {
    l.warn(
        "express::send: ongoing at cluster:{}, from:{}, to:{}",
        key.cluster,
        key.from,
        key.to);
    co_return;
  }
  auto s = make_lw_shared<sender>(_exchanger, key);
  _senders.emplace(key, s);
  s->_task =
      s->start(message->snapshot).finally([this, key] { _senders.erase(key); });
  co_return;
}

future<> express::receive(pair key, rpc::source<snapshot_chunk_ptr> source) {
  if (_receivers.contains(key)) {
    l.warn(
        "express::receive: ongoing at cluster:{}, from:{}, to:{}",
        key.cluster,
        key.from,
        key.to);
    co_return;
  }
  auto s = make_lw_shared<receiver>(_exchanger, key);
  _receivers.emplace(key, s);
  s->_task = s->start(std::move(source)).finally([this, key] {
    _receivers.erase(key);
  });
  co_return;
}

future<> express::sender::start(snapshot_ptr snapshot) {
  // TODO(jyc)
  try {
    auto sink = co_await _exchanger.make_sink_for_snapshot_chunk(
        _pair.cluster, _pair.from, _pair.to);
    auto deferred = defer([&sink] { (void)sink.close().discard_result(); });
    uint64_t chunk_id = 0;
    uint64_t total_chunks = 0;
    uint64_t snapshot_chunk_size = _exchanger.config().snapshot_chunk_size;
    total_chunks += (snapshot->file_size - 1) / snapshot_chunk_size + 1;
    for (const auto& file : snapshot->files) {
      total_chunks += (file->file_size - 1) / snapshot_chunk_size + 1;
    }
    co_await split_and_send(snapshot, {}, total_chunks, chunk_id, sink);
    for (const auto& file : snapshot->files) {
      co_await split_and_send(snapshot, file, total_chunks, chunk_id, sink);
    }
    co_await sink.flush();
    (void)_exchanger.notify_successful({_pair.cluster, _pair.to})
        .discard_result();
  } catch (util::logic_error& e) {
    l.error("express::sender::start: {}", e.what());
  } catch (util::closed_error& e) {
    l.info("express::sender::start: closed {}", e.what());
  } catch (...) {
    l.error("express::sender::start: {}", std::current_exception());
    (void)_exchanger.notify_unreachable({_pair.cluster, _pair.to})
        .discard_result();
  }
  co_return;
}

future<> express::sender::stop() {
  if (_task) {
    return _task->discard_result();
  }
  return make_ready_future<>();
}

future<> express::sender::split_and_send(
    protocol::snapshot_ptr snapshot,
    protocol::snapshot_file_ptr file,
    uint64_t total_chunks,
    uint64_t& chunk_id,
    seastar::rpc::sink<protocol::snapshot_chunk_ptr>& sink) const {
  const auto& file_path = file ? file->file_path : snapshot->file_path;
  auto file_size = file ? file->file_size : snapshot->file_size;
  auto f = co_await open_file_dma(file_path, open_flags::ro);
  auto defer_close = seastar::defer([&f] { (void)f.close().discard_result(); });
  uint64_t actual_file_size = co_await f.size();
  if (file_size != actual_file_size) {
    throw util::failed_precondition_error(fmt::format(
        "inconsistent file size, expect:{}, actual:{}",
        file_size,
        actual_file_size));
  }
  if (file_size == 0) {
    throw util::out_of_range_error(fmt::format("empty file:{}", file_path));
  }
  auto fstream = make_file_input_stream(f);
  uint64_t snapshot_chunk_size = _exchanger.config().snapshot_chunk_size;
  uint64_t file_chunk_count = (file_size - 1) / snapshot_chunk_size + 1;
  for (uint64_t i = 0; i < file_chunk_count; ++i) {
    if (_close) {
      throw util::closed_error();
    }
    auto c = make_lw_shared<protocol::snapshot_chunk>();
    c->group_id = snapshot->group_id;
    c->log_id = snapshot->log_id;
    c->from = _pair.from;
    c->id = chunk_id++;
    c->count = total_chunks;
    c->size = (i == file_chunk_count - 1) ? file_size % snapshot_chunk_size
                                          : snapshot_chunk_size;
    auto buf = co_await fstream.read_exactly(c->size);
    c->data = std::string(buf.get(), buf.size());
    c->membership = snapshot->membership;
    c->file_path = file_path;
    c->file_size = file_size;
    c->file_chunk_id = i;
    c->file_chunk_count = file_chunk_count;
    c->file_info = file;
    c->on_disk_index = snapshot->on_disk_index;
    c->witness = snapshot->witness;
    co_await sink(c);
  }
  co_return;
}

future<> express::receiver::start(rpc::source<snapshot_chunk_ptr> source) {
  // TODO(jyc):
  //  1. create temp dir to hold data
  //  2. receive and re-construct snapshot and relating files
  //  3. move(rename) files to final locations
  //  4. done and cleanup
  co_return;
}

future<> express::receiver::stop() {
  if (_task) {
    return _task->discard_result();
  }
  return make_ready_future<>();
}

}  // namespace rafter::transport
