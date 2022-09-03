//
// Created by jason on 2022/1/12.
//

#include "express.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>

#include "rafter/config.hh"
#include "server/snapshot_context.hh"
#include "transport/exchanger.hh"
#include "transport/logger.hh"
#include "transport/snapshot_chunk_tracker.hh"
#include "util/error.hh"

namespace rafter::transport {

using namespace protocol;

future<> express::stop() {
  auto s = _senders.begin();
  while (s != _senders.end()) {
    co_await s->second->stop();
    s = _senders.begin();
  }
  auto r = _receivers.begin();
  while (r != _receivers.end()) {
    co_await r->second->stop();
    r = _receivers.begin();
  }
  co_return;
}

future<> express::send(message m) {
  auto key = pair{m.cluster, m.from, m.to};
  if (_senders.contains(key)) {
    // TODO(jyc): limit max concurrent senders
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
      s->start(m.snapshot)
          .handle_exception([this, pair = key](std::exception_ptr e) {
            l.error("express::sender::start: {}", e);
            // TODO(jyc): report unreachable?
            return _exchanger.notify_unreachable({pair.cluster, pair.to});
          })
          .finally([this, key] { _senders.erase(key); });
  co_return;
}

future<> express::receive(
    pair key, log_id lid, srpc::source<snapshot_chunk> source) {
  if (_receivers.contains(key)) {
    // TODO(jyc): limit max concurrent receivers
    l.warn(
        "express::receive: ongoing at cluster:{}, from:{}, to:{}",
        key.cluster,
        key.from,
        key.to);
    co_return;
  }
  auto s = make_lw_shared<receiver>(_exchanger, key);
  _receivers.emplace(key, s);
  s->_task =
      s->start(lid, std::move(source))
          .handle_exception([this, pair = key](std::exception_ptr e) {
            l.error("express::receiver::start: {}", e);
            // TODO(jyc): report unreachable?
            return _exchanger.notify_unreachable({pair.cluster, pair.to});
          })
          .finally([this, key] { _receivers.erase(key); });
  co_return;
}

future<> express::sender::start(snapshot_ptr snapshot) {
  // TODO(jyc): optimization: read from files and send out can be pipelined
  // TODO(jyc): what can we do in face of a extremely slow peer ?
  // TODO(jyc): can we stop sending once leader change occur
  group_id remote{_pair.cluster, _pair.to};
  group_id local{_pair.cluster, _pair.from};
  try {
    l.info(
        "express:sender::start: sending snapshot chunks from {} to {}",
        local,
        remote);
    auto sink = co_await _exchanger.make_sink_for_snapshot_chunk(
        _pair.cluster, _pair.from, _pair.to, snapshot->log_id);
    uint64_t chunk_id = 0;
    uint64_t total_chunks = 0;
    uint64_t snapshot_chunk_size = config::shard().snapshot_chunk_size;
    total_chunks += (snapshot->file_size - 1) / snapshot_chunk_size + 1;
    for (const auto& file : snapshot->files) {
      total_chunks += (file->file_size - 1) / snapshot_chunk_size + 1;
    }
    co_await split_and_send(snapshot, {}, total_chunks, chunk_id, sink);
    for (const auto& file : snapshot->files) {
      co_await split_and_send(snapshot, file, total_chunks, chunk_id, sink);
    }
    co_await sink.flush();
    co_await sink.close();
    co_await _exchanger.notify_snapshot_status(remote, false);
    co_return;
  } catch (util::logic_error& e) {
    l.error("express::sender::start: {}", e.what());
  } catch (util::closed_error& e) {
    l.info("express::sender::start: closed {}", e.what());
  }
  co_await _exchanger.notify_snapshot_status(remote, true);
  co_return;
}

future<> express::sender::stop() {
  l.info(
      "express::sender[{}:{}->{}] stopping...",
      _pair.cluster,
      _pair.from,
      _pair.to);
  _close = true;
  if (_task) {
    return _task->handle_exception([](std::exception_ptr e) {
      l.warn("express::sender::stop: exception:{}", e);
    });
  }
  return make_ready_future<>();
}

future<> express::sender::split_and_send(
    snapshot_ptr snapshot,
    snapshot_file_ptr file,
    uint64_t total_chunks,
    uint64_t& chunk_id,
    srpc::sink<snapshot_chunk>& sink) const {
  if (_close) {
    return make_exception_future<>(util::closed_error());
  }
  const auto& file_path = file ? file->file_path : snapshot->file_path;
  auto file_size = file ? file->file_size : snapshot->file_size;
  auto func = [&, total_chunks](class file& f) -> future<> {
    uint64_t actual_file_size = co_await f.size();
    if (file_size != actual_file_size) {
      co_await coroutine::return_exception(
          util::failed_precondition_error(fmt::format(
              "inconsistent file size, expect:{}, actual:{}",
              file_size,
              actual_file_size)));
    }
    if (file_size == 0) {
      co_await coroutine::return_exception(
          util::out_of_range_error(fmt::format("empty file:{}", file_path)));
    }
    auto fstream = make_file_input_stream(f);
    uint64_t snapshot_chunk_size = config::shard().snapshot_chunk_size;
    uint64_t file_chunk_count = (file_size - 1) / snapshot_chunk_size + 1;
    for (uint64_t i = 0; i < file_chunk_count; ++i) {
      if (_close) {
        co_await coroutine::return_exception(util::closed_error());
      }
      snapshot_chunk chunk;
      chunk.group_id = {_pair.cluster, _pair.to};
      chunk.log_id = snapshot->log_id;
      chunk.from = _pair.from;
      chunk.id = chunk_id++;
      chunk.count = total_chunks;
      chunk.size = (i == file_chunk_count - 1) ? file_size % snapshot_chunk_size
                                               : snapshot_chunk_size;
      auto buf = co_await fstream.read_exactly(chunk.size);
      chunk.data = std::string(buf.get(), buf.size());
      chunk.membership = snapshot->membership;
      chunk.file_path = file_path;
      chunk.file_size = file_size;
      chunk.file_chunk_id = i;
      chunk.file_chunk_count = file_chunk_count;
      chunk.file_info = file;
      chunk.on_disk_index = snapshot->on_disk_index;
      chunk.witness = snapshot->witness;
      co_await sink(chunk);
    }
    co_return;
  };
  return with_file(open_file_dma(file_path, open_flags::ro), std::move(func))
      .handle_exception([&sink](std::exception_ptr ep) {
        return sink.close().then([ep = std::move(ep)]() mutable {
          return make_exception_future<>(std::move(ep));
        });
      });
}

future<> express::receiver::start(
    log_id lid, srpc::source<snapshot_chunk> source) {
  // TODO(jyc): optimization: receive chunks and write to files can be pipelined
  // TODO(jyc): what can we do in face of a extremely slow peer ?
  // TODO(jyc): can we stop receiving once leader change occur
  // TODO(jyc): snapshot data validation

  // TODO(jyc):
  //  1. create temp dir to hold data
  //  2. receive and re-construct snapshot and relating files
  //  3. move(rename) files to final locations
  //  4. done and cleanup
  group_id remote{_pair.cluster, _pair.from};
  group_id local{_pair.cluster, _pair.to};
  l.info(
      "express::receiver::start: receiving snapshot chunks from {} to {}",
      remote,
      local);
  auto ctx = server::snapshot_context(
      _exchanger.get_snapshot_dir(local),
      lid.index,
      _pair.from,
      server::snapshot_context::mode::receiving);
  co_await ctx.create_tmp_dir();
  snapshot_chunk_tracker tracker;
  while (true) {
    auto data = co_await source();
    if (!data) {
      break;
    }
    auto& chunk = std::get<0>(*data);
    // TODO(jyc): check if this node is removed here
    if (chunk.file_chunk_id != 0) {
      co_await ctx.remove_tmp_dir();
      co_await coroutine::return_exception(util::short_read_error());
    }
    // create file, write the first one, read more
    tracker.record(chunk);
    std::filesystem::path path{ctx.get_tmp_dir()};
    path.append(chunk.file_path);
    auto func = [&source, &first = chunk, &tracker](file f) -> future<> {
      auto fstream = co_await make_file_output_stream(f);
      co_await fstream.write(first.data);
      for (uint64_t id = 1; id < first.file_chunk_count; ++id) {
        auto data = co_await source();
        if (!data) {
          co_await coroutine::return_exception(util::short_read_error());
        }
        auto& chunk = std::get<0>(*data);
        tracker.record(chunk);
        co_await fstream.write(chunk.data);
      }
      co_await fstream.flush();
      co_await fstream.close();
    };
    auto of = open_flags::create | open_flags::wo | open_flags::truncate |
              open_flags::dsync;
    co_await with_file(open_file_dma(path.string(), of), std::move(func))
        .handle_exception([&ctx](std::exception_ptr ex) {
          l.error(
              "remove temporary director:{} due to exception:{}",
              ctx.get_tmp_dir(),
              ex);
          return ctx.remove_tmp_dir();
        });

    //      co_await open_file_dma(path.string(), create | wo | truncate |
    //      dsync)
    //          .then([&source, &prev_id](file f) {
    //            return make_file_output_stream(f).then(
    //                [&source, &prev_id](output_stream<char>&& out) {
    //                  return do_with(
    //                      std::move(out),
    //                      [&source, &prev_id](output_stream<char>& out) {
    //                        return repeat([&source, &prev_id, &out] {
    //                          return source().then(
    //                              [&prev_id,
    //                               &out](std::optional<std::tuple<snapshot_chunk>>
    //                                         data) {
    //                                if (!data.has_value()) {
    //                                  return
    //                                  make_ready_future<stop_iteration>(
    //                                      stop_iteration::yes);
    //                                }
    //                                auto& chunk = std::get<0>(*data);
    //                                if (prev_id + 1 != chunk.file_chunk_id)
    //                                {
    //                                  // early stop
    //                                  return
    //                                  make_ready_future<stop_iteration>(
    //                                      stop_iteration::yes);
    //                                }
    //                                prev_id = chunk.file_chunk_id + 1;
    //                                return out.write(chunk.data)
    //                                    .then([done = prev_id ==
    //                                                  chunk.file_chunk_count]
    //                                                  {
    //                                      return
    //                                      make_ready_future<stop_iteration>(
    //                                          done ? stop_iteration::yes
    //                                               : stop_iteration::no);
    //                                    });
    //                              });
    //                        });
    //                      });
    //                });
    //          });

    if (tracker.have_all_chunks()) {
      l.debug("express::receiver::start: received last chunk");
      // TODO(jyc): data validation
      auto msg = co_await tracker.finalize(ctx).handle_exception(
          [&ctx](std::exception_ptr ex) {
            l.error(
                "remove temporary director:{} due to exception:{}",
                ctx.get_tmp_dir(),
                ex);
            return ctx.remove_tmp_dir().then(
                [ex] { return make_exception_future<message>(ex); });
          });
      co_await _exchanger.notify_message(std::move(msg));
      co_await _exchanger.notify_snapshot(local, _pair.from);
      break;
    }
  }

  co_return;
}

future<> express::receiver::stop() {
  l.info(
      "express::receiver[{}:{}->{}] stopping...",
      _pair.cluster,
      _pair.from,
      _pair.to);
  _close = true;
  if (_task) {
    return _task->handle_exception([](std::exception_ptr e) {
      l.warn("express::receiver::stop: exception:{}", e);
    });
  }
  return make_ready_future<>();
}

}  // namespace rafter::transport
