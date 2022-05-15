//
// Created by jason on 2022/4/28.
//

#include "snapshotter.hh"

#include <seastar/core/fstream.hh>

#include "rsm/logger.hh"
#include "util/error.hh"
#include "util/file.hh"

namespace rafter::rsm {

using namespace protocol;

snapshotter::snapshotter(
    protocol::group_id gid,
    storage::logdb &logdb,
    std::function<std::string(protocol::group_id)> snapshot_dir)
  : _gid(gid)
  , _logdb(logdb)
  , _snapshot_dir(std::move(snapshot_dir))
  , _dir(_snapshot_dir(_gid)) {}

future<bool> snapshotter::shrunk(protocol::snapshot_ptr snapshot) {
  // TODO(jyc) rsm is shrunk snapshot file
  co_return true;
}

future<server::snapshot> snapshotter::save(
    std::function<future<bool>(std::any, output_stream<char> &, files &)> saver,
    snapshot_metadata meta) {
  server::snapshot ss{
      .ctx = get_snapshot_context(
          meta.lid.index, meta.request.exported(), meta.request.path),
      .ss = make_lw_shared<snapshot>(),
  };
  co_await ss.ctx.create_tmp_dir();
  files fs;
  auto file_path = ss.ctx.get_tmp_file_path();
  auto header_buf = temporary_buffer<char>::aligned(4096, 4096);  // FIXME
  std::fill_n(header_buf.get_write(), header_buf.size(), 0);
  using enum open_flags;
  co_await open_file_dma(file_path, create | wo | truncate | dsync)
      .then([&](file f) {
        return make_file_output_stream(f).then([&](output_stream<char> &&out) {
          return do_with(std::move(out), [&](output_stream<char> &out) {
            // write a dummy header
            return out.write(header_buf.get(), header_buf.size())
                .then([&]() mutable {
                  return saver(std::move(meta.ctx), out, fs);
                })
                .then([&](bool dummy) {
                  ss.ss->dummy = dummy;
                  return out.flush();
                })
                .then([&out] { return out.close(); });
          });
        });
      });
  co_await fs.prepare_files(ss.ctx.get_tmp_dir(), ss.ctx.get_final_dir());
  ss.ss->files.swap(fs._files);
  ss.ss->group_id = _gid;
  ss.ss->file_path = ss.ctx.get_file_path();
  ss.ss->membership = std::move(meta.membership);
  ss.ss->log_id = meta.lid;
  ss.ss->smtype = meta.smtype;
  // TODO(jyc): checksum & size & write header to
  co_await with_file(
      open_file_dma(file_path, wo | dsync), [&header_buf](file f) {
        return f.dma_write(0, header_buf.get(), header_buf.size())
            .then([&](size_t size) {
              if (size < header_buf.size()) {
                return make_exception_future<>(util::short_write_error());
              }
              return f.flush();
            });
      });
  co_return std::move(ss);
}

future<> snapshotter::load(
    std::function<future<>(input_stream<char> &, const snapshot_files &)>
        loader,
    snapshot_ptr ss) {
  auto file_path = get_file_path(ss->log_id.index);
  co_await open_file_dma(file_path, open_flags::ro).then([&](file f) {
    return do_with(
        make_file_input_stream(std::move(f)), [&](input_stream<char> &in) {
          return in.read_exactly(4096)
              .then([](temporary_buffer<char> buf) {
                // FIXME: parse header
                return make_ready_future<>();
              })
              .then([&] { return loader(in, ss->files); })
              .then([&] { return in.close(); });
        });
  });
}

future<snapshot_ptr> snapshotter::get_snapshot() {
  return _logdb.query_snapshot(_gid);
}

future<> snapshotter::compact(uint64_t index) {
  return _logdb.query_snapshot(_gid).then([this, index](snapshot_ptr ss) {
    if (ss->log_id.index <= index) {
      return make_exception_future<>(util::panic("invalid compaction"));
    }
    l.debug("{}: snapshot {}, compact to {}", _gid, ss->log_id.index, index);
    return remove(index);
  });
}

future<> snapshotter::commit(snapshot_ptr ss, const snapshot_request &req) {
  auto ctx = get_snapshot_context(ss->log_id.index, req.exported(), req.path);
  co_await ctx.save_snapshot_metadata(ss);
  co_await ctx.finalize_snapshot(ss);
  if (!req.exported()) {
    update up{.gid = _gid, .snapshot = ss};
    co_await _logdb.save({&up, 1});
  }
  co_await ctx.remove_flag_file();
}

future<> snapshotter::remove(uint64_t index) {
  auto ctx = get_snapshot_context(index);
  co_await ctx.remove_final_dir();
}

future<> snapshotter::remove_flag(uint64_t index) {
  auto ctx = get_snapshot_context(index);
  co_await ctx.remove_flag_file();
}

server::snapshot_context snapshotter::get_snapshot_context(uint64_t index) {
  using enum server::snapshot_context::mode;
  return server::snapshot_context(_dir, index, _gid.node, snapshotting);
}

server::snapshot_context snapshotter::get_snapshot_context(
    uint64_t index, bool exported, std::string_view path) {
  if (exported) {
    if (path.empty()) {
      throw util::panic("empty path when trying to export snapshot");
    }
  }
  using enum server::snapshot_context::mode;
  return server::snapshot_context(path, index, _gid.node, snapshotting);
}

future<> snapshotter::process_orphans() {
  // TODO(jyc): process orphans
  return make_ready_future<>();
}

future<bool> snapshotter::is_snapshot(std::string_view dir) {
  if (!std::regex_match(
          dir.begin(), dir.end(), server::snapshot_context::DIR_RE)) {
    return make_ready_future<bool>(false);
  }
  return util::exist_file(
             std::filesystem::path(_dir).append(dir).string(),
             server::snapshot_context::FLAG_NAME)
      .then([](bool has) { return !has; });
}

future<bool> snapshotter::is_zombie(std::string_view dir) {
  bool match =
      std::regex_match(
          dir.begin(), dir.end(), server::snapshot_context::GEN_DIR_RE) ||
      std::regex_match(
          dir.begin(), dir.end(), server::snapshot_context::RCV_DIR_RE);
  return make_ready_future<bool>(match);
}

future<bool> snapshotter::is_orphan(std::string_view dir) {
  if (!std::regex_match(
          dir.begin(), dir.end(), server::snapshot_context::DIR_RE)) {
    return make_ready_future<bool>(false);
  }
  return util::exist_file(
      std::filesystem::path(_dir).append(dir).string(),
      server::snapshot_context::FLAG_NAME);
}

std::string snapshotter::get_file_path(uint64_t index) {
  return get_snapshot_context(index).get_file_path();
}

}  // namespace rafter::rsm
