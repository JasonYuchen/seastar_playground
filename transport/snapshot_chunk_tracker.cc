//
// Created by jason on 2022/5/16.
//

#include "snapshot_chunk_tracker.hh"

#include <seastar/core/coroutine.hh>

#include "transport/logger.hh"
#include "util/error.hh"

namespace rafter::transport {

using namespace protocol;

bool snapshot_chunk_tracker::record(const snapshot_chunk& chunk) {
  if (chunk.id != _expected_next_id) {
    l.error(
        "unexpected snapshot chunk id, want:{}, actual:{}",
        _expected_next_id,
        chunk.id);
    return false;
  }
  if (chunk.id == 0) {
    l.info(
        "recorded first chunk with {} {} from {} ",
        chunk.group_id,
        chunk.log_id,
        chunk.from);
    _first = chunk;
  }
  if (_first.from != chunk.from) {
    l.error(
        "chunk from unexpected remote node, want:{}, actual:{}",
        _first.from,
        chunk.from);
    return false;
  }
  if (chunk.file_chunk_id == 0 && chunk.file_info) {
    _files.emplace_back(chunk.file_info);
  }
  _expected_next_id++;
  return true;
}

future<message> snapshot_chunk_tracker::finalize(
    server::snapshot_context& ctx) {
  if (_first.id != 0) {
    l.error("invalid chunk with id:{}", _first.id);
    co_await coroutine::return_exception(util::panic("invalid chunk"));
  }
  if (!have_all_chunks()) {
    l.error(
        "finalize called on incomplete chunks, expected:{}, only:{}",
        _first.count,
        _expected_next_id + 1);
    co_await coroutine::return_exception(util::panic("incomplete chunks"));
  }
  auto snap = make_lw_shared<snapshot>();
  snap->log_id = _first.log_id;
  snap->on_disk_index = _first.on_disk_index;
  snap->membership = _first.membership;
  auto filename = std::filesystem::path(_first.file_path).filename().string();
  snap->file_path = std::filesystem::path(ctx.get_final_dir()).append(filename);
  snap->file_size = _first.file_size;
  snap->witness = _first.witness;
  snap->files = std::move(_files);
  for (auto& file : snap->files) {
    auto path =
        std::filesystem::path(ctx.get_final_dir()).append(file->filename());
    file->file_path = path;
  }
  // TODO(jyc): handle snapshot out-of-date error
  co_await ctx.finalize_snapshot(snap);
  message msg{
      .type = message_type::install_snapshot,
      .cluster = _first.group_id.cluster,
      .from = _first.from,
      .to = _first.group_id.node,
      .snapshot = std::move(snap)};
  co_return std::move(msg);
}

}  // namespace rafter::transport
