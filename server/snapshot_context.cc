//
// Created by jason on 2022/4/28.
//

#include "snapshot_context.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/file.hh>

#include "protocol/serializer.hh"
#include "util/error.hh"
#include "util/file.hh"

using std::filesystem::path;

namespace {

using rafter::server::snapshot_context;

std::string get_tmp_dir_name(
    std::string_view root,
    std::string_view suffix,
    uint64_t index,
    uint64_t from) {
  return path(root).append(
      fmt::format("{}-{}.{}", snapshot_context::dir_name(index), from, suffix));
}

std::string get_final_dir_name(std::string_view root, uint64_t index) {
  return path(root).append(snapshot_context::dir_name(index));
}

}  // namespace

namespace rafter::server {

std::string snapshot_context::dir_name(uint64_t index) {
  return fmt::format("{:016X}", index);
}

std::string snapshot_context::file_name(uint64_t index, bool shrink) {
  return fmt::format(
      "{:016X}.{}",
      index,
      shrink ? snapshot_context::SNK_SUFFIX : snapshot_context::FILE_SUFFIX);
}

snapshot_context::snapshot_context(
    std::string_view root_dir, uint64_t index, uint64_t from, mode m)
  : _index(index)
  , _root_dir(root_dir)
  , _tmp_dir(get_tmp_dir_name(
        _root_dir,
        m == mode::snapshotting ? GEN_SUFFIX : RCV_SUFFIX,
        index,
        from))
  , _final_dir(get_final_dir_name(_root_dir, index))
  , _file_path(path(_final_dir).append(file_name(index))) {}

std::string snapshot_context::get_tmp_file_path() const {
  return path(_tmp_dir).append(file_name(_index));
}

future<> snapshot_context::create_tmp_dir() {
  return recursive_touch_directory(_tmp_dir);
}

future<> snapshot_context::remove_tmp_dir() {
  return recursive_remove_directory(_tmp_dir).then(
      [this] { return sync_directory(_root_dir); });
}

future<> snapshot_context::remove_final_dir() {
  return recursive_remove_directory(_final_dir).then([this] {
    return sync_directory(_root_dir);
  });
}

future<> snapshot_context::remove_flag_file() {
  return util::remove_file(_final_dir, FLAG_NAME);
}

future<> snapshot_context::create_flag_file(protocol::snapshot_ptr ss) {
  return util::create_file(_tmp_dir, FLAG_NAME, protocol::write_to_string(*ss));
}

future<bool> snapshot_context::has_flag_file() {
  return util::exist_file(_final_dir, FLAG_NAME);
}

future<> snapshot_context::finalize_snapshot(protocol::snapshot_ptr ss) {
  if (co_await file_exists(_final_dir)) {
    throw util::snapshot_out_of_date();
  }
  co_await create_flag_file(ss);
  co_await rename_file(_tmp_dir, _final_dir);
  co_await sync_directory(_root_dir);
}

future<> snapshot_context::save_snapshot_metadata(protocol::snapshot_ptr ss) {
  return util::create_file(_tmp_dir, META_NAME, protocol::write_to_string(*ss));
}

}  // namespace rafter::server
