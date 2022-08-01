//
// Created by jason on 2022/4/28.
//

#include "files.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

#include "util/error.hh"

namespace rafter::rsm {

void files::add_file(uint64_t file_id, std::string path, std::string meta) {
  if (_ids.contains(file_id)) {
    throw util::invalid_argument("file_id", "duplicated");
  }
  _ids.insert(file_id);
  auto& f = _files.emplace_back(make_lw_shared<protocol::snapshot_file>());
  f->file_id = file_id;
  f->file_path = std::move(path);
  f->metadata = std::move(meta);
}

future<> files::prepare_files(
    std::string_view tmp_dir, std::string_view final_dir) {
  for (const auto& f : _files) {
    auto fn = f->filename();
    auto fp = std::filesystem::path(tmp_dir).append(fn).string();
    co_await link_file(f->file_path, fp);
    auto fi = co_await file_stat(fp);
    if (fi.type == directory_entry_type::directory) {
      // TODO(jyc): shall we support a directory ? and shall we move the check
      //  to the files::add_file
      co_await coroutine::return_exception(
          util::invalid_argument("file", "rsm file is a directory"));
    }
    if (fi.size == 0) {
      co_await coroutine::return_exception(
          util::invalid_argument("file", "empty rsm file"));
    }
    f->file_path = std::filesystem::path(final_dir).append(fn).string();
    f->file_size = fi.size;
  }
}

}  // namespace rafter::rsm
