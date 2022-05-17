//
// Created by jason on 2022/4/28.
//

#pragma once

#include <seastar/core/reactor.hh>

#include "rsm/files.hh"
#include "server/snapshot_context.hh"
#include "storage/logdb.hh"
#include "util/seastarx.hh"

namespace rafter::rsm {

class snapshotter {
 public:
  snapshotter(
      protocol::group_id gid,
      storage::logdb& logdb,
      protocol::snapshot_dir_func snapshot_dir);

  future<bool> shrunk(protocol::snapshot_ptr snapshot);

  // TODO(jyc): support stream

  // session_manager will be dumped inside the saver
  future<server::snapshot> save(
      std::function<future<bool>(std::any, output_stream<char>&, files&)> saver,
      protocol::snapshot_metadata meta);

  // session_manager will be loaded inside the loader
  future<> load(
      std::function<future<>(
          input_stream<char>&, const protocol::snapshot_files&)> loader,
      protocol::snapshot_ptr ss);

  future<protocol::snapshot_ptr> get_snapshot();

  // TODO(jyc): support on-disk statemachine
  //  future<> shrink(uint64_t index);

  future<> compact(uint64_t index);

  future<> commit(
      protocol::snapshot_ptr ss, const protocol::snapshot_request& req);

  future<> remove(uint64_t index);

  future<> remove_flag(uint64_t index);

  server::snapshot_context get_snapshot_context(uint64_t index);

  server::snapshot_context get_snapshot_context(
      uint64_t index, bool exported, std::string_view path);

 private:
  future<> process_orphans();

  future<bool> is_snapshot(std::string_view dir);
  future<bool> is_zombie(std::string_view dir);
  future<bool> is_orphan(std::string_view dir);

  // TODO(jyc): eliminate this
  std::string get_file_path(uint64_t index);

  protocol::group_id _gid;
  storage::logdb& _logdb;
  protocol::snapshot_dir_func _snapshot_dir;
  std::string _dir;
};

}  // namespace rafter::rsm
