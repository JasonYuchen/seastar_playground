//
// Created by jason on 2022/4/28.
//

#pragma once

#include <seastar/core/future.hh>
#include <unordered_set>

#include "protocol/raft.hh"
#include "util/seastarx.hh"

namespace rafter::rsm {

class files {
 public:
  void add_file(uint64_t file_id, std::string path, std::string meta);

  size_t size() const { return _files.size(); }

 private:
  friend class snapshotter;
  future<> prepare_files(std::string_view tmp_dir, std::string_view final_dir);

  std::vector<protocol::snapshot_file_ptr> _files;
  std::unordered_set<uint64_t> _ids;
};

}  // namespace rafter::rsm
