//
// Created by jason on 2022/4/28.
//

#pragma once

#include <regex>
#include <seastar/core/future.hh>
#include <string>

#include "protocol/raft.hh"
#include "util/seastarx.hh"

namespace rafter::server {

class snapshot_context {
 public:
  enum class mode : uint8_t {
    snapshotting,
    receiving,
  };
  static inline constexpr char META_NAME[] = "snapshot.metadata";
  static inline constexpr char FLAG_NAME[] = "snapshot.flag";
  static inline constexpr char FILE_SUFFIX[] = "snap";
  static inline constexpr char GEN_SUFFIX[] = "generating";
  static inline constexpr char RCV_SUFFIX[] = "receiving";
  static inline constexpr char SNK_SUFFIX[] = "shrunk";
  static inline const std::regex DIR_RE{"^snapshot-[0-9A-F]+$"};
  static inline const std::regex DIR_INDEX_RE{"^snapshot-([0-9A-F]+)$"};
  static inline const std::regex GEN_DIR_RE{
      "^snapshot-[0-9A-F]+-[0-9A-F]+\\.generating$"};
  static inline const std::regex RCV_DIR_RE{
      "^snapshot-[0-9A-F]+-[0-9A-F]+\\.receiving$"};
  static std::string dir_name(uint64_t index);
  static std::string file_name(uint64_t index, bool shrink = false);

  snapshot_context(
      std::string_view root_dir, uint64_t index, uint64_t from, mode m);

  const std::string& get_root_dir() const { return _root_dir; }
  const std::string& get_tmp_dir() const { return _tmp_dir; }
  const std::string& get_final_dir() const { return _final_dir; }
  const std::string& get_file_path() const { return _file_path; }
  std::string get_shrunk_file_path() const { return file_name(_index, true); }
  std::string get_tmp_file_path() const;

  future<> create_tmp_dir();
  future<> remove_tmp_dir();
  future<> remove_final_dir();
  future<> remove_flag_file();
  future<> create_flag_file(protocol::snapshot_ptr ss);
  future<bool> has_flag_file();
  future<> finalize_snapshot(protocol::snapshot_ptr ss);
  future<> save_snapshot_metadata(protocol::snapshot_ptr ss);

 private:
  uint64_t _index = protocol::log_id::INVALID_INDEX;
  std::string _root_dir;
  std::string _tmp_dir;
  std::string _final_dir;
  std::string _file_path;
};

struct snapshot {
  snapshot_context ctx;
  protocol::snapshot_ptr ss;
};

}  // namespace rafter::server
