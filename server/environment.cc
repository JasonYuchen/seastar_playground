//
// Created by jason on 2022/5/22.
//

#include "environment.hh"

#include <filesystem>
#include <seastar/core/smp.hh>

namespace rafter::server {

using namespace protocol;

std::function<std::string(group_id)> environment::get_snapshot_dir_func(
    std::string root_dir) {
  return [root_dir = std::move(root_dir)](group_id gid) {
    return std::filesystem::path(root_dir)
        .append(fmt::format("snapshot/{:020d}_{:020d}", gid.cluster, gid.node))
        .string();
  };
}
std::function<unsigned(uint64_t)> environment::get_partition_func() {
  return [](uint64_t cluster_id) { return cluster_id % seastar::smp::count; };
}

}  // namespace rafter::server
