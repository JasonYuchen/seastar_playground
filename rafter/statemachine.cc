//
// Created by jason on 2022/9/24.
//

#include "statemachine.hh"

#include "rafter/logger.hh"

namespace rafter {

kv_statemachine::kv_statemachine(protocol::group_id gid) : _gid(gid) {
  l.trace("{} kv_statemachine::kv_statemachine", _gid);
}

future<statemachine::result> kv_statemachine::open() {
  // TODO(jyc): protocol
  l.trace("{} kv_statemachine::open", _gid);
  return make_ready_future<result>();
}

future<statemachine::result> kv_statemachine::update(
    uint64_t index, std::string_view cmd) {
  l.trace("{} kv_statemachine::update: index:{}, cmd:{}", _gid, index, cmd);
  auto pos = cmd.find('=');
  if (pos == std::string::npos) {
    return make_exception_future<result>(result{1, "INVALID CMD"});
  }
  _data[std::string(cmd.substr(0, pos))] = cmd.substr(pos + 1);
  return make_ready_future<result>(result{0, "OK"});
}
future<statemachine::result> kv_statemachine::lookup(std::string_view cmd) {
  l.trace("{} kv_statemachine::lookup: cmd:{}", _gid, cmd);
  if (auto it = _data.find(cmd); it != _data.end()) {
    return make_ready_future<result>(result{0, it->second});
  }
  return make_ready_future<result>(result{2, "NOT FOUND"});
}
future<> kv_statemachine::sync() {
  // TODO(jyc): protocol
  l.trace("{} kv_statemachine::sync", _gid);
  return make_ready_future<>();
}
future<std::any> kv_statemachine::prepare() {
  // TODO(jyc): protocol
  l.trace("{} kv_statemachine::prepare", _gid);
  return make_ready_future<std::any>();
}
future<statemachine::snapshot_status> kv_statemachine::save_snapshot(
    std::any ctx, output_stream<char>& writer, rsm::files& fs, bool& abort) {
  // TODO(jyc): protocol
  l.trace("{} kv_statemachine::save_snapshot", _gid);
  return make_ready_future<snapshot_status>(snapshot_status::done);
}
future<statemachine::snapshot_status> kv_statemachine::recover_from_snapshot(
    input_stream<char>& reader,
    const protocol::snapshot_files& fs,
    bool& abort) {
  // TODO(jyc): protocol
  l.trace("{} kv_statemachine::recover_from_snapshot", _gid);
  return make_ready_future<snapshot_status>(snapshot_status::done);
}
future<> kv_statemachine::close() {
  // TODO(jyc): protocol
  l.trace("{} kv_statemachine::close", _gid);
  return make_ready_future<>();
}
protocol::state_machine_type kv_statemachine::type() const {
  return protocol::state_machine_type::regular;
}

}  // namespace rafter
