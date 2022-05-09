//
// Created by jason on 2022/4/25.
//

#include "session_manager.hh"

#include "rafter/config.hh"

namespace rafter::rsm {

session_manager::session_manager()
  : _sessions(config::shard().lru_max_session_count) {}

bool session_manager::register_client(uint64_t client_id) {
  auto it = _sessions.find(client_id);
  if (it == _sessions.end()) {
    auto [node, inserted] = _sessions.insert(client_id, session{client_id});
    assert(inserted);
    return true;
  }
  // TODO(jyc): already exists
  return false;
}

bool session_manager::unregister_client(uint64_t client_id) {
  return _sessions.erase(client_id);
}

session* session_manager::registered_client(uint64_t client_id) {
  auto it = _sessions.find(client_id);
  if (it == _sessions.end()) {
    return nullptr;
  }
  return &it->second;
}

session& session_manager::must_registered_client(uint64_t client_id) {
  return _sessions[client_id];
}

}  // namespace rafter::rsm
