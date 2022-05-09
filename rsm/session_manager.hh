//
// Created by jason on 2022/4/25.
//

#pragma once

#include "rsm/session.hh"
#include "util/lru.hh"
#include "util/types.hh"

namespace rafter::rsm {

class session_manager {
 public:
  session_manager();
  DEFAULT_MOVE_AND_ASSIGN(session_manager);

  bool register_client(uint64_t client_id);

  bool unregister_client(uint64_t client_id);

  session* registered_client(uint64_t client_id);

  session& must_registered_client(uint64_t client_id);

 private:
  friend struct util::serializer<rsm::session_manager>;

  util::lru<uint64_t, session> _sessions;
};

}  // namespace rafter::rsm

namespace rafter::util {

template <>
struct serializer<rsm::session_manager> {
  template <typename Input>
  static rsm::session_manager read(Input& i) {
    rsm::session_manager v;
    size_t count = deserialize(i, type<uint64_t>());
    while (count--) {
      auto item = deserialize(i, type<std::pair<uint64_t, rsm::session>>());
      auto [node, inserted] = v._sessions.insert(std::move(item));
      assert(inserted);
    }
    return v;
  }
  template <typename Output>
  static void write(Output& o, const rsm::session_manager& v) {
    serialize(o, v._sessions.size());
    v._sessions.iterate([&o](const auto& item) { serialize(o, item); });
  }
  template <typename Input>
  static void skip(Input& i) {
    size_t count = deserialize(i, type<uint64_t>());
    while (count--) {
      skip(i, type<std::pair<uint64_t, rsm::session>>());
    }
  }
};

}  // namespace rafter::util
