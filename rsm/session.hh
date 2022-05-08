//
// Created by jason on 2022/4/25.
//

#pragma once

#include <vector>

#include "protocol/client.hh"
#include "protocol/raft.hh"
#include "util/serializer.hh"
#include "util/types.hh"

namespace rafter::rsm {

class session {
 public:
  explicit session(uint64_t client_id);
  DEFAULT_MOVE_AND_ASSIGN(session);

  void response(uint64_t series_id, protocol::rsm_result result);

  std::optional<protocol::rsm_result> response(uint64_t series_id) const;

  bool has_response(uint64_t series_id) const;

  void clear_to(uint64_t series_id);

  bool has_responded(uint64_t series_id) const;

 private:
  friend struct util::serializer<session>;

  uint64_t _client_id;
  uint64_t _responded = protocol::session::NOOP_SERIES_ID;
  // use vector as the series id is monotonously increasing
  std::unordered_map<uint64_t, protocol::rsm_result> _history;
};

}  // namespace rafter::rsm

namespace rafter::util {

template <>
struct serializer<rsm::session> {
  template <typename Input>
  static rsm::session read(Input& i) {
    rsm::session v(0);
    v._client_id = deserialize(i, type<uint64_t>());
    v._responded = deserialize(i, type<uint64_t>());
    v._history = deserialize(i, type<decltype(rsm::session::_history)>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const rsm::session& v) {
    serialize(o, v._client_id);
    serialize(o, v._responded);
    serialize(o, v._history);
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<decltype(rsm::session::_history)>());
  }
};

}  // namespace rafter::util
