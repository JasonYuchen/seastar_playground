//
// Created by jason on 2021/10/27.
//

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include "protocol/raft.hh"
#include "util/serializer.hh"
#include "util/types.hh"

namespace rafter::util {

using namespace protocol;

template<> struct serializer<raft_role>
    : public detail::numeric_serializer<raft_role> {};
template<> struct serializer<message_type>
    : public detail::numeric_serializer<message_type> {};
template<> struct serializer<entry_type>
    : public detail::numeric_serializer<entry_type> {};
template<> struct serializer<config_change_type>
    : public detail::numeric_serializer<config_change_type> {};
template<> struct serializer<state_machine_type>
    : public detail::numeric_serializer<state_machine_type> {};
template<> struct serializer<compression_type>
    : public detail::numeric_serializer<compression_type> {};
template<> struct serializer<checksum_type>
    : public detail::numeric_serializer<checksum_type> {};

template<> struct serializer<group_id> {
  template<typename Input>
  static group_id read(Input& i) {
    group_id v;
    v.cluster = deserialize(i, type<uint64_t>());
    v.node = deserialize(i, type<uint64_t>());
    return v;
  }
  template<typename Output>
  static void write(Output& o, const group_id& v) {
    serialize(o, v.cluster);
    serialize(o, v.node);
  }
  template<typename Input>
  static void skip(Input& i) {
    i.skip(16);
  }
};

template<> struct serializer<log_id> {
  template<typename Input>
  static log_id read(Input& i) {
    log_id v;
    v.term = deserialize(i, type<uint64_t>());
    v.index = deserialize(i, type<uint64_t>());
    return v;
  }
  template<typename Output>
  static void write(Output& o, const log_id& v) {
    serialize(o, v.term);
    serialize(o, v.index);
  }
  template<typename Input>
  static void skip(Input& i) {
    i.skip(16);
  }
};

template<> struct serializer<bootstrap> {
  template<typename Input>
  static bootstrap read(Input& i) {
    bootstrap v;
    v.addresses = deserialize(i, type<decltype(bootstrap::addresses)>());
    v.join = deserialize(i, type<bool>());
    v.smtype = deserialize(i, type<state_machine_type>());
    return v;
  }
  template<typename Output>
  static void write(Output& o, const bootstrap& v) {
    serialize(o, v.addresses);
    serialize(o, v.join);
    serialize(o, v.smtype);
  }
  template<typename Input>
  static void skip(Input& i) {
    skip(i, type<decltype(bootstrap::addresses)>());
    skip(i, type<bool>());
    skip(i, type<state_machine_type>());
  }
};

template<> struct serializer<hard_state> {
  template<typename Input>
  static hard_state read(Input& i) {
    hard_state v;
    v.term = deserialize(i, type<uint64_t>());
    v.vote = deserialize(i, type<uint64_t>());
    v.commit = deserialize(i, type<uint64_t>());
    return v;
  }
  template<typename Output>
  static void write(Output& o, const hard_state& v) {
    serialize(o, v.term);
    serialize(o, v.vote);
    serialize(o, v.commit);
  }
  template<typename Input>
  static void skip(Input& i) {
    i.skip(24);
  }
};

template<> struct serializer<update> {
  template<typename Input>
  static update read(Input& i) {
    deserialize(i, type<uint64_t>());
    update v;
    v.gid = deserialize(i, type<group_id>());
    v.state = deserialize(i, type<hard_state>());
    v.first_index = deserialize(i, type<uint64_t>());
    v.last_index = deserialize(i, type<uint64_t>());
    v.snapshot_index = deserialize(i, type<uint64_t>());
    v.entries_to_save = deserialize(i, type<log_entry_vector>());
    v.snapshot = deserialize(i, type<snapshot_ptr>());
    return v;
  }
  template<typename Output>
  static void write(Output& o, const update& v) {
    seastar::measuring_output_stream mo;
    serialize(mo, v.gid);
    serialize(mo, v.state);
    serialize(mo, v.first_index);
    serialize(mo, v.last_index);
    serialize(mo, v.snapshot_index);
    serialize(mo, v.entries_to_save);
    serialize(mo, v.snapshot);
    serialize(o, mo.size());
    serialize(o, v.gid);
    serialize(o, v.state);
    serialize(o, v.first_index);
    serialize(o, v.last_index);
    serialize(o, v.snapshot_index);
    serialize(o, v.entries_to_save);
    serialize(o, v.snapshot);
  }
  template<typename Input>
  static void skip(Input& i) {
    i.skip(deserialize(i, type<uint64_t>()));
  }
};

}

namespace rafter::protocol {

// TODO: design pipeline-style writer for checksum, compression, and so on

struct serializer {};

template<typename T, typename Output>
void write(serializer, Output& o, const T& v) {
  util::serialize(o, v);
}

template<typename T, typename Input>
T read(serializer, Input& i, util::type<T>) {
  return util::deserialize(i, util::type<T>());
}

}  // namespace rafter::protocol
