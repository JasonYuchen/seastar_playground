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

template <>
struct serializer<raft_role> : public detail::numeric_serializer<raft_role> {};
template <>
struct serializer<message_type>
  : public detail::numeric_serializer<message_type> {};
template <>
struct serializer<entry_type>
  : public detail::numeric_serializer<entry_type> {};
template <>
struct serializer<config_change_type>
  : public detail::numeric_serializer<config_change_type> {};
template <>
struct serializer<state_machine_type>
  : public detail::numeric_serializer<state_machine_type> {};
template <>
struct serializer<compression_type>
  : public detail::numeric_serializer<compression_type> {};
template <>
struct serializer<checksum_type>
  : public detail::numeric_serializer<checksum_type> {};

template <>
struct serializer<group_id> {
  template <typename Input>
  static group_id read(Input& i) {
    group_id v;
    v.cluster = deserialize(i, type<uint64_t>());
    v.node = deserialize(i, type<uint64_t>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const group_id& v) {
    serialize(o, v.cluster);
    serialize(o, v.node);
  }
  template <typename Input>
  static void skip(Input& i) {
    i.skip(16);
  }
};

template <>
struct serializer<log_id> {
  template <typename Input>
  static log_id read(Input& i) {
    log_id v;
    v.term = deserialize(i, type<uint64_t>());
    v.index = deserialize(i, type<uint64_t>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const log_id& v) {
    serialize(o, v.term);
    serialize(o, v.index);
  }
  template <typename Input>
  static void skip(Input& i) {
    i.skip(16);
  }
};

template <>
struct serializer<bootstrap> {
  template <typename Input>
  static bootstrap read(Input& i) {
    bootstrap v;
    v.addresses = deserialize(i, type<decltype(bootstrap::addresses)>());
    v.join = deserialize(i, type<bool>());
    v.smtype = deserialize(i, type<state_machine_type>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const bootstrap& v) {
    serialize(o, v.addresses);
    serialize(o, v.join);
    serialize(o, v.smtype);
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<decltype(bootstrap::addresses)>());
    skip(i, type<bool>());
    skip(i, type<state_machine_type>());
  }
};

template <>
struct serializer<membership> {
  template <typename Input>
  static membership read(Input& i) {
    membership v;
    v.config_change_id = deserialize(i, type<uint64_t>());
    v.addresses = deserialize(i, type<decltype(membership::addresses)>());
    v.observers = deserialize(i, type<decltype(membership::observers)>());
    v.witnesses = deserialize(i, type<decltype(membership::witnesses)>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const membership& v) {
    serialize(o, v.config_change_id);
    serialize(o, v.addresses);
    serialize(o, v.observers);
    serialize(o, v.witnesses);
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<uint64_t>());
    skip(i, type<decltype(membership::addresses)>());
    skip(i, type<decltype(membership::observers)>());
    skip(i, type<decltype(membership::witnesses)>());
  }
};

template <>
struct serializer<log_entry> {
  template <typename Input>
  static log_entry read(Input& i) {
    log_entry v;
    v.lid = deserialize(i, type<log_id>());
    v.type = deserialize(i, type<entry_type>());
    v.key = deserialize(i, type<uint64_t>());
    v.client_id = deserialize(i, type<uint64_t>());
    v.series_id = deserialize(i, type<uint64_t>());
    v.responded_to = deserialize(i, type<uint64_t>());
    v.payload = deserialize(i, type<std::string>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const log_entry& v) {
    serialize(o, v.lid);
    serialize(o, v.type);
    serialize(o, v.key);
    serialize(o, v.client_id);
    serialize(o, v.series_id);
    serialize(o, v.responded_to);
    serialize(o, v.payload);
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<log_id>());
    skip(i, type<entry_type>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<std::string>());
  }
};

template <>
struct serializer<hard_state> {
  template <typename Input>
  static hard_state read(Input& i) {
    hard_state v;
    v.term = deserialize(i, type<uint64_t>());
    v.vote = deserialize(i, type<uint64_t>());
    v.commit = deserialize(i, type<uint64_t>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const hard_state& v) {
    serialize(o, v.term);
    serialize(o, v.vote);
    serialize(o, v.commit);
  }
  template <typename Input>
  static void skip(Input& i) {
    i.skip(24);
  }
};

template <>
struct serializer<snapshot_file> {
  template <typename Input>
  static snapshot_file read(Input& i) {
    snapshot_file v;
    v.file_id = deserialize(i, type<uint64_t>());
    v.file_size = deserialize(i, type<uint64_t>());
    v.file_path = deserialize(i, type<std::string>());
    v.metadata = deserialize(i, type<std::string>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const snapshot_file& v) {
    serialize(o, v.file_id);
    serialize(o, v.file_size);
    serialize(o, v.file_path);
    serialize(o, v.metadata);
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<std::string>());
    skip(i, type<std::string>());
  }
};

template <>
struct serializer<snapshot> {
  template <typename Input>
  static snapshot read(Input& i) {
    snapshot v;
    v.group_id = deserialize(i, type<group_id>());
    v.log_id = deserialize(i, type<log_id>());
    v.file_path = deserialize(i, type<std::string>());
    v.file_size = deserialize(i, type<uint64_t>());
    if (deserialize(i, type<bool>())) {
      v.membership = deserialize(i, type<membership_ptr>());
    }
    v.files = deserialize(i, type<decltype(snapshot::files)>());
    v.smtype = deserialize(i, type<state_machine_type>());
    v.imported = deserialize(i, type<bool>());
    v.witness = deserialize(i, type<bool>());
    v.dummy = deserialize(i, type<bool>());
    v.on_disk_index = deserialize(i, type<uint64_t>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const snapshot& v) {
    serialize(o, v.group_id);
    serialize(o, v.log_id);
    serialize(o, v.file_path);
    serialize(o, v.file_size);
    serialize(o, v.membership.operator bool());
    if (v.membership) {
      serialize(o, v.membership);
    }
    serialize(o, v.files);
    serialize(o, v.smtype);
    serialize(o, v.imported);
    serialize(o, v.witness);
    serialize(o, v.dummy);
    serialize(o, v.on_disk_index);
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<group_id>());
    skip(i, type<log_id>());
    skip(i, type<std::string>());
    skip(i, type<uint64_t>());
    if (deserialize(i, type<bool>())) {
      skip(i, type<membership_ptr>());
    }
    skip(i, type<decltype(snapshot::files)>());
    skip(i, type<state_machine_type>());
    skip(i, type<bool>());
    skip(i, type<bool>());
    skip(i, type<bool>());
    skip(i, type<uint64_t>());
  }
};

template <>
struct serializer<hint> {
  template <typename Input>
  static hint read(Input& i) {
    hint v;
    v.low = deserialize(i, type<uint64_t>());
    v.high = deserialize(i, type<uint64_t>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const hint& v) {
    serialize(o, v.low);
    serialize(o, v.high);
  }
  template <typename Input>
  static void skip(Input& i) {
    i.skip(16);
  }
};

template <>
struct serializer<message> {
  template <typename Input>
  static message read(Input& i) {
    message v;
    v.type = deserialize(i, type<message_type>());
    v.cluster = deserialize(i, type<uint64_t>());
    v.from = deserialize(i, type<uint64_t>());
    v.to = deserialize(i, type<uint64_t>());
    v.term = deserialize(i, type<uint64_t>());
    v.lid = deserialize(i, type<log_id>());
    v.commit = deserialize(i, type<uint64_t>());
    v.witness = deserialize(i, type<bool>());
    v.reject = deserialize(i, type<bool>());
    v.hint = deserialize(i, type<hint>());
    v.entries = deserialize(i, type<log_entry_vector>());
    if (deserialize(i, type<bool>())) {
      v.snapshot = deserialize(i, type<snapshot_ptr>());
    }
    return v;
  }
  template <typename Output>
  static void write(Output& o, const message& v) {
    serialize(o, v.type);
    serialize(o, v.cluster);
    serialize(o, v.from);
    serialize(o, v.to);
    serialize(o, v.term);
    serialize(o, v.lid);
    serialize(o, v.commit);
    serialize(o, v.witness);
    serialize(o, v.reject);
    serialize(o, v.hint);
    serialize(o, v.entries);
    serialize(o, v.snapshot.operator bool());
    if (v.snapshot) {
      serialize(o, v.snapshot);
    }
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<message_type>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<log_id>());
    skip(i, type<uint64_t>());
    skip(i, type<bool>());
    skip(i, type<bool>());
    skip(i, type<hint>());
    skip(i, type<log_entry_vector>());
    if (deserialize(i, type<bool>())) {
      skip(i, type<snapshot_ptr>());
    }
  }
};

template <>
struct serializer<config_change> {
  template <typename Input>
  static config_change read(Input& i) {
    config_change v;
    v.config_change_id = deserialize(i, type<uint64_t>());
    v.type = deserialize(i, type<config_change_type>());
    v.node = deserialize(i, type<uint64_t>());
    v.address = deserialize(i, type<std::string>());
    v.initialize = deserialize(i, type<bool>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const config_change& v) {
    serialize(o, v.config_change_id);
    serialize(o, v.type);
    serialize(o, v.node);
    serialize(o, v.address);
    serialize(o, v.initialize);
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<uint64_t>());
    skip(i, type<config_change_type>());
    skip(i, type<uint64_t>());
    skip(i, type<std::string>());
    skip(i, type<bool>());
  }
};

template <>
struct serializer<snapshot_chunk> {
  template <typename Input>
  static snapshot_chunk read(Input& i) {
    snapshot_chunk v;
    v.deployment_id = deserialize(i, type<uint64_t>());
    v.group_id = deserialize(i, type<group_id>());
    v.log_id = deserialize(i, type<log_id>());
    v.from = deserialize(i, type<uint64_t>());
    v.id = deserialize(i, type<uint64_t>());
    v.size = deserialize(i, type<uint64_t>());
    v.count = deserialize(i, type<uint64_t>());
    v.data = deserialize(i, type<std::string>());
    if (deserialize(i, type<bool>())) {
      v.membership = deserialize(i, type<membership_ptr>());
    }
    v.file_path = deserialize(i, type<std::string>());
    v.file_size = deserialize(i, type<uint64_t>());
    v.file_chunk_id = deserialize(i, type<uint64_t>());
    v.file_chunk_count = deserialize(i, type<uint64_t>());
    if (deserialize(i, type<bool>())) {
      v.file_info = deserialize(i, type<snapshot_file_ptr>());
    }
    v.on_disk_index = deserialize(i, type<uint64_t>());
    v.witness = deserialize(i, type<bool>());
    return v;
  }
  template <typename Output>
  static void write(Output& o, const snapshot_chunk& v) {
    serialize(o, v.deployment_id);
    serialize(o, v.group_id);
    serialize(o, v.log_id);
    serialize(o, v.from);
    serialize(o, v.id);
    serialize(o, v.size);
    serialize(o, v.count);
    serialize(o, v.data);
    serialize(o, v.membership.operator bool());
    if (v.membership) {
      serialize(o, v.membership);
    }
    serialize(o, v.file_path);
    serialize(o, v.file_size);
    serialize(o, v.file_chunk_id);
    serialize(o, v.file_chunk_count);
    serialize(o, v.file_info.operator bool());
    if (v.file_info) {
      serialize(o, v.file_info);
    }
    serialize(o, v.on_disk_index);
    serialize(o, v.witness);
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<uint64_t>());
    skip(i, type<group_id>());
    skip(i, type<log_id>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<std::string>());
    if (deserialize(i, type<bool>())) {
      skip(i, type<membership_ptr>());
    }
    skip(i, type<std::string>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    if (deserialize(i, type<bool>())) {
      skip(i, type<snapshot_file_ptr>());
    }
    skip(i, type<uint64_t>());
    skip(i, type<bool>());
  }
};

template <>
struct serializer<update> {
  template <typename Input>
  static update read(Input& i) {
    update v;
    v.gid = deserialize(i, type<group_id>());
    v.state = deserialize(i, type<hard_state>());
    v.first_index = deserialize(i, type<uint64_t>());
    v.last_index = deserialize(i, type<uint64_t>());
    v.snapshot_index = deserialize(i, type<uint64_t>());
    v.entries_to_save = deserialize(i, type<log_entry_vector>());
    bool has_snapshot = deserialize(i, type<bool>());
    if (has_snapshot) {
      v.snapshot = deserialize(i, type<snapshot_ptr>());
    }
    return v;
  }
  template <typename Output>
  static void write(Output& o, const update& v) {
    serialize(o, v.gid);
    serialize(o, v.state);
    serialize(o, v.first_index);
    serialize(o, v.last_index);
    serialize(o, v.snapshot_index);
    serialize(o, v.entries_to_save);
    serialize(o, v.snapshot.operator bool());
    if (v.snapshot) {
      serialize(o, v.snapshot);
    }
  }
  template <typename Input>
  static void skip(Input& i) {
    skip(i, type<group_id>());
    skip(i, type<hard_state>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<uint64_t>());
    skip(i, type<log_entry_vector>());
    if (deserialize(i, type<bool>())) {
      skip(i, type<snapshot_ptr>());
    }
  }
};

}  // namespace rafter::util

namespace rafter::protocol {

// TODO(jyc): design pipeline-style writer for checksum, compression, and so on

// TODO(jyc): double check pointer<T> serialization
//  by default, a bool will be appended to indicate the pointer's existence,
//  but when faced with pointers in a container, the pointers cannot be null,
//  avoid this redundant bool by template specialization

struct serializer {};

template <typename T, typename Output>
void write(serializer, Output& o, const T& v) {
  util::serialize(o, v);
}

template <typename T, typename Input>
T read(serializer, Input& i, util::type<T>) {
  return util::deserialize(i, util::type<T>());
}

template <typename T>
size_t sizer(const T& obj) {
  seastar::measuring_output_stream mo;
  write(serializer{}, mo, obj);
  return mo.size();
}

}  // namespace rafter::protocol
