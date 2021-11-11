//
// Created by jason on 2021/11/6.
//

#include "serializer.hh"

#include <seastar/core/coroutine.hh>

namespace rafter::protocol {

using namespace seastar;
using namespace std;

using util::fragmented_temporary_buffer;

// T is unordered_map<uint64_t, integral> or unordered_map<uint64_t, string>
template<typename T>
void dumper(T&& m, fragmented_temporary_buffer::ostream& o) {
  o.write_le(m.size());
  for (auto&& [id, s] : m) {
    o.write_le(id);
    if constexpr (is_integral_v<remove_reference_t<decltype(s)>>) {
      o.write_le(s);
    } else {
      o.write_le(s.size());
      o.write(s);
    }
  }
}

// T is unordered_map<uint64_t, integral> or unordered_map<uint64_t, string>
template<typename T>
void loader(T&& m, fragmented_temporary_buffer::istream& i) {
  auto size = i.read_le<uint64_t>();
  m.clear();
  for (size_t j = 0; j < size; ++j) {
    auto id = i.read_le<uint64_t>();
    using value_t = typename remove_reference_t<decltype(m)>::mapped_type;
    if constexpr (is_integral_v<value_t>) {
      m[id] = i.read_le<value_t>();
    } else {
      m[id] = i.read_string(i.read_le<uint64_t>());
    }
  }
}

uint64_t serialize(
    const group_id& obj, fragmented_temporary_buffer::ostream& o) {
  o.write_le(obj.cluster);
  o.write_le(obj.node);
  return obj.bytes();
}

uint64_t deserialize(
    group_id& obj, fragmented_temporary_buffer::istream& i) {
  obj.cluster = i.read_le<uint64_t>();
  obj.node = i.read_le<uint64_t>();
  return obj.bytes();
}

uint64_t serialize(
    const log_id& obj, fragmented_temporary_buffer::ostream& o) {
  o.write_le(obj.term);
  o.write_le(obj.index);
  return obj.bytes();
}

uint64_t deserialize(
    log_id& obj, fragmented_temporary_buffer::istream& i) {
  obj.term = i.read_le<uint64_t>();
  obj.index = i.read_le<uint64_t>();
  return obj.bytes();
}

uint64_t serialize(
    const struct bootstrap& obj, fragmented_temporary_buffer::ostream& o) {
  // TODO
  return 0;
}

uint64_t deserialize(
    struct bootstrap& obj, fragmented_temporary_buffer::istream& i) {
  // TODO
  return 0;
}

uint64_t serialize(
    const struct membership& obj, fragmented_temporary_buffer::ostream& o) {
  uint64_t total = obj.bytes();
  o.write_le(total);
  o.write_le(obj.config_change_id);
  dumper(obj.addresses, o);
  dumper(obj.observers, o);
  dumper(obj.witnesses, o);
  dumper(obj.removed, o);
  return total;
}

uint64_t deserialize(
    struct membership& obj, fragmented_temporary_buffer::istream& i) {
  auto total = i.read_le<uint64_t>();
  obj.config_change_id = i.read_le<uint64_t>();
  loader(obj.addresses, i);
  loader(obj.observers, i);
  loader(obj.witnesses, i);
  loader(obj.removed, i);
  return total;
}

uint64_t serialize(
    const struct log_entry& obj, fragmented_temporary_buffer::ostream& o) {
  uint64_t total = obj.bytes();
  o.write_le(total);
  serialize(obj.id, o);
  o.write_le(obj.type);
  o.write_le(obj.key);
  o.write_le(obj.client_id);
  o.write_le(obj.series_id);
  o.write_le(obj.responded_to);
  o.write_le(obj.payload.size());
  o.write(obj.payload);
  return total;
}

uint64_t deserialize(
    struct log_entry& obj, fragmented_temporary_buffer::istream& i) {
  auto total = i.read_le<uint64_t>();
  deserialize(obj.id, i);
  obj.type = i.read_le<entry_type>();
  obj.key = i.read_le<uint64_t>();
  obj.client_id = i.read_le<uint64_t>();
  obj.series_id = i.read_le<uint64_t>();
  obj.responded_to = i.read_le<uint64_t>();
  obj.payload = i.read_string(i.read_le<size_t>());
  if (total != obj.bytes()) [[unlikely]] {
    // TODO: throw
  }
  return total;
}

uint64_t serialize(
    const struct hard_state& obj, fragmented_temporary_buffer::ostream& o) {
  o.write_le(obj.term);
  o.write_le(obj.vote);
  o.write_le(obj.commit);
  return obj.bytes();
}

uint64_t deserialize(
    struct hard_state& obj, fragmented_temporary_buffer::istream& i) {
  obj.term = i.read_le<uint64_t>();
  obj.vote = i.read_le<uint64_t>();
  obj.commit = i.read_le<uint64_t>();
  return obj.bytes();
}

uint64_t serialize(
    const struct snapshot_file& obj, fragmented_temporary_buffer::ostream& o) {
  uint64_t total = obj.bytes();
  o.write_le(total);
  o.write_le(obj.file_id);
  o.write_le(obj.file_size);
  o.write_le(obj.file_path.size());
  o.write(obj.file_path);
  o.write_le(obj.metadata.size());
  o.write(obj.metadata);
  return total;
}

uint64_t deserialize(
    struct snapshot_file& obj, fragmented_temporary_buffer::istream& i) {
  auto total = i.read_le<uint64_t>();
  obj.file_id = i.read_le<uint64_t>();
  obj.file_size = i.read_le<uint64_t>();
  obj.file_path = i.read_string(i.read_le<size_t>());
  obj.metadata = i.read_string(i.read_le<size_t>());
  return total;
}

uint64_t serialize(
    const struct snapshot& obj, fragmented_temporary_buffer::ostream& o) {
  uint64_t total = 0;
  // TODO
  return total;
}

uint64_t deserialize(
    struct snapshot& obj, fragmented_temporary_buffer::istream& i) {
  uint64_t total = 0;
  // TODO
  return total;
}

uint64_t serialize(
    const struct hint& obj,
    util::fragmented_temporary_buffer::ostream& o) {
  o.write_le(obj.low);
  o.write_le(obj.high);
  return obj.bytes();
}

uint64_t deserialize(
    struct hint& obj,
    util::fragmented_temporary_buffer::istream& i) {
  obj.low = i.read_le<uint64_t>();
  obj.high = i.read_le<uint64_t>();
  return obj.bytes();
}

uint64_t serialize(
    const struct message& obj, fragmented_temporary_buffer::ostream& o) {
  // TODO
  return 0;
}

uint64_t deserialize(
    struct message& obj, fragmented_temporary_buffer::istream& i) {
  // TODO
  return 0;
}

uint64_t serialize(
    const struct config_change& obj, fragmented_temporary_buffer::ostream& o) {
  // TODO
  return 0;
}

uint64_t deserialize(
    struct config_change& obj, fragmented_temporary_buffer::istream& i) {
  // TODO
  return 0;
}

uint64_t serialize(
    const struct update& obj, fragmented_temporary_buffer::ostream& o) {
  uint64_t total = obj.bytes();
  o.write_le(total);
  serialize(obj.group_id, o);
  serialize(obj.state, o);
  o.write_le(obj.first_index);
  o.write_le(obj.last_index);
  o.write_le(obj.entries_to_save.size());
  for (auto&& e : obj.entries_to_save) {
    serialize(*e, o);
  }
  o.write_le(obj.snapshot.operator bool());
  if (obj.snapshot) {
    serialize(*obj.snapshot, o);
  }
  return total;
}

uint64_t deserialize(
    struct update& obj, fragmented_temporary_buffer::istream& i) {
  auto total = i.read_le<uint64_t>();
  deserialize(obj.group_id, i);
  deserialize(obj.state, i);
  obj.first_index = i.read_le<uint64_t>();
  obj.last_index = i.read_le<uint64_t>();
  auto size = i.read_le<uint64_t>();
  obj.entries_to_save.clear();
  obj.entries_to_save.reserve(size);
  for (size_t j = 0; j < size; ++j) {
    deserialize(*obj.entries_to_save.emplace_back(), i);
  }
  if (i.read_le<bool>()) {
    obj.snapshot = make_lw_shared<snapshot>();
    deserialize(*obj.snapshot, i);
  }
  if (total != obj.bytes()) [[unlikely]] {
    // TODO: throw
  }
  return total;
}

future<uint64_t> deserialize_meta(struct update& obj, input_stream<char>& i) {
  fragmented_temporary_buffer::fragment_list fragments;
  fragments.emplace_back(co_await i.read_exactly(obj.meta_bytes()));
  auto buffer = fragmented_temporary_buffer(
      std::move(fragments), obj.meta_bytes());
  auto in_stream = buffer.as_istream();
  auto total = in_stream.read_le<uint64_t>();
  deserialize(obj.group_id, in_stream);
  deserialize(obj.state, in_stream);
  obj.first_index = in_stream.read_le<uint64_t>();
  obj.last_index = in_stream.read_le<uint64_t>();
  if (in_stream.bytes_left() > 0) [[unlikely]] {
    // TODO: inconsistent length
  }
  // skip to next update data position
  co_await i.skip(total - obj.meta_bytes());
  co_return total;
}


}  // namespace rafter::protocol
