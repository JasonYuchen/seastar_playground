//
// Created by jason on 2021/11/6.
//

#include "serializer.hh"

#include <seastar/core/coroutine.hh>

#include "util/error.hh"

namespace rafter::protocol {

using namespace seastar;
using namespace std;

using util::fragmented_temporary_buffer;

class serializer_adapter {
 public:
  serializer_adapter(char* buf, size_t size)
      : _buf(buf), _end(_buf + size), _cur(buf) {}

  template<util::endian::detail::numerical T>
  void write(T data) {
    write(reinterpret_cast<const char*>(&data), sizeof(T));
  }

  template<util::endian::detail::numerical T>
  void write_le(T data) {
    write(htole(data));
  }

  template<util::endian::detail::numerical T>
  void write_be(T data) {
    write(htobe(data));
  }

  void write(const char* data, size_t size) {
    if (_end - _cur < size) [[unlikely]] {
      throw util::io_error(util::code::short_write);
    }
    std::copy_n(data, size, _cur);
    _cur += size;
  }

  template<util::endian::detail::numerical T>
  T read() {
    if (_end - _cur < sizeof(T)) [[unlikely]] {
      throw util::io_error(util::code::short_read);
    }
    T obj;
    std::copy_n(_cur, sizeof(T), reinterpret_cast<char*>(&obj));
    _cur += sizeof(T);
    return obj;
  }

  template<util::endian::detail::numerical T>
  T read_le() {
    return letoh(read<T>());
  }

  template<util::endian::detail::numerical T>
  T read_be() {
    return betoh(read<T>());
  }

  std::string read_string(size_t n) {
    if (_end - _cur < n) [[unlikely]] {
      throw util::io_error(util::code::short_read);
    }
    std::string obj{_cur, n};
    _cur += n;
    return obj;
  }

 private:
  char* _buf;
  char* _end;
  char* _cur = 0;
};

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
  uint64_t total = obj.bytes();
  o.write_le(total);
  dumper(obj.addresses, o);
  o.write_le(obj.join);
  o.write_le(obj.smtype);
  return total;
}

uint64_t deserialize(
    struct bootstrap& obj, fragmented_temporary_buffer::istream& i) {
  auto total = i.read_le<uint64_t>();
  loader(obj.addresses, i);
  obj.join = i.read_le<bool>();
  obj.smtype = i.read_le<state_machine_type>();
  return total;
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
  serialize(obj.lid, o);
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
  deserialize(obj.lid, i);
  obj.type = i.read_le<entry_type>();
  obj.key = i.read_le<uint64_t>();
  obj.client_id = i.read_le<uint64_t>();
  obj.series_id = i.read_le<uint64_t>();
  obj.responded_to = i.read_le<uint64_t>();
  obj.payload = i.read_string(i.read_le<size_t>());
  if (total != obj.bytes()) [[unlikely]] {
    throw util::io_error(
        util::code::corruption, "deserialize log_entry failed");
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
  uint64_t total = obj.bytes();
  o.write_le(total);
  serialize(obj.group_id, o);
  serialize(obj.log_id, o);
  o.write_le(obj.file_path.size());
  o.write(obj.file_path);
  o.write_le(obj.file_size);
  o.write_le(obj.membership.operator bool());
  if (obj.membership) {
    serialize(*obj.membership, o);
  }
  o.write_le(obj.files.size());
  for (const auto& file : obj.files) {
    serialize(*file, o);
  }
  o.write_le(obj.smtype);
  o.write_le(obj.imported);
  o.write_le(obj.witness);
  o.write_le(obj.dummy);
  o.write_le(obj.on_disk_index);
  return total;
}

uint64_t deserialize(
    struct snapshot& obj, fragmented_temporary_buffer::istream& i) {
  auto total = i.read_le<uint64_t>();
  deserialize(obj.group_id, i);
  deserialize(obj.log_id, i);
  obj.file_path = i.read_string(i.read_le<uint64_t>());
  obj.file_size = i.read_le<uint64_t>();
  if (i.read_le<bool>()) {
    obj.membership = make_lw_shared<membership>();
    deserialize(*obj.membership, i);
  }
  obj.files.resize(i.read_le<uint64_t>());
  for (auto&& file : obj.files) {
    file = make_lw_shared<snapshot_file>();
    deserialize(*file, i);
  }
  obj.smtype = i.read_le<state_machine_type>();
  obj.imported = i.read_le<bool>();
  obj.witness = i.read_le<bool>();
  obj.dummy = i.read_le<bool>();
  obj.on_disk_index = i.read_le<uint64_t>();
  if (total != obj.bytes()) [[unlikely]] {
    throw util::io_error(util::code::corruption, "deserialize snapshot failed");
  }
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
  uint64_t total = obj.bytes();
  o.write_le(total);
  o.write_le(obj.config_change_id);
  o.write_le(obj.type);
  o.write_le(obj.node);
  o.write_le(obj.address.size());
  o.write(obj.address);
  o.write_le(obj.initialize);
  return total;
}

uint64_t deserialize(
    struct config_change& obj, fragmented_temporary_buffer::istream& i) {
  auto total = i.read_le<uint64_t>();
  obj.config_change_id = i.read_le<uint64_t>();
  obj.type = i.read_le<config_change_type>();
  obj.node = i.read_le<uint64_t>();
  obj.address = i.read_string(i.read_le<uint64_t>());
  obj.initialize = i.read_le<bool>();
  return total;
}

uint64_t serialize(
    const struct update& obj, fragmented_temporary_buffer::ostream& o) {
  // TODO: introduce meta crc32
  uint64_t total = obj.bytes();
  o.write_le(total);
  serialize(obj.gid, o);
  serialize(obj.state, o);
  o.write_le(obj.first_index);
  o.write_le(obj.last_index);
  o.write_le(obj.snapshot_index);
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
  deserialize(obj.gid, i);
  deserialize(obj.state, i);
  obj.first_index = i.read_le<uint64_t>();
  obj.last_index = i.read_le<uint64_t>();
  obj.snapshot_index = i.read_le<uint64_t>();
  obj.entries_to_save.resize(i.read_le<uint64_t>());
  for (auto&& entry : obj.entries_to_save) {
    entry = make_lw_shared<log_entry>();
    deserialize(*entry, i);
  }
  if (i.read_le<bool>()) {
    obj.snapshot = make_lw_shared<snapshot>();
    deserialize(*obj.snapshot, i);
  }
  if (total != obj.bytes()) [[unlikely]] {
    throw util::io_error(util::code::corruption, "deserialize update failed");
  }
  return total;
}

future<uint64_t> deserialize_meta(struct update& obj, input_stream<char>& i) {
  // TODO: introduce meta crc32
  fragmented_temporary_buffer::fragment_list fragments;
  fragments.emplace_back(co_await i.read_exactly(obj.meta_bytes()));
  if (fragments.back().size() < obj.meta_bytes()) {
    co_return 0;
  }
  auto buffer = fragmented_temporary_buffer(
      std::move(fragments), obj.meta_bytes());
  auto in_stream = buffer.as_istream();
  auto total = in_stream.read_le<uint64_t>();
  if (total < obj.meta_bytes()) {
    // we have reached the trailing 0 due to alignment
    co_return 0;
  }
  deserialize(obj.gid, in_stream);
  deserialize(obj.state, in_stream);
  obj.first_index = in_stream.read_le<uint64_t>();
  obj.last_index = in_stream.read_le<uint64_t>();
  obj.snapshot_index = in_stream.read_le<uint64_t>();
  if (in_stream.bytes_left() > 0) [[unlikely]] {
    co_return coroutine::make_exception(util::io_error(
        util::code::corruption, "deserialize update meta failed"));
  }
  // skip to next update data position
  co_await i.skip(total - obj.meta_bytes());
  co_return total;
}

}  // namespace rafter::protocol
