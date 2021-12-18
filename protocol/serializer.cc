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
  uint64_t total = obj.bytes();
  o.write_le(total);
  o.write_le(obj.type);
  o.write_le(obj.cluster);
  o.write_le(obj.from);
  o.write_le(obj.to);
  o.write_le(obj.term);
  o.write_le(obj.log_term);
  o.write_le(obj.log_index);
  o.write_le(obj.commit);
  o.write_le(obj.witness);
  o.write_le(obj.reject);
  serialize(obj.hint, o);
  o.write_le(obj.entries.size());
  for (const auto& e : obj.entries) {
    serialize(*e, o);
  }
  o.write_le(obj.snapshot.operator bool());
  if (obj.snapshot) {
    serialize(*obj.snapshot, o);
  }
  return total;
}

uint64_t deserialize(
    struct message& obj, fragmented_temporary_buffer::istream& i) {
  auto total = i.read_le<uint64_t>();
  obj.type = i.read_le<message_type>();
  obj.cluster = i.read_le<uint64_t>();
  obj.from = i.read_le<uint64_t>();
  obj.to = i.read_le<uint64_t>();
  obj.term = i.read_le<uint64_t>();
  obj.log_term = i.read_le<uint64_t>();
  obj.log_index = i.read_le<uint64_t>();
  obj.commit = i.read_le<uint64_t>();
  obj.witness = i.read_le<bool>();
  obj.reject = i.read_le<bool>();
  deserialize(obj.hint, i);
  obj.entries.resize(i.read_le<uint64_t>());
  for (auto&& entry : obj.entries) {
    entry = make_lw_shared<log_entry>();
    deserialize(*entry, i);
  }
  if (i.read_le<bool>()) {
    obj.snapshot = make_lw_shared<snapshot>();
    deserialize(*obj.snapshot, i);
  }
  if (total != obj.bytes()) [[unlikely]] {
    throw util::io_error(util::code::corruption, "deserialize message failed");
  }
  return total;
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

}  // namespace rafter::protocol
