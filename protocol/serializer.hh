//
// Created by jason on 2021/10/27.
//

#pragma once

#include <seastar/core/future.hh>

#include "protocol/raft.hh"
#include "util/endian.hh"

namespace rafter::protocol {

// TODO: design pipeline-style writer for checksum, compression, and so on

template<typename T>
class o_adapter {
 public:
  explicit o_adapter(T& out) : out_(out) {}
  template<typename U>
  void write(U data) {
    if constexpr (std::is_integral_v<U>) {
      data = util::htole(data);
      out_.write(reinterpret_cast<const char*>(&data), sizeof(U));
    } else {
      out_.write(data.data(), data.size());
    }
  }

  void write(const char* data, size_t size) {
    out_.write(data, size);
  }

 private:
  T& out_;
};

template<typename T>
seastar::future<uint64_t> serialize(const struct log_entry& obj, T& out) {
  o_adapter o(out);
  uint64_t total = 49 + obj.payload.size();
  o.write(total);
  o.write(obj.id.term);
  o.write(obj.id.index);
  o.write(obj.type);
  o.write(obj.key);
  o.write(obj.client_id);
  o.write(obj.series_id);
  o.write(obj.responded_to);
  o.write(obj.payload);
  co_return total;
}

template<typename T>
seastar::future<uint64_t> deserialize(struct log_entry& obj, T& in) {
  uint64_t total = in.read<uint64_t>();
  obj.id.term = in.read<uint64_t>();
  obj.id.index = in.read<uint64_t>();
  obj.id.type = in.read<uint8_t>();
  obj.key = in.read<uint64_t>();
  obj.client_id = in.read<uint64_t>();
  obj.series_id = in.read<uint64_t>();
  obj.responded_to = in.read<uint64_t>();
  obj.payload = in.read_string(total - 49);
  co_return total;
}

template<typename T>
seastar::future<uint64_t> serialize(const struct membership& obj, T& out) {
  o_adapter o(out);

  // TODO
}

template<typename T>
seastar::future<uint64_t> deserialize(struct membership& obj, T& in) {
  // TODO
}

template<typename T>
seastar::future<uint64_t> serialize(const struct snapshot_file& obj, T& out) {
  // TODO
}

template<typename T>
seastar::future<uint64_t> deserialize(struct snapshot_file& obj, T& in) {
  // TODO
}

template<typename T>
seastar::future<uint64_t> serialize(const struct snapshot& obj, T& out) {
  // TODO
}

template<typename T>
seastar::future<uint64_t> deserialize(struct snapshot& obj, T& in) {
  // TODO
}

template<typename T>
seastar::future<uint64_t> serialize(const struct update& obj, T& out) {
  // TODO
}

template<typename T>
seastar::future<uint64_t> deserialize(struct update& obj, T& in) {
  // TODO
}

}  // namespace rafter::protocol
