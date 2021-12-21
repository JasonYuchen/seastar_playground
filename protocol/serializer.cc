//
// Created by jason on 2021/11/6.
//

#include "serializer.hh"

#include <seastar/core/coroutine.hh>

#include "util/error.hh"

namespace rafter::protocol {

using namespace seastar;
using namespace std;

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
      throw util::short_write_error();
    }
    std::copy_n(data, size, _cur);
    _cur += size;
  }

  template<util::endian::detail::numerical T>
  T read() {
    if (_end - _cur < sizeof(T)) [[unlikely]] {
      throw util::short_read_error();
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
      throw util::short_read_error();
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

}  // namespace rafter::protocol
