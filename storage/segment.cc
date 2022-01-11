//
// Created by jason on 2021/9/21.
//

#include "segment.hh"

#include <charconv>
#include <seastar/core/reactor.hh>

#include "protocol/serializer.hh"
#include "storage/logger.hh"
#include "util/error.hh"

namespace rafter::storage {

using namespace protocol;
using namespace seastar;
using namespace std;

future<unique_ptr<segment>> segment::open(
    uint64_t filename, string filepath, bool existing) {
  // TODO(jyc): tune seastar::file_open_options
  l.info("open segment:{}, existing:{}", filepath, existing);
  assert(filename > 0);
  auto s = make_unique<segment>();
  s->_filename = filename;
  s->_filepath = std::move(filepath);
  if (existing) {
    // an existing segment is read-only in a new start
    auto st = co_await file_stat(s->_filepath);
    s->_bytes = st.size;
    s->_file = co_await open_file_dma(s->_filepath, open_flags::ro);
  } else {
    s->_file = co_await open_file_dma(
        s->_filepath, open_flags::create | open_flags::rw);
    s->_tail.reserve(util::fragmented_temporary_buffer::DEFAULT_FRAGMENT_SIZE);
  }
  assert(s->_file);
  co_return s;
}

uint64_t segment::filename() const noexcept { return _filename; }

uint64_t segment::bytes() const noexcept { return _bytes; }

future<uint64_t> segment::append(const update& up) {
  // TODO(jyc): tune the default segment size
  util::fragmented_temporary_buffer buffer(
      0, _file.memory_dma_alignment(), _file.disk_write_dma_alignment() << 1);
  auto out_stream = buffer.as_ostream();
  auto written_bytes = _tail.size();
  out_stream.write(_tail);
  _tail.clear();
  auto serialized_size = sizer(up);
  write(serializer{}, out_stream, serialized_size);
  write(serializer{}, out_stream, up);
  written_bytes += 8 + serialized_size;
  uint64_t last_chunk_fill =
      align_up(written_bytes, _file.disk_write_dma_alignment()) - written_bytes;
  out_stream.fill(0, last_chunk_fill);
  uint64_t last_chunk_remove = align_down(
      buffer.bytes() - written_bytes, _file.disk_write_dma_alignment());
  buffer.remove_suffix(last_chunk_remove);
  assert(written_bytes + last_chunk_fill == buffer.bytes());

  for (auto it : buffer) {
    assert(_aligned_pos % _file.disk_write_dma_alignment() == 0);
    assert(it.size() % _file.disk_write_dma_alignment() == 0);
    auto bytes = co_await _file.dma_write(_aligned_pos, it.data(), it.size());
    if (bytes < it.size()) {
      l.error(
          "{} segment::append: short write, expect:{}, actual:{}",
          up.gid,
          it.size(),
          bytes);
      co_return coroutine::make_exception(util::short_write_error());
    }
    if (written_bytes < it.size()) {
      // the last chunk
      _tail.append(it.data(), written_bytes);
      written_bytes -= _tail.size();
      break;
    }
    _aligned_pos += bytes;
    written_bytes -= bytes;
  }
  assert(written_bytes == 0);
  _bytes = _aligned_pos + _tail.size();
  co_return _bytes;
}

future<update> segment::query(index::entry i) const {
  if (i.filename != _filename) [[unlikely]] {
    l.error(
        "segment::query: filename mismatch, expect:{}, actual:{}",
        i.filename,
        _filename);
    co_return coroutine::make_exception(util::failed_precondition_error());
  }
  auto file_in_stream = make_file_input_stream(_file, i.offset);
  auto buffer = co_await util::fragmented_temporary_buffer::from_stream_exactly(
      file_in_stream, i.length);
  co_await file_in_stream.close();
  auto in_stream = buffer.as_istream();
  auto size = in_stream.read_le<uint64_t>();
  assert(size + 8 == i.length);
  update up = read(serializer{}, in_stream, util::type<update>());
  if (in_stream.bytes_left() > 0) {
    l.warn(
        "segment::query: inconsistent data length, expect:{}, actual:{}",
        i.length,
        i.length + in_stream.bytes_left());
  }
  co_return std::move(up);
}

future<size_t> segment::query(
    span<const index::entry> indexes,
    log_entry_vector& entries,
    size_t left_bytes) const {
  if (indexes.empty() || left_bytes == 0) {
    co_return left_bytes;
  }
  for (const auto& i : indexes) {
    if (i.filename != _filename) [[unlikely]] {
      l.error(
          "segment::query: filename mismatch, expect:{}, actual:{}",
          i.filename,
          _filename);
      co_return coroutine::make_exception(util::failed_precondition_error());
    }
  }
  uint64_t max_pos = indexes.back().offset + indexes.back().length;
  if (max_pos > _bytes) {
    l.error(
        "segment::query: potential corruption data in segment:{}, file "
        "size out of range, expect:{}, actual:{}",
        _filename,
        max_pos,
        _bytes);
    co_return coroutine::make_exception(util::out_of_range_error());
  }

  // TODO(jyc): tune file stream options
  auto file_in_stream = make_file_input_stream(_file, indexes.front().offset);
  util::fragmented_temporary_buffer::fragment_list fragments;
  size_t size = 0;
  size_t last_offset = indexes.front().offset;
  for (const auto& i : indexes) {
    co_await file_in_stream.skip(i.offset - last_offset);
    fragments.emplace_back(co_await file_in_stream.read_exactly(i.length));
    last_offset = i.offset + i.length;
    size += i.length;
    if (fragments.back().size() < i.length) {
      l.error(
          "segment::query: short read in segment:{}, expect:{}, actual:{}",
          _filename,
          i.length,
          fragments.back().size());
      co_return coroutine::make_exception(util::short_read_error());
    }
  }
  co_await file_in_stream.close();
  util::fragmented_temporary_buffer buffer{std::move(fragments), size};
  auto in_stream = buffer.as_istream();
  uint64_t bytes_left = in_stream.bytes_left();
  uint64_t expected_index = indexes.front().first_index;
  for (const auto& i : indexes) {
    auto sz = in_stream.read_le<uint64_t>();
    assert(sz + 8 == i.length);
    update up = read(serializer{}, in_stream, util::type<update>());
    auto read_bytes = bytes_left - in_stream.bytes_left();
    bytes_left = in_stream.bytes_left();
    if (read_bytes != i.length) [[unlikely]] {
      l.error(
          "segment::query: inconsistent update length in segment:{}, "
          "expect:{}, actual:{}",
          _filename,
          i.length,
          read_bytes);
      co_return coroutine::make_exception(util::corruption_error());
    }
    for (auto& ent : up.entries_to_save) {
      if (ent->lid.index < expected_index) {
        continue;
      }
      if (ent->lid.index > i.last_index) {
        break;
      }
      if (ent->lid.index != expected_index) [[unlikely]] {
        l.error(
            "{} segment::query: log hole found in segment:{}, missing:{}",
            up.gid,
            _filename,
            expected_index);
        co_return coroutine::make_exception(util::corruption_error());
      }
      if (left_bytes < ent->bytes()) {
        co_return 0;
      }
      left_bytes -= ent->bytes();
      expected_index++;
      entries.emplace_back(std::move(ent));
    }
  }

  if (in_stream.bytes_left() > 0) {
    l.warn(
        "segment::query: unexpected trailing data in segment:{}, {} bytes",
        _filepath,
        in_stream.bytes_left());
  }
  co_return left_bytes;
}

future<> segment::sync() { co_await _file.flush(); }

future<> segment::close() { co_await _file.close(); }

future<> segment::remove() { co_await remove_file(_filepath); }

future<> segment::list_update(
    function<future<>(const update& up, index::entry e)> next) const {
  l.info("reconstructing index for segment:{}", _filepath);
  if (_bytes == 0) {
    co_return;
  }
  // TODO(jyc): tune file stream options
  uint64_t offset = 0;
  auto stat = co_await file_stat(_filepath);
  auto in_stream = make_file_input_stream(_file, offset);
  auto deserializer =
      [&stat](auto& up, auto& istream, uint64_t offset) -> future<uint64_t> {
    util::fragmented_temporary_buffer::fragment_list fragments;
    fragments.emplace_back(co_await istream.read_exactly(update::meta_bytes()));
    if (fragments.back().size() < up.meta_bytes()) {
      throw util::short_read_error("incomplete update meta");
    }
    auto buffer = util::fragmented_temporary_buffer(
        std::move(fragments), update::meta_bytes());
    auto i = buffer.as_istream();
    auto size = i.read_le<uint64_t>();
    if (size < up.meta_bytes()) {
      // we have reached the trailing 0 due to alignment
      co_return 0;
    }
    up.gid = read(serializer{}, i, util::type<group_id>());
    up.state = read(serializer{}, i, util::type<hard_state>());
    up.first_index = read(serializer{}, i, util::type<uint64_t>());
    up.last_index = read(serializer{}, i, util::type<uint64_t>());
    up.snapshot_index = read(serializer{}, i, util::type<uint64_t>());
    if (i.bytes_left() > 0) [[unlikely]] {
      co_return coroutine::make_exception(util::corruption_error());
    }
    // we may reach the eof too early
    if (offset + size + 8 > stat.size) {
      // this index is valid though, the full update is partial
      throw util::short_read_error("incomplete update body");
    }
    // skip to next update data position
    co_await istream.skip(size + 8 - update::meta_bytes());
    co_return size + 8;
  };
  while (true) {
    try {
      update up;
      auto length = co_await deserializer(up, in_stream, offset);
      if (length == 0) {
        break;
      }
      index::entry e{
          .filename = _filename,
          .offset = offset,
          .length = length,
      };
      co_await next(up, e);
      offset += length;
    } catch (util::io_error& e) {
      l.warn(
          "corrupted data in segment:{}, offset:{}, error:{}",
          _filepath,
          offset,
          e.what());
      break;
    }
  }
}

segment::operator bool() const { return _file.operator bool(); }

string segment::form_name(uint64_t filename) {
  return fmt::format("{:05d}_{:020d}.{}", this_shard_id(), filename, SUFFIX);
}

std::string segment::form_path(std::string_view dir, uint64_t filename) {
  return form_path(dir, this_shard_id(), filename);
}

string segment::form_path(string_view dir, unsigned shard, uint64_t filename) {
  return fmt::format("{}/{:05d}_{:020d}.{}", dir, shard, filename, SUFFIX);
}

pair<unsigned, uint64_t> segment::parse_name(string_view name) {
  if (name.length() != 30) {
    // {:05d}_{:020d}.log
    return {0, INVALID_FILENAME};
  }
  auto sep_pos = name.find('_');
  auto suffix_pos = name.find('.');
  if (sep_pos == string::npos || suffix_pos == string::npos) {
    return {0, INVALID_FILENAME};
  }
  if (name.substr(suffix_pos + 1) != SUFFIX) {
    return {0, INVALID_FILENAME};
  }
  name.remove_suffix(name.size() - suffix_pos);
  unsigned shard_id = 0;
  uint64_t filename = 0;
  auto r = from_chars(name.data(), name.data() + sep_pos, shard_id);
  if (r.ec != errc() || r.ptr != name.data() + sep_pos) {
    return {0, INVALID_FILENAME};
  }
  name.remove_prefix(sep_pos + 1);
  r = from_chars(name.data(), name.data() + name.size(), filename);
  if (r.ec != errc() || r.ptr != name.data() + name.size()) {
    return {0, INVALID_FILENAME};
  }
  return {shard_id, filename};
}

string segment::debug_string() const {
  return fmt::format(
      "segment[filename={}, fullpath={}, valid_file={}, bytes={}, "
      "aligned_pos={}, tail_size={}]",
      _filename,
      _filepath,
      _file.operator bool(),
      _bytes,
      _aligned_pos,
      _tail.size());
}

}  // namespace rafter::storage
