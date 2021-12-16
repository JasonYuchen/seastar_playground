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

using namespace seastar;
using namespace std;
using util::code;

future<unique_ptr<segment>> segment::open(
    uint64_t filename, string filepath, bool existing) {

  // TODO: tune seastar::file_open_options
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
    s->_tail.reserve(util::fragmented_temporary_buffer::default_fragment_size);
  }
  assert(s->_file);
  co_return s;
}

uint64_t segment::bytes() const noexcept {
  return _bytes;
}

future<uint64_t> segment::append(const protocol::update& update) {
  // TODO: tune the default segment size
  util::fragmented_temporary_buffer buffer(
      0, _file.memory_dma_alignment(), _file.disk_write_dma_alignment() << 1);
  auto out_stream = buffer.as_ostream();
  uint64_t written_bytes = _tail.size();
  out_stream.write(_tail);
  _tail.clear();
  written_bytes += protocol::serialize(update, out_stream);
  uint64_t last_chunk_fill = align_up(
      written_bytes, _file.disk_write_dma_alignment()) - written_bytes;
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
      co_return coroutine::make_exception(util::io_error(
          l, code::short_write,
          "{} segment::append: short write, expect:{}, actual:{}",
          update.gid.to_string(), it.size(), bytes));
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

future<protocol::update> segment::query(index::entry i) const {
  if (i.filename != _filename) [[unlikely]] {
    co_return coroutine::make_exception(util::logic_error(
        l, code::failed_precondition,
        "segment::query: filename mismatch, index has:{}, this segment is:{}",
        i.filename, _filename));
  }
  auto file_in_stream = make_file_input_stream(_file, i.offset);
  auto buffer = co_await util::fragmented_temporary_buffer::from_stream_exactly(
      file_in_stream, i.length);
  auto in_stream = buffer.as_istream();
  protocol::update up;
  auto bytes = protocol::deserialize(up, in_stream);
  if (bytes != i.length) {
    l.warn("segment::query: inconsistent data length, expect:{}, actual:{}",
           i.length, bytes);
  }
  co_return std::move(up);
}

future<size_t> segment::query(
    span<const index::entry> indexes,
    protocol::log_entry_vector& entries,
    size_t left_bytes) const {
  if (indexes.empty() || left_bytes == 0) {
    co_return left_bytes;
  }
  for (const auto& i : indexes) {
    if (i.filename != _filename) [[unlikely]] {
      co_return coroutine::make_exception(util::logic_error(
          l, code::failed_precondition,
          "segment::query: filename mismatch, expect:{}, actual:{}",
          i.filename, _filename));
    }
  }
  uint64_t max_pos = indexes.back().offset + indexes.back().length;
  if (max_pos > _bytes) {
    co_return coroutine::make_exception(util::logic_error(
        l, code::out_of_range,
        "segment::query: file size out-of-range, expect:{}, actual:{}",
        max_pos, _bytes));
  }

  // TODO: tune file stream options
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
      co_return coroutine::make_exception(util::io_error(
          l, code::short_read,
          "segment::query: short read, expect:{}, actual:{}",
          i.length, fragments.back().size()));
    }
  }
  util::fragmented_temporary_buffer buffer{std::move(fragments), size};
  auto in_stream = buffer.as_istream();
  uint64_t expected_index = indexes.front().first_index;
  for (const auto& i : indexes) {
    protocol::update up;
    auto length = protocol::deserialize(up, in_stream);
    if (length != i.length) [[unlikely]] {
      co_return coroutine::make_exception(util::io_error(
          l, code::corruption,
          "{} segment::query: inconsistent update length found in segment:{}, "
          "expect:{}, actual:{}",
          up.gid.to_string(), _filepath, i.length, length));
    }
    for (auto& ent : up.entries_to_save) {
      if (ent->lid.index < expected_index) {
        continue;
      }
      if (ent->lid.index > i.last_index) {
        break;
      }
      if (ent->lid.index != expected_index) [[unlikely]] {
        co_return coroutine::make_exception(util::io_error(
            l, code::corruption,
            "{} segment::query: log hole found in segment:{}, "
            "expect:{}, actual:{}",
            up.gid.to_string(), _filepath, expected_index, ent->lid.index));
      }
      if (left_bytes < ent->bytes()) {
        co_return 0;
      }
      left_bytes -= ent->bytes();
      expected_index++;
      entries.emplace_back(std::move(ent));
    }
  }

  if (in_stream.bytes_left()) {
    l.warn("segment::query: unexpected trailing data in segment:{}, {} bytes",
           _filepath, in_stream.bytes_left());
  }
  co_return left_bytes;
}

future<> segment::sync() {
  co_await _file.flush();
}

future<> segment::close() {
  co_await _file.close();
}

future<> segment::remove() {
  co_await remove_file(_filepath);
}

future<> segment::list_update(
    function<future<>(const protocol::update& up, index::entry e)> next) const {
  l.info("reconstructing index for segment:{}", _filepath);
  if (_bytes == 0) {
    co_return;
  }
  // TODO: tune file stream options
  uint64_t offset = 0;
  auto in_stream = seastar::make_file_input_stream(_file, offset);
  while (true) {
    try {
      protocol::update up;
      auto length = co_await protocol::deserialize_meta(up, in_stream);
      if (length == 0) {
        break;
      }
      index::entry e {
          .filename = _filename,
          .offset = offset,
          .length = length,
      };
      co_await next(up, e);
      offset += length;
    } catch (util::io_error& e) {
      l.warn("corrupted data in segment:{}, offset:{}, error:{}",
             _filepath, offset, e.what());
      break;
    }
  }
}

segment::operator bool() const {
  return _file.operator bool();
}

string segment::form_name(uint64_t filename) {
  return fmt::format("{:05d}_{:020d}.{}", this_shard_id(), filename, SUFFIX);
}

string segment::form_path(string_view dir, uint64_t filename) {
  return fmt::format(
      "{}/{:05d}_{:020d}.{}", dir, this_shard_id(), filename, SUFFIX);
}

pair<unsigned, uint64_t> segment::parse_name(string_view name) {
  auto sep_pos = name.find('_');
  auto suffix_pos = name.find('.');
  if (sep_pos == string::npos || suffix_pos == string::npos) {
    return {0, INVALID_FILENAME};
  }
  name.remove_suffix(name.size() - suffix_pos);
  unsigned shard_id = 0;
  uint64_t filename = 0;
  auto r = from_chars(name.data(), name.data() + sep_pos, shard_id);
  if (r.ec != errc()) {
    return {0, INVALID_FILENAME};
  }
  name.remove_prefix(sep_pos + 1);
  r = from_chars(name.data(), name.data() + name.size(), filename);
  if (r.ec != errc()) {
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
