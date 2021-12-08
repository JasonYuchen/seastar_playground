//
// Created by jason on 2021/9/21.
//

#include "segment.hh"

#include <seastar/core/reactor.hh>

#include "protocol/serializer.hh"
#include "storage/logger.hh"
#include "util/error.hh"

using namespace seastar;
using namespace std;

namespace rafter::storage {

future<unique_ptr<segment>> segment::open(
    uint64_t filename, string filepath, bool existing) {

  // TODO: tune seastar::file_open_options
  l.debug("open segment:{}, existing:{}", filepath, existing);
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
  co_return s;
}

uint64_t segment::bytes() const noexcept {
  return _bytes;
}

future<uint64_t> segment::append(const protocol::update& update) {
  // TODO(jason): refine this routine with new fragmented_temporary_buffer

  util::fragmented_temporary_buffer buffer(0, _file.memory_dma_alignment());
  auto out_stream = buffer.as_ostream();
  uint64_t written_bytes = _tail.size();
  out_stream.write(_tail);
  _tail.clear();
  written_bytes += protocol::serialize(update, out_stream);
  uint64_t last_chunk_remove = align_down(
      buffer.bytes() - written_bytes, _file.disk_write_dma_alignment());
  buffer.remove_suffix(last_chunk_remove);

  for (auto it : buffer) {
    assert(_aligned_pos % _file.disk_write_dma_alignment() == 0);
    assert(it.size() % _file.disk_write_dma_alignment() == 0);
    auto bytes = co_await _file.dma_write(_aligned_pos, it.data(), it.size());
    if (bytes < it.size()) {
      co_return coroutine::make_exception(util::io_error(
          l, util::code::short_write,
          "{} segment::append: short write, expect:{}, actual:{}",
          update.group_id.to_string(), it.size(), bytes));
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

future<> segment::query(
    index::entry i,
    function<seastar::future<>(const protocol::update& up)> f) const {
  if (i.filename != _filename) [[unlikely]] {
    throw util::logic_error(
        l, util::code::failed_precondition,
        "segment::query: filename mismatch, expect:{}, actual:{}",
        i.filename, _filename);
  }
  auto file_in_stream = make_file_input_stream(_file, i.offset);
  auto buffer = co_await util::fragmented_temporary_buffer::from_stream_exactly(
      file_in_stream, i.length);
  auto in_stream = buffer.as_istream();
  protocol::update up;
  protocol::deserialize(up, in_stream);
  co_await f(up);
  co_return;
}

future<> segment::query(
    span<const index::entry> indexes,
    protocol::log_entry_vector& entries,
    size_t& left_bytes) const {
  if (indexes.empty()) {
    co_return;
  }
  for (const auto& i : indexes) {
    if (i.filename != _filename) [[unlikely]] {
      throw util::logic_error(
          l, util::code::failed_precondition,
          "segment::query: filename mismatch, expect:{}, actual:{}",
          i.filename, _filename);
    }
  }
  uint64_t max_pos = indexes.back().offset + indexes.back().length;
  if (max_pos > _bytes) {
    throw util::logic_error(
        l, util::code::out_of_range,
        "segment::query: file size out-of-range, expect:{}, actual:{}",
        max_pos, _bytes);
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
      throw util::io_error(
          l, util::code::short_read,
          "segment::query: short read, expect:{}, actual:{}",
          i.length, fragments.back().size());
    }
  }
  util::fragmented_temporary_buffer buffer{std::move(fragments), size};
  auto in_stream = buffer.as_istream();
  uint64_t expected_index = indexes.front().first_index;
  for (const auto& i : indexes) {
    protocol::update up;
    auto length = protocol::deserialize(up, in_stream);
    if (length != i.length) [[unlikely]] {
      throw util::io_error(
          l, util::code::corruption,
          "{} segment::query: inconsistent update length found in segment:{}, "
          "expect:{}, actual:{}",
          up.group_id.to_string(), _filepath, i.length, length);
    }
    for (auto& ent : up.entries_to_save) {
      if (ent->id.index < expected_index) {
        continue;
      }
      if (ent->id.index > i.last_index) {
        break;
      }
      if (ent->id.index != expected_index) [[unlikely]] {
        throw util::io_error(
            l, util::code::corruption,
            "segment::query: log hole found in segment:{}, "
            "expect:{}, actual:{}",
            _filepath, expected_index, ent->id.index);
      }
      if (left_bytes < ent->bytes()) {
        left_bytes = 0;
        co_return;
      }
      entries.emplace_back(std::move(ent));
      left_bytes -= ent->bytes();
      expected_index++;
    }
  }

  if (in_stream.bytes_left()) {
    l.warn("segment::query: unexpected trailing data in segment:{}, {} bytes",
           _filepath, in_stream.bytes_left());
  }
  co_return;
}

future<> segment::sync() {
  co_return co_await _file.flush();
}

future<> segment::close() {
  co_return co_await _file.close();
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
      // TODO: index::entry only need some metadata from update, avoid parsing
      //  the whole data
      protocol::update up;
      auto length = co_await protocol::deserialize_meta(up, in_stream);
      if (length == 0) {
        break;
      }
      index::entry e {
          .id = up.group_id,
          .filename = _filename,
          .offset = offset,
          .length = length,
      };
      co_await next(up, e);
      offset += length;
    } catch (std::exception& e) {
      // TODO: refine exception type
      l.warn("corrupted data found in segment:{}, offset:{}",
             _filepath, offset);
      break;
    }
  }
  co_return;
}

segment::operator bool() const {
  return _file.operator bool();
}

string segment::debug() const {
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
