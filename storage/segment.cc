//
// Created by jason on 2021/9/21.
//

#include "segment.hh"

#include "util/endian.hh"

using namespace seastar;
using namespace std;

namespace rafter::storage {

bool segment::index::contains(uint64_t index) const noexcept {
  return index >= _start_index && index < _start_index + _locators.size();
}

uint64_t segment::index::term(uint64_t index) const noexcept {
  assert(contains(index));
  return _locators[index - _start_index].id.term;
}

uint64_t segment::index::bytes() const noexcept {
  return _locators.size() * sizeof(log_locator);
}

void segment::index::append(segment::log_locator locator) {
  _locators.emplace_back(locator);
}

void segment::index::append(span<log_locator> entries) {
  _locators.insert(_locators.end(), entries.begin(), entries.end());
}

segment segment::create(filesystem::path filepath) {
  // TODO: tune seastar::file_open_options

  // TODO: create new segment for upcoming logs
  //  1. dma open (create)
  //  2. allocate header data
  //  3. update file header and sync
  //  4. sync parent directory? in the segment_manager
  segment s;
  s._file = co_await open_file_dma(
      filepath.string(),
      open_flags::create | open_flags::rw);
  auto header_size = co_await s._header
      .allocate(s._file.memory_dma_alignment())
      .serialize(s._file);
  if (header_size != header::HEADER_SIZE) {
    // TODO: throw if short write
  }
  s._bytes = header::HEADER_SIZE;
  s._archived = false;
  s._loaded = true;
  s._tail.reserve(s._file.memory_dma_alignment());
  return s;
}

segment segment::parse(filesystem::path filepath) {
  // TODO: tune seastar::file_open_options

  // TODO: reconstruct from existing segment file (.log or .log_inprogress)
  //  1. dma open
  //  2. parse header (throw if corrupted)
  //  3. set _archived = true, _loaded = false
  //  4. if .log_inprogress then parse the whole file, truncate it if necessary
  segment s;
  s._file = co_await open_file_dma(filepath.string(), open_flags::rw);
  co_await s._header.deserialize(s._file);
  s._archived = true;
  s._loaded = false;
  return s;
}

uint64_t segment::bytes() const noexcept {
  return _bytes;
}

future<> segment::update_hard_state(protocol::hard_state state) {
  assert(!_archived);
  auto header_size = co_await _header
      .set_term(state.term)
      .set_vote(state.vote)
      .set_commit(state.commit)
      .fill_checksum(0)
      .serialize(_file);
  if (header_size != header::HEADER_SIZE) {
    // TODO: throw if short write
  }
  co_return;
}

future<uint64_t> segment::append(span<protocol::log_entry_ptr> entries) {
  assert(!_archived);
  if (entries.empty()) {
    co_return 0;
  }
  // TODO: append/overwrite entries according to the index

  // 0. fast append to the tail or overwrite in middle
  if (entries.front()->id.index > _index.last_index() + 1) {
    // TODO: throw append error, index hole
  }

  if (entries.front()->id.index < _index.first_index()) {
    // TODO: throw append error, overwrite committed entries
  }

  if (entries.front()->id.index != _index.last_index() + 1) {
    // TODO: prepare for overwrite
  }

  // 1. calculate total writen bytes for buffer allocation, and generate
  //    locators for index
  vector<log_locator> locators;
  locators.reserve(entries.size());
  auto size = accumulate(
      entries.begin(),
      entries.end(),
      _tail.size(),
      [_pos, &locators](size_t size, const protocol::log_entry_ptr& ptr) {
        locators.emplace_back(log_locator{.id = ptr->id,
            .offset = _pos + size});
        return size + ptr->payload.size(); // + log_entry_header_size
      });

  // 2. allocate buffer, remove extra bytes, pre-append tailing bytes to make
  //    sure _pos is always aligned
  util::fragmented_temporary_buffer buffer(size,
                                           _file.memory_dma_alignment());
  buffer.remove_suffix(buffer.bytes() - size);
  auto& out = buffer.as_ostream();
  out.write(_tail.data(), _tail.size());
  _tail.clear();

  // 3. serialize all entries to the buffer
  for (const auto& entry : entries) {
    // entry->serialize(out);
  }

  // 4. write buffers to disk, maintain the _tail
  for (auto it : buffer) {
    assert(_pos % _file.disk_write_dma_alignment() == 0);
    auto written = co_await _file.dma_write(_pos, it->data(), it->size());
    if (written < it->size()) {
      // TODO: throw short_write?
    }
    auto aligned_written = align_down(written, _file.disk_write_dma_alignment());
    _pos += aligned_written;
    if (aligned_written < written) {
      // should only occur once in the last iterator
      _tail.append(it->data() + aligned_written, written - aligned_written);
    }
  }

  // 5. update index accordingly and return the size of current segment
  _index.append(locators);
  _bytes = _pos + _tail.size();
  co_return _bytes;
}

future<> segment::finalize() {
  assert(!_archived);
  // TODO: archive this segment, and no more change in future
  //  1. dump index to the tail
  //  2. sync segment
  //  3. rename .log_inprogress -> .log
  //  4. sync parent directory? in the segment_manager
  //  5. set _archived = false, _loaded = false, clear index

  // TODO: all indexes of archived segments are cleared immediately
  //  consider using LRU cache to hold recently active indexes
}

future<> segment::sync() {
  assert(!_archived);
  co_return co_await _file.flush();
}

segment::header& segment::header::allocate(uint64_t alignment) {
  data = seastar::temporary_buffer<char>::aligned(
      alignment, header::HEADER_SIZE);
  std::fill_n(data.get_write(), data.size(), 0);
}

uint64_t segment::header::term() const noexcept {
  return util::read_le<uint64_t>(data.get() + TERM);
}

segment::header& segment::header::set_term(uint64_t term) noexcept {
  util::write_le(term, data.get_write() + TERM);
  return *this;
}

uint64_t segment::header::vote() const noexcept {
  return util::read_le<uint64_t>(data.get() + VOTE);
}

segment::header& segment::header::set_vote(uint64_t vote) noexcept {
  util::write_le(vote, data.get_write() + VOTE);
  return *this;
}

uint64_t segment::header::commit() const noexcept {
  return util::read_le<uint64_t>(data.get() + COMMIT);
}

segment::header& segment::header::set_commit(uint64_t commit) const noexcept {
  util::write_le(vote, data.get_write() + COMMIT);
  return *this;
}

segment::header& segment::header::fill_checksum(uint32_t type) {
  util::write_le(type, data.get_write() + CHECKSUM_TYPE);
  uint32_t checksum = 0;  // TODO: calculate checksum
  util::write_le(checksum, data.get_write() + CHECKSUM);
  return *this;
}

segment::header& segment::header::set_index_offset(uint64_t index_offset) {
  util::write_le(index_offset, data.get_write() + INDEX_OFFSET);
  return *this;
}

seastar::future<size_t> segment::header::serialize(file& file) const {
  co_return co_await file.dma_write(0, data.get(), data.size());
}

seastar::future<> segment::header::deserialize(file& file) {
  // let exception e.g. EOF propagate
  data = co_await file.dma_read_exactly(0, HEADER_SIZE);
  if (data.size() != HEADER_SIZE) {
    // TODO: throw if short read
  }
  // TODO: do some validation
}

}  // namespace rafter::storage
