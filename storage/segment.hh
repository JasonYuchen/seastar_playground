//
// Created by jason on 2021/9/10.
//

#pragma once

#include <seastar/core/queue.hh>

#include <filesystem>
#include <numeric>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>
#include <span>
#include <utility>
#include <vector>

#include "protocol/raft.hh"
#include "util/fragmented_temporary_buffer.hh"
#include "util/types.hh"

namespace rafter::storage {

class segment {
 public:
  DEFAULT_MOVE_AND_ASSIGN(segment);

  struct log_locator {
    log_id id;
    uint64_t offset = 0;
  };

  class index {
   public:
    DEFAULT_COPY_MOVE_AND_ASSIGN(index);
    bool contains(uint64_t index) const noexcept {
      return index >= _start_index && index < _start_index + _locators.size();
    }
    uint64_t term(uint64_t index) const noexcept {
      assert(contains(index));
      return _locators[index - _start_index].id.term;
    }
    uint64_t bytes() const noexcept {
      return _locators.size() * sizeof(log_locator);
    }
    void append(log_locator locator) {
      _locators.emplace_back(locator);
    }
    void append(std::span<log_locator> entries) {
      _locators.insert(_locators.end(), entries.begin(), entries.end());
    }
    seastar::future<> serialize(seastar::output_stream<char>& out) const {
      // TODO
      co_return;
    }
    seastar::future<> deserialize() const {
      // TODO
      co_return;
    }

   private:
    uint64_t _start_index = 0;  // == locators_.front().id.index;
    std::vector<log_locator> _locators;
  };

  static segment create(std::filesystem::path filepath) {
    // TODO: tune seastar::file_open_options

    // TODO: create new segment for upcoming logs
    //  1. dma open (create)
    //  2. allocate header data
    //  3. update file header and sync
    //  4. sync parent directory? in the segment_manager
    segment s;
    s._file = co_await seastar::open_file_dma(
        filepath.string(),
        seastar::open_flags::create | seastar::open_flags::rw);
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

  static segment parse(std::filesystem::path filepath) {
    // TODO: tune seastar::file_open_options

    // TODO: reconstruct from existing segment file (.log or .log_inprogress)
    //  1. dma open
    //  2. parse header (throw if corrupted)
    //  3. set _archived = true, _loaded = false
    //  4. if .log_inprogress then parse the whole file, truncate and archive it
    segment s;
    s._file = co_await seastar::open_file_dma(
        filepath.string(), seastar::open_flags::rw);
    co_await s._header.deserialize(s._file);
    s._archived = true;
    s._loaded = false;
    return s;
  }

  seastar::future<> update_hard_state(protocol::hard_state state) {
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

  seastar::future<uint64_t> append(std::span<protocol::log_entry_ptr> entries) {
    assert(!_archived);
    // TODO: append entries to the tail

    // 1. calculate total writen bytes for buffer allocation, and generate
    //    locators for index
    std::vector<log_locator> locators;
    locators.reserve(entries.size());
    auto size = std::accumulate(
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
      auto aligned_written = seastar::align_down(written, _file.disk_write_dma_alignment());
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

  seastar::future<> finalize() {
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

  seastar::future<> sync() {
    assert(!_archived);
    co_return co_await _file.flush();
  }

 private:
  segment() = default;
  struct header {
    static constexpr uint64_t HEADER_SIZE = 4096;
    static constexpr uint64_t TERM = 0;
    static constexpr uint64_t VOTE = 8;
    static constexpr uint64_t COMMIT = 16;
    static constexpr uint64_t START_INDEX = 24;
    static constexpr uint64_t END_INDEX = 32;
    static constexpr uint64_t CHECKSUM_TYPE = 36;
    static constexpr uint64_t CHECKSUM = 40;

    header() = default;
    DEFAULT_MOVE_AND_ASSIGN(header);

    header& allocate(uint64_t alignment) {
      data = seastar::temporary_buffer<char>::aligned(
          alignment, header::HEADER_SIZE);
      ::memset(data.get_write(), 0, data.size());
    }

    uint64_t term() const noexcept {
      return read_le<uint64_t>(data.get() + TERM);
    }

    header& set_term(uint64_t term) noexcept {
      write_le(term, data.get_write() + TERM);
      return *this;
    }

    uint64_t vote() const noexcept {
      return read_le<uint64_t>(data.get() + VOTE);
    }

    header& set_vote(uint64_t vote) noexcept {
      write_le(vote, data.get_write() + VOTE);
      return *this;
    }

    uint64_t commit() const noexcept {
      return read_le<uint64_t>(data.get() + COMMIT);
    }

    header& set_commit(uint64_t commit) const noexcept {
      write_le(vote, data.get_write() + COMMIT);
      return *this;
    }

    header& fill_checksum(uint32_t type) {
      write_le(type, data.get_write() + CHECKSUM_TYPE);
      uint32_t checksum = 0;  // TODO: calculate checksum
      write_le(checksum, data.get_write() + CHECKSUM);
      return *this;
    }

    seastar::future<size_t> serialize(seastar::file& file) const {
      co_return co_await file.dma_write(0, data.get(), data.size());
    }

    seastar::future<> deserialize(seastar::file& file) {
      // let exception e.g. EOF propagate
      data = co_await file.dma_read_exactly(0, HEADER_SIZE);
      if (data.size() != HEADER_SIZE) {
        // TODO: throw if short read
      }
      // TODO: do some validation
    }

    seastar::temporary_buffer<char> data;
  };

 private:
  header _header;
  // TODO: scheduling group
  seastar::file _file;
  index _index;
  uint64_t _bytes = 0;
  bool _archived = false;
  bool _loaded = false;
  uint64_t _pos = 0;
  std::string _tail;
};

class segment_manager {
 public:
  // TODO: storage configuration class
  segment_manager(
      group_id id, std::filesystem::path data_dir, size_t queue_cap)
      : _group_id(id), _path(std::move(data_dir)) {
    // TODO: setup WAL module
    //  1. validate data_dir
    //  2. parse existing segments
    //  3. create active segment
  }

  future<> append(protocol::hard_state state,
                  std::span<protocol::log_entry_ptr> entries) {
    auto bytes = co_await _active_segment->append(entries);
    co_await _active_segment->update_hard_state(state);
    if (bytes >= _rolling_size) {
      co_await _active_segment->finalize();
      // TODO: rolling
    }
  }

 private:
  const group_id _group_id;
  // segment dir, e.g. <data_dir>/<group_id>
  std::filesystem::path _path;
  const uint64_t _rolling_size = 1024 * 1024 * 1024;  // in B
  // start entry -> archived segment
  std::map<log_id, std::unique_ptr<segment>> _segments;
  std::unique_ptr<segment> _active_segment;
};

}  // namespace rafter::storage
