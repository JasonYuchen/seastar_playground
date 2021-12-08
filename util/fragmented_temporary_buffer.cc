//
// Created by jason on 2021/9/15.
//

#include "fragmented_temporary_buffer.hh"

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>

#include "util/error.hh"

using namespace seastar;
using namespace std;

namespace rafter::util {

fragmented_temporary_buffer::fragmented_temporary_buffer(
    size_t size, size_t alignment, size_t fragment_size) {
  fragment_size = align_up(fragment_size, alignment);
  size = align_up(size, fragment_size);
  auto count = size / fragment_size;
  for (size_t i = 0; i < count; ++i) {
    add_fragment(fragment_size);
  }
  _bytes = size;
  _alignment = alignment;
}

fragmented_temporary_buffer::fragmented_temporary_buffer(
    fragment_list fragments, size_t bytes)
  : _fragments(std::move(fragments)), _bytes(bytes) {}

future<fragmented_temporary_buffer>
fragmented_temporary_buffer::from_stream_exactly(
    input_stream<char>& in, size_t size) {
  fragment_list fragments;
  auto left = size;
  while (left) {
    auto tmp = co_await in.read_up_to(left);
    if (tmp.empty()) {
      co_return fragmented_temporary_buffer();
    }
    left -= tmp.size();
    fragments.emplace_back(std::move(tmp));
  }
  co_return fragmented_temporary_buffer(std::move(fragments), size);
}

future<fragmented_temporary_buffer>
fragmented_temporary_buffer::from_stream_up_to(
    input_stream<char>& in, size_t size) {
  fragment_list fragments;
  auto left = size;
  while (left) {
    auto tmp = co_await in.read_up_to(left);
    if (tmp.empty()) {
      break;
    }
    left -= tmp.size();
    fragments.emplace_back(std::move(tmp));
  }
  co_return fragmented_temporary_buffer(std::move(fragments), size - left);
}

void fragmented_temporary_buffer::remove_prefix(size_t n) noexcept {
  _bytes -= n;
  auto it = _fragments.begin();
  while (it->size() < n) {
    n -= it->size();
    ++it;
  }
  if (n) {
    it->trim_front(n);
  }
  _fragments.erase(_fragments.begin(), it);
}

void fragmented_temporary_buffer::remove_suffix(size_t n) noexcept {
  _bytes -= n;
  auto it = _fragments.rbegin();
  while (it->size() < n) {
    n -= it->size();
    ++it;
  }
  if (n) {
    it->trim(it->size() - n);
  }
  _fragments.erase(it.base(), _fragments.end());
}

fragmented_temporary_buffer::istream
fragmented_temporary_buffer::as_istream() const noexcept {
  return istream{_fragments.begin(), _bytes};
}

fragmented_temporary_buffer::ostream
fragmented_temporary_buffer::as_ostream() noexcept {
  // usually we will write immediately, make sure we have at least 1 fragment
  if (empty()) {
    add_fragment();
  }
  return ostream{*this, _fragments.begin(), _bytes};
}

fragmented_temporary_buffer::iterator
fragmented_temporary_buffer::begin() const noexcept {
  return _bytes ? iterator{_fragments.begin(), _bytes} : iterator{};
}

fragmented_temporary_buffer::iterator
fragmented_temporary_buffer::end() const noexcept {
  return {};
}

void fragmented_temporary_buffer::add_fragment(size_t fragment_size) {
  _fragments.emplace_back(
      temporary_buffer<char>::aligned(_alignment, fragment_size));
  _bytes += _fragments.back().size();
}

fragmented_temporary_buffer::iterator::iterator(
    fragment_list::const_iterator it, size_t left)
  : _it(it), _current(it->get(), std::min(it->size(), left)), _left(left) {}

fragmented_temporary_buffer::iterator&
fragmented_temporary_buffer::iterator::operator++() noexcept {
  _left -= _current.size();
  if (_left) {
    ++_it;
    _current = string_view(_it->get(), std::min(_left, _it->size()));
  }
  return *this;
}

fragmented_temporary_buffer::iterator
fragmented_temporary_buffer::iterator::operator++(int) noexcept {
  auto it = *this;
  operator++();
  return it;
}

fragmented_temporary_buffer::istream::istream(
    fragment_list::const_iterator it, size_t size) noexcept
  : _current(it)
  , _curr_pos(size ? _current->get() : nullptr)
  , _curr_end(size ? _current->get() + _current->size() : nullptr)
  , _bytes_left(size) {}

size_t fragmented_temporary_buffer::istream::bytes_left() const noexcept {
  return _bytes_left ? _bytes_left - (_curr_pos - _current->get()) : 0;
}

void fragmented_temporary_buffer::istream::skip(size_t n) noexcept {
  if (_curr_end - _curr_pos < n) [[unlikely]] {
    auto left = std::min(n, bytes_left());
    while (left) {
      auto len = std::min(left, static_cast<size_t>(_curr_end - _curr_pos));
      left -= len;
      if (left) {
        next_fragment();
      } else {
        _curr_pos += len;
      }
    }
  }
  _curr_pos += n;
}

fragmented_temporary_buffer::view
fragmented_temporary_buffer::istream::read(size_t n) {
  if (_curr_end - _curr_pos >= n) [[likely]] {
    auto v = view(_current, _curr_pos - _current->get(), n);
    _curr_pos += n;
    return v;
  }
  check_range(n);
  auto v = view(_current, _curr_pos - _current->get(), n);
  n -= _curr_end - _curr_pos;
  next_fragment();
  while (n > _current->size()) {
    n -= _current->size();
    next_fragment();
  }
  _curr_pos += n;
  return v;
}

string fragmented_temporary_buffer::istream::read_string(size_t n) {
  string s;
  if (_curr_end - _curr_pos >= n) [[likely]] {
    s.append(_curr_pos, n);
    _curr_pos += n;
    return s;
  }
  check_range(n);
  s.reserve(n);
  s.append(_curr_pos, _curr_end - _curr_pos);
  n -= _curr_end - _curr_pos;
  next_fragment();
  while (n > _current->size()) {
    s.append(_current->get(), _current->size());
    n -= _current->size();
    next_fragment();
  }
  s.append(_curr_pos, n);
  _curr_pos += n;
  return s;
}

void fragmented_temporary_buffer::istream::next_fragment() {
  _bytes_left -= _current->size();
  if (_bytes_left) {
    _current++;
    _curr_pos = _current->get();
    _curr_end = _current->get() + _current->size();
  } else {
    _curr_pos = nullptr;
    _curr_end = nullptr;
  }
}

void fragmented_temporary_buffer::istream::check_range(size_t size) {
  if (bytes_left() < size) [[unlikely]] {
    throw util::io_error(
        util::code::short_read,
        "fragmented_temporary_buffer::istream read error, want={}, left={}",
        size, _bytes_left);
  }
}

fragmented_temporary_buffer::ostream::ostream(
    fragmented_temporary_buffer& buffer,
    fragment_list::iterator it,
    size_t size) noexcept
  : _buffer(buffer)
  , _current(it)
  , _curr_pos(size ? _current->get_write() : nullptr)
  , _curr_end(size ? _current->get_write() + _current->size() : nullptr)
  , _bytes_left(size) {}

void fragmented_temporary_buffer::ostream::write(
    const char* data, size_t size) {
  if (_curr_end - _curr_pos < size) [[unlikely]] {
    size_t left = size;
    while (left) {
      auto len = std::min(left, static_cast<size_t>(_curr_end - _curr_pos));
      std::copy_n(data + size - left, len, _curr_pos);
      left -= len;
      if (left) {
        next_fragment();
      } else {
        _curr_pos += len;
      }
    }
    return;
  }
  std::copy_n(data, size, _curr_pos);
  _curr_pos += size;
}

void fragmented_temporary_buffer::ostream::fill(char c, size_t size) {
  if (_curr_end - _curr_pos < size) [[unlikely]] {
    size_t left = size;
    while (left) {
      auto len = std::min(left, static_cast<size_t>(_curr_end - _curr_pos));
      std::fill_n(_curr_pos, len, c);
      left -= len;
      if (left) {
        next_fragment();
      } else {
        _curr_pos += len;
      }
    }
    return;
  }
  std::fill_n(_curr_pos, size, c);
  _curr_pos += size;
}

void fragmented_temporary_buffer::ostream::remove_suffix_to_fit() noexcept {
  auto left = _bytes_left ? _bytes_left - (_curr_pos - _current->get()) : 0;
  _buffer.remove_suffix(left);
}

void fragmented_temporary_buffer::ostream::next_fragment() {
  _bytes_left -= _current->size();
  if (!_bytes_left) {
    _buffer.add_fragment();
    _current++;
    _bytes_left += _current->size();
  } else {
    _current++;
  }
  _curr_pos = _current->get_write();
  _curr_end = _current->get_write() + _current->size();
}

fragmented_temporary_buffer::view::view(
    fragment_list::const_iterator it, size_t pos, size_t size)
  : _current(it)
  , _curr_pos(it->get() + pos)
  , _curr_size(std::min(it->size() - pos, size))
  , _total_size(size) {}

fragmented_temporary_buffer::view::view(std::string_view s) noexcept
  : _curr_pos(s.data())
  , _curr_size(s.size())
  , _total_size(s.size()) {}

fragmented_temporary_buffer::view::view(const std::string& s) noexcept
  : _curr_pos(s.data())
  , _curr_size(s.size())
  , _total_size(s.size()) {}

bool fragmented_temporary_buffer::view::operator==(
    const view& rhs) const noexcept {
  auto lhs_it = begin();
  auto rhs_it = rhs.begin();
  if (empty() || rhs.empty()) {
    return empty() && rhs.empty();
  }
  auto lhs_view = *lhs_it;
  auto rhs_view = *rhs_it;
  while (lhs_it != end() && rhs_it != rhs.end()) {
    if (lhs_view.empty()) {
      ++lhs_it;
      if (lhs_it != end()) {
        lhs_view = *lhs_it;
      }
    }
    if (rhs_view.empty()) {
      ++rhs_it;
      if (rhs_it != rhs.end()) {
        rhs_view = *rhs_it;
      }
    }

    auto len = std::min(lhs_view.size(), rhs_view.size());
    if (!std::equal(lhs_view.data(), lhs_view.data() + len, rhs_view.data())) {
      return false;
    }
    lhs_view.remove_prefix(len);
    rhs_view.remove_prefix(len);
  }
  return lhs_it == end() && rhs_it == rhs.end();
}

bool fragmented_temporary_buffer::view::operator!=(
    const view& rhs) const noexcept {
  return !(*this == rhs);
}

fragmented_temporary_buffer::view::iterator&
fragmented_temporary_buffer::view::iterator::operator++() noexcept {
  _left -= _current.size();
  if (_left) {
    ++_it;
    _current = string_view{
        reinterpret_cast<const char*>(_it->get()),
        std::min(_left, _it->size())};
  }
  return *this;
}

fragmented_temporary_buffer::view::iterator
fragmented_temporary_buffer::view::iterator::operator++(int) noexcept {
  auto it = *this;
  operator++();
  return it;
}

fragmented_temporary_buffer::view::iterator::iterator(
    fragment_list::const_iterator it,
    string_view current,
    size_t left) noexcept
  : _it(it)
  , _left(left)
  , _current(current) {}

fragmented_temporary_buffer::view::iterator
fragmented_temporary_buffer::view::begin() const noexcept {
  return {_current, {_curr_pos, _curr_size}, _total_size};
}

fragmented_temporary_buffer::view::iterator
fragmented_temporary_buffer::view::end() const noexcept {
  return {};
}

}  // namespace rafter::util