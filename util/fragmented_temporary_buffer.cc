//
// Created by jason on 2021/9/15.
//

#include "fragmented_temporary_buffer.hh"

#include <seastar/core/align.hh>

using namespace seastar;

namespace rafter::util {

fragmented_temporary_buffer::fragmented_temporary_buffer(size_t size,
                                                         size_t alignment) {
  size = align_up(size, default_fragment_size);
  auto count = size / default_fragment_size;
  fragment_vector fragments;
  fragments.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    fragments.emplace_back(
        temporary_buffer<char>::aligned(alignment, default_fragment_size));
  }
  _fragments = std::move(fragments);
  _bytes = size;
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
  return {_fragments.begin(), _bytes};
}

fragmented_temporary_buffer::ostream
fragmented_temporary_buffer::as_ostream() noexcept {
  if (_fragments.size() != 1) {
    return ostream::fragmented(_fragments.begin(), _bytes);
  }
  return ostream::simple(_fragments.begin()->get_write(),
                         _fragments.begin()->size());
}

fragmented_temporary_buffer::iterator
fragmented_temporary_buffer::begin() const noexcept {
  return _bytes ? iterator{_fragments.begin(), _bytes} : iterator{};
}

fragmented_temporary_buffer::iterator
fragmented_temporary_buffer::end() const noexcept {
  return {};
}

fragmented_temporary_buffer::iterator::iterator(
    fragment_vector::const_iterator it, size_t left)
    : _it(it), _current(it->get(), std::min(it->size(), left)), _left(left) {}

fragmented_temporary_buffer::iterator&
fragmented_temporary_buffer::iterator::operator++() noexcept {
  _left -= _current.size();
  if (_left) {
    ++_it;
    _current = std::string_view(_it->get(), std::min(_left, _it->size()));
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
    fragment_vector::const_iterator it, size_t size) noexcept
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
  // TODO: check range
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

std::string fragmented_temporary_buffer::istream::read_string(size_t n) {
  std::string s;
  s.reserve(n);
  if (_curr_end - _curr_pos >= n) [[likely]] {
    s.append(_curr_pos, n);
    _curr_pos += n;
    return s;
  }
  // TODO: sheck range
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

fragmented_temporary_buffer::view::view(
    fragment_vector::const_iterator it, size_t pos, size_t size)
  : _current(it)
  , _curr_pos(it->get() + pos)
  , _curr_size(std::min(it->size() - pos, size))
  , _total_size(size) {}

fragmented_temporary_buffer::view::iterator&
fragmented_temporary_buffer::view::iterator::operator++() noexcept {
  _left -= _current.size();
  if (_left) {
    ++_it;
    _current = std::string_view{
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
    fragment_vector::const_iterator it,
    std::string_view current,
    size_t left) noexcept
  : _it(it)
  , _left(left)
  , _current(current) {}

fragmented_temporary_buffer::view::iterator
fragmented_temporary_buffer::view::begin() const noexcept {
  return {_current, {_curr_pos, _curr_size}, _curr_size};
}

fragmented_temporary_buffer::view::iterator
fragmented_temporary_buffer::view::end() const noexcept {
  return {};
}

}  // namespace rafter::util