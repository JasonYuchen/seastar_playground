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

fragmented_temporary_buffer::ostream
fragmented_temporary_buffer::as_ostream() noexcept {
  if (_fragments.size() != 1) {
    return ostream::fragmented(_fragments.begin(), _bytes);
  }
  return ostream::simple(_fragments.begin()->get_write(),
                         _fragments.begin()->size());
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

}  // namespace rafter::util