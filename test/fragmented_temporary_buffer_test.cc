//
// Created by jason on 2021/9/12.
//

#include "test/base.hh"
#include "util/fragmented_temporary_buffer.hh"

using namespace rafter::util;

RAFTER_TEST_CASE(fragmented_temporary_buffer_1_fragment) {
  auto fragment_size = fragmented_temporary_buffer::default_fragment_size;
  auto buffer_size = fragment_size - 10;
  std::string data(buffer_size, 'c');
  fragmented_temporary_buffer buffer(buffer_size, 4096);
  buffer.as_ostream().write(data.data(), data.size());
  BOOST_CHECK_EQUAL(buffer.size(), buffer_size);
  int i = 0;
  for (auto it = buffer.begin(); it != buffer.end(); it++, i++) {}
  BOOST_CHECK_EQUAL(i, 1);
}
