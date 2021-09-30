//
// Created by jason on 2021/9/12.
//

#include "util/fragmented_temporary_buffer.hh"

#include "test/base.hh"

using namespace rafter::util;

// TODO: add test cases
class fragmented_temporary_buffer_test : public rafter_test_base {};

RAFTER_TEST_F(fragmented_temporary_buffer_test, one_fragment) {
  l.info("running");
  EXPECT_TRUE(true);
  EXPECT_FALSE(false);
  co_return;
  //  auto fragment_size = fragmented_temporary_buffer::default_fragment_size;
  //  auto buffer_size = fragment_size - 10;
  //  std::string data(buffer_size, 'c');
  //  fragmented_temporary_buffer buffer(buffer_size, 4096);
  //  buffer.as_ostream().write(data.data(), data.size());
  //  EXPECT_EQ(buffer.size(), buffer_size);
  //  int i = 0;
  //  for (auto it = buffer.begin(); it != buffer.end(); it++, i++) {}
  //  EXPECT_EQ(i, 1);
  //  co_return;
}
