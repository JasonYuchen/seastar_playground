//
// Created by jason on 2021/9/12.
//

#include "util/fragmented_temporary_buffer.hh"

#include "test/base.hh"

using namespace rafter::test;
using namespace rafter::util;

// TODO: add test cases
class fragmented_temporary_buffer_test
    : public ::testing::Test,
      public ::testing::WithParamInterface<int> {
 protected:
  void SetUp() override {
    auto segment_size = 8192 / GetParam();
    _buffer = std::make_unique<fragmented_temporary_buffer>(segment_size);
  }

  std::unique_ptr<fragmented_temporary_buffer> _buffer;
};

INSTANTIATE_TEST_SUITE_P(fragmented_temporary_buffer,
                         fragmented_temporary_buffer_test,
                         ::testing::Values(1, 2, 3));

RAFTER_TEST(fragmented_temporary_buffer_test, zero_can_be_expanded) {
  l.info("running case 1");
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

RAFTER_TEST_P(fragmented_temporary_buffer_test, read_write) {
  l.info("running case 2");
  co_return;
}
