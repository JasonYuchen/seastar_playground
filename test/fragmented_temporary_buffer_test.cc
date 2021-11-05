//
// Created by jason on 2021/9/12.
//

#include <string_view>

#include "util/fragmented_temporary_buffer.hh"

#include "test/base.hh"

using namespace rafter::test;
using namespace rafter::util;
using namespace std::string_view_literals;

// TODO: add test cases
class fragmented_temporary_buffer_test
    : public ::testing::Test,
      public ::testing::WithParamInterface<int> {
 protected:
  void SetUp() override {
    // for case 1: 1 x 8192
    // for case 2: 2 x 4096
    // for case 3: 3 x 4096 (aligned up 40 default 4096)
    auto size = 8192;
    auto fragment_size = size / GetParam();
    _buffer = std::make_unique<fragmented_temporary_buffer>(
        size,
        fragmented_temporary_buffer::default_fragment_alignment,
        fragment_size);
  }

  std::unique_ptr<fragmented_temporary_buffer> _buffer;
};

INSTANTIATE_TEST_SUITE_P(fragmented_temporary_buffer,
                         fragmented_temporary_buffer_test,
                         ::testing::Values(1, 2, 3),
                         ::testing::PrintToStringParamName());

RAFTER_TEST(fragmented_temporary_buffer_test, zero_can_be_expanded) {
  fragmented_temporary_buffer buffer;
  auto os = buffer.as_ostream();
  os.write(123ULL);
  os.write("number", 6);
  buffer.remove_suffix(buffer.bytes() - 14);
  EXPECT_EQ(buffer.bytes(), 14);
  auto is = buffer.as_istream();
  EXPECT_EQ(is.read<uint64_t>(), 123ULL);
  EXPECT_EQ("number"sv, is.read(6));
  EXPECT_EQ(buffer.bytes(), 14);
  co_return;
}

RAFTER_TEST(fragmented_temporary_buffer_test, throw_if_out_of_range) {
  fragmented_temporary_buffer buffer;
  auto is = buffer.as_istream();
  EXPECT_THROW(is.read(1), std::out_of_range);
  auto os = buffer.as_ostream();
  os.write(1ULL);
  buffer.remove_suffix(buffer.bytes() - 8);
  is = buffer.as_istream();
  is.read<uint64_t>();
  EXPECT_THROW(is.read(1), std::out_of_range);
  co_return;
}

RAFTER_TEST(fragmented_temporary_buffer_test, from_stream) {
  // TODO
  co_return;
}

RAFTER_TEST_P(fragmented_temporary_buffer_test, read_write_across_fragments) {
  // TODO
  std::string all(4090, 0);
  auto os = _buffer->as_ostream();
  os.fill(0, 4090);
  auto umax = UINT64_MAX;
  all.append(reinterpret_cast<const char*>(&umax), sizeof(umax));
  os.write(UINT64_MAX);
  all.append("number", 6);
  os.write("number", 6);
  std::string data(3000, 'c');
  all.append(data);
  os.write(data.data(), data.size());
  auto is = _buffer->as_istream();
  EXPECT_EQ(std::string(4000, 0), is.read(4000));
  is.skip(90);
  EXPECT_EQ(UINT64_MAX, is.read<uint64_t>());
  EXPECT_EQ("number", is.read_string(6));
  EXPECT_EQ(std::string(3000, 'c'), is.read(3000));
  std::string reassemble;
  for(auto&& fragment : *_buffer) {
    switch (GetParam()) {
      case 1:
        EXPECT_EQ(fragment.size(), 8192); break;
      case 2:
        EXPECT_EQ(fragment.size(), 4096); break;
      case 3:
        EXPECT_EQ(fragment.size(), 4096); break;
      default:
        EXPECT_TRUE(false);
    }
    reassemble.append(fragment);
  }
  reassemble = reassemble.substr(0, all.size());
  EXPECT_EQ(reassemble, all);
  co_return;
}
