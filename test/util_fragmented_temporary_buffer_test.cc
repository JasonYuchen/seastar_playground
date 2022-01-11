//
// Created by jason on 2021/9/12.
//

#include <string_view>

#include "test/base.hh"
#include "util/error.hh"
#include "util/fragmented_temporary_buffer.hh"

using namespace rafter::util;
using view = rafter::util::fragmented_temporary_buffer::view;
using namespace std::string_view_literals;

namespace {

class fragmented_temporary_buffer_basic : public ::testing::Test {
 protected:
  void SetUp() override {
    _buffer = std::make_unique<fragmented_temporary_buffer>();
  }

  std::unique_ptr<fragmented_temporary_buffer> _buffer;
};

RAFTER_TEST_F(fragmented_temporary_buffer_basic, zero_can_be_expanded) {
  auto os = _buffer->as_ostream();
  os.write(123ULL);
  os.write("number", 6);
  _buffer->remove_suffix(_buffer->bytes() - 14);
  EXPECT_EQ(_buffer->bytes(), 14);
  auto is = _buffer->as_istream();
  EXPECT_EQ(is.read<uint64_t>(), 123ULL);
  EXPECT_EQ(view("number"sv), is.read(6));
  EXPECT_EQ(_buffer->bytes(), 14);
  co_return;
}

RAFTER_TEST_F(fragmented_temporary_buffer_basic, throw_if_out_of_range) {
  auto is = _buffer->as_istream();
  EXPECT_THROW(is.read(1), rafter::util::out_of_range_error);
  auto os = _buffer->as_ostream();
  os.write(1ULL);
  _buffer->remove_suffix(_buffer->bytes() - 8);
  is = _buffer->as_istream();
  is.read<uint64_t>();
  EXPECT_THROW(is.read(1), rafter::util::out_of_range_error);
  co_return;
}

RAFTER_TEST_F(fragmented_temporary_buffer_basic, fragment_size_is_honored) {
  _buffer = std::make_unique<fragmented_temporary_buffer>(0, 1024, 2048);
  EXPECT_EQ(_buffer->bytes(), 0);
  auto os = _buffer->as_ostream();
  os.fill(0, 4096);
  EXPECT_EQ(_buffer->bytes(), 4096);
  os.fill(0, 1024);
  EXPECT_EQ(_buffer->bytes(), 6144);
  os.remove_suffix_to_fit();
  EXPECT_EQ(_buffer->bytes(), 5120);
  co_return;
}

class fragmented_temporary_buffer_fit
  : public ::testing::Test
  , public ::testing::WithParamInterface<int> {
 protected:
  void SetUp() override {
    _buffer = std::make_unique<fragmented_temporary_buffer>(64, 2, 16);
  }

  std::unique_ptr<fragmented_temporary_buffer> _buffer;
};

INSTANTIATE_TEST_SUITE_P(
    fragmented_temporary_buffer,
    fragmented_temporary_buffer_fit,
    ::testing::Values(0, 7, 14, 21, 28, 35, 42, 49, 56, 63),
    ::testing::PrintToStringParamName());

RAFTER_TEST_P(fragmented_temporary_buffer_fit, remove_to_fit) {
  std::string data(GetParam(), 'c');
  auto os = _buffer->as_ostream();
  os.write(data.data(), data.size());
  os.remove_suffix_to_fit();
  EXPECT_EQ(_buffer->bytes(), GetParam());
  auto is = _buffer->as_istream();
  EXPECT_EQ(is.bytes_left(), GetParam());
  // if read(0) will trigger a false UB alarm, avoid it
  if (GetParam() > 0) {
    EXPECT_EQ(view(data), is.read(GetParam()));
  }
  co_return;
}

class fragmented_temporary_buffer_segment
  : public ::testing::Test
  , public ::testing::WithParamInterface<int> {
 protected:
  void SetUp() override {
    // for case 1: 1 x 8192
    // for case 2: 2 x 4096
    // for case 3: 3 x 4096 (aligned up 40 default 4096)
    auto size = 8192;
    auto fragment_size = size / GetParam();
    _buffer = std::make_unique<fragmented_temporary_buffer>(
        size,
        fragmented_temporary_buffer::DEFAULT_FRAGMENT_ALIGNMENT,
        fragment_size);
  }

  std::unique_ptr<fragmented_temporary_buffer> _buffer;
};

INSTANTIATE_TEST_SUITE_P(
    fragmented_temporary_buffer,
    fragmented_temporary_buffer_segment,
    ::testing::Values(1, 2, 3),
    ::testing::PrintToStringParamName());

RAFTER_TEST_P(
    fragmented_temporary_buffer_segment, read_write_across_fragments) {
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
  EXPECT_EQ(view(std::string(4000, 0)), is.read(4000));
  is.skip(90);
  EXPECT_EQ(UINT64_MAX, is.read<uint64_t>());
  EXPECT_EQ("number", is.read_string(6));
  EXPECT_EQ(view(std::string(3000, 'c')), is.read(3000));
  std::string reassemble;
  for (auto&& fragment : *_buffer) {
    switch (GetParam()) {
      case 1:
        EXPECT_EQ(fragment.size(), 8192);
        break;
      case 2:
        EXPECT_EQ(fragment.size(), 4096);
        break;
      case 3:
        EXPECT_EQ(fragment.size(), 4096);
        break;
      default:
        ADD_FAILURE();
    }
    reassemble.append(fragment);
  }
  reassemble = reassemble.substr(0, all.size());
  EXPECT_EQ(reassemble, all);
  co_return;
}

}  // namespace
