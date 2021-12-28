//
// Created by jason on 2021/12/27.
//

#include "test/base.hh"
#include "util/worker.hh"

using namespace rafter::util;
using namespace seastar;

using rafter::test::base;
using rafter::test::l;

namespace {

class worker_test : public ::testing::Test {
 protected:
  void SetUp() override {
    base::submit([this]() -> future<> {
      _worker = std::make_unique<worker<int>>("test_worker", 10);
      co_return;
    });
  }

  void TearDown() override {
    base::submit([this]() -> future<> {
      co_await _worker->close();
      _worker.reset();
      co_return;
    });
  }

  std::unique_ptr<worker<int>> _worker;
};

RAFTER_TEST_F(worker_test, normal) {
  promise<> pr;
  auto fut = pr.get_future();
  std::vector<int> result;
  std::vector<int> expect;
  _worker->start([pr = std::move(pr), &result](int a) mutable -> future<> {
    result.push_back(a);
    if (a == 9) {
      pr.set_value();
    }
    co_return;
  });
  for (int i = 0; i < 10; ++i) {
    expect.push_back(i);
    co_await _worker->push_eventually(int{i});
  }
  co_await fut.discard_result();
  EXPECT_EQ(result, expect);
  co_return;
}

RAFTER_TEST_F(worker_test, multiple_producer) {
  promise<> pr;
  auto fut = pr.get_future();
  std::vector<int> result;
  std::vector<int> expect;
  std::vector<future<>> turn;
  for (int i = 0; i < 15; ++i) {
    expect.push_back(i);
    turn.emplace_back(_worker->push_eventually(int{i}));
    if (i < 10) {
      EXPECT_TRUE(turn.back().available());
    } else {
      EXPECT_FALSE(turn.back().available());
    }
  }
  EXPECT_EQ(_worker->waiters(), 5);
  _worker->start([pr = std::move(pr), &result](int a) mutable -> future<> {
    if (!result.empty()) {
      EXPECT_EQ(result.back() + 1, a) << "incorrect order";
    }
    result.push_back(a);
    if (a == 14) {
      pr.set_value();
    }
    co_return;
  });
  co_await fut.discard_result();
  for (auto& f : turn) {
    EXPECT_TRUE(f.available());
  }
  EXPECT_EQ(result, expect);
  EXPECT_EQ(_worker->waiters(), 0);
  co_return;
}

}  // namespace
