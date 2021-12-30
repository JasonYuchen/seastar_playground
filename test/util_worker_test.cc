//
// Created by jason on 2021/12/27.
//

#include <seastar/core/sleep.hh>

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
  _worker->start(
      [pr = std::move(pr), &result](auto& a, bool& open) mutable -> future<> {
        result.insert(result.end(), a.begin(), a.end());
        if (result.size() == 10) {
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
  _worker->start(
      [pr = std::move(pr), &result](auto& a, bool& open) mutable -> future<> {
        if (!result.empty()) {
          EXPECT_EQ(result.back() + 1, a.front()) << "incorrect order";
        } else {
          // the first full batch
          EXPECT_EQ(a.size(), 10);
        }
        result.insert(result.end(), a.begin(), a.end());
        if (result.size() == 15) {
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

RAFTER_TEST_F(worker_test, close_will_notify_waiter) {
  std::vector<future<>> turn;
  for (int i = 0; i < 15; ++i) {
    turn.emplace_back(_worker->push_eventually(int{i}));
    if (i < 10) {
      EXPECT_TRUE(turn.back().available());
    } else {
      EXPECT_FALSE(turn.back().available());
    }
  }
  EXPECT_EQ(_worker->waiters(), 5);
  co_await _worker->close();
  for (int i = 10; i < 15; ++i) {
    EXPECT_THROW(turn[i].get(), rafter::util::closed_error);
  }
  EXPECT_EQ(_worker->waiters(), 0);
  co_return;
}

RAFTER_TEST_F(worker_test, throw_if_push_to_closed_worker) {
  std::vector<future<>> turn;
  for (int i = 0; i < 15; ++i) {
    turn.emplace_back(_worker->push_eventually(int{i}));
    if (i < 10) {
      EXPECT_TRUE(turn.back().available());
    } else {
      EXPECT_FALSE(turn.back().available());
    }
  }
  bool closed = false;
  _worker->start([&closed](auto& a, bool& open) -> future<> {
    if (a.size() == 10) {
      // yield to wait for closed
      EXPECT_TRUE(open);
      co_await sleep(std::chrono::milliseconds(500));
    }
    closed = closed || !open;
  });
  EXPECT_EQ(_worker->waiters(), 5);
  // yield to let Func run for the first time
  co_await sleep(std::chrono::milliseconds(100));
  co_await _worker->close();
  EXPECT_TRUE(closed);
  for (int i = 10; i < 15; ++i) {
    EXPECT_THROW(turn[i].get(), rafter::util::closed_error);
  }
  EXPECT_EQ(_worker->waiters(), 0);
  EXPECT_THROW(
      co_await _worker->push_eventually(0), rafter::util::closed_error);
  co_return;
}

}  // namespace
