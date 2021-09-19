//
// Created by jason on 2021/9/15.
//

#pragma once

#include <thread>

#include <gtest/gtest.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>


#define RAFTER_TEST_CASE_F(suite, name) \
  TEST_F(suite, name) {                 \
    auto f =                             \
  }                                     \
  seastar::future<> GTEST_TEST_CLASS_NAME_(suite, name)::SeastarTestBody()

class rafter_test_base {
 public:
  inline static const int argc = 3;
  inline static const char* argv[] = {"rafter_test_base", ""};
  rafter_test_base() {
    engine_thread_ = std::thread([this] {
      return app_.run(argc, const_cast<char **>(argv), []() -> seastar::future<> {
        // TODO: handle signal
        co_return;
      });
    });
  }
  ~rafter_test_base() {
    // TODO: send signal
    engine_thread_.join();
  }
 private:
  std::thread engine_thread1_;
  seastar::app_template app_;
};