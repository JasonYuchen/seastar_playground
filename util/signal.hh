//
// Created by jason on 2021/9/19.
//

#pragma once

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>

namespace rafter::util {

class stop_signal {
 public:
  stop_signal();
  ~stop_signal();
  seastar::future<> wait();
  bool stopping() const { return _caught; }

 private:
  void signaled();

 private:
  bool _caught = false;
  seastar::condition_variable _cond;
};

}  // namespace rafter::util
