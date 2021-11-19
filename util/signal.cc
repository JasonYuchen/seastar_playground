//
// Created by jason on 2021/9/21.
//

#include "signal.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>

using namespace seastar;

namespace rafter::util {

stop_signal::stop_signal() {
  engine().handle_signal(SIGINT, [this] { signaled(SIGINT); });
  engine().handle_signal(SIGTERM, [this] { signaled(SIGTERM); });
}

stop_signal::~stop_signal() {
  // There's no way to unregister a handler yet, so register a no-op handler
  // instead.
  engine().handle_signal(SIGINT, [] {});
  engine().handle_signal(SIGTERM, [] {});
}

future<int> stop_signal::wait() {
  co_await _cond.wait([this] { return _caught; });
  co_return _signum;
}

void stop_signal::signaled(int signum) {
  if (_caught) {
    return;
  }
  _signum = signum;
  _caught = true;
  _cond.broadcast();
}

}  // namespace rafter::util
