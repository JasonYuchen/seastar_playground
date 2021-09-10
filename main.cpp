#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <iostream>
#include <chrono>

using namespace std;
using namespace chrono_literals;

class test {
 public:
  int _id = -1;
  explicit test(int id) : _id(id) {
    cout << "ctor " << _id << endl;
  }
  test(test&& r) noexcept {
    _id = r._id; r._id = -1;
    cout << "move ctor from " << _id << endl;
  }
  test& operator=(test&& r) noexcept {
    _id = r._id; r._id = -1;
    cout << "move assign from " << _id << endl;
    return *this;
  }
  ~test() {
    cout << "dtor " << _id << endl;
  }

  seastar::future<> handle() {
    cout << "start" << endl;
    co_await seastar::sleep(1s);
    cout << "done" << endl;
  }
};

seastar::future<> handle(test o) {
  co_return co_await o.handle();
}
seastar::future<> handle_lref(test& o) {
  co_return co_await o.handle();
}
seastar::future<> handle_rref(test&& o) {
  co_return co_await o.handle();
}
seastar::future<> handle_sp(seastar::lw_shared_ptr<test> o) {
  co_return co_await o->handle();
}


int main(int argc, char** argv) {
  seastar::app_template app;
  app.run(argc, argv, [] () -> seastar::future<> {
    for (int i = 1; i <= 2; ++i) {
      // 1
      test o(i);
//      (void)o.handle();
      // 2
//       (void)handle(std::move(o));
      // 3
//       (void)handle_lref(o);
      // 4
       (void)handle_rref(std::move(o));
      // 5
//       auto p = seastar::make_lw_shared<test>(i);
//       (void)handle_sp(p);
    }
    co_await seastar::sleep(3s);
  });
}