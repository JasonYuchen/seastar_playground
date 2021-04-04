#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <iostream>
#include <chrono>

using namespace std;
using namespace chrono_literals;

seastar::future<> getstatus()
{
  std::cout << 1 << std::endl;
  co_await seastar::sleep(1s);
  std::cout << 2 << std::endl;
  std::cout << "Hello world from" << std::endl;
  auto a = co_await seastar::file_stat("/home/ubuntu");
  std::cout << a.size << std::endl;
  co_await seastar::sleep(1s);
  std::cout << "seastar" << std::endl;
  co_return;
}

int main(int argc, char** argv) {
  seastar::app_template app;
  app.run(argc, argv, [] {
    int *a = new int[1];
    return getstatus();
  });
}