#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>
#include <iostream>
#include <chrono>

#include "util/fragmented_temporary_buffer.hh"

using namespace std;
using namespace chrono_literals;

int main(int argc, char** argv) {
  seastar::app_template app;
  app.run(argc, argv, [] () -> seastar::future<> {
    fragmented_temporary_buffer buf(49, 4096);
    std::cout << buf.size() << std::endl;
    buf.remove_suffix(buf.size() - 49);
    std::cout << buf.size() << std::endl;
    auto out = buf.as_ostream();
    std::string a(25, 'a');
    std::string b(24, 'b');
    std::cout << "writing a" << std::endl;
    out.write(a.data(), a.size());
    std::cout << "writing b" << std::endl;
    out.write(b.data(), b.size());
    buf.remove_prefix(5);
    int cnt = 0;
    auto it = buf.begin();
    while (true) {
      if (it == buf.end()) {
        break;
      }
      cnt++;
      std::cout << cnt << " fragment" << std::endl;
      std::cout << "\tsize " << it->size() << std::endl;
      std::cout << "\tcontent " << std::string(it->data(), it->size()) << std::endl;
      it++;
    }
    co_return;

  });
}