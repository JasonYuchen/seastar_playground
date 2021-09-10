//
// Created by jason on 2021/8/28.
//

#ifndef SEASTAR_PLAYGROUND__ECHO_H_
#define SEASTAR_PLAYGROUND__ECHO_H_

seastar::future<> handle_connection(seastar::connected_socket s,
                                    seastar::socket_address a) {
  uint64_t talked_bytes = 0;
  auto out = s.output();
  auto in = s.input();
  while (true) {
    auto in_buf = co_await in.read();
    if (in_buf) {
      talked_bytes += in_buf.size();
      co_await out.write(std::move(in_buf));
      co_await out.flush();
    } else {
      break;
    }
  }
  co_await out.close();
  fmt::print(stdout, "closing connection from {}, talked {} bytes\n", a, talked_bytes);
  co_return;
}

seastar::future<> service_loop() {
  seastar::listen_options lo;
  lo.reuse_address = true;
  auto listener = seastar::listen(seastar::make_ipv4_address({8086}), lo);
  while (true) {
    auto res = co_await listener.accept();
    fmt::print(stdout, "accepted connection from {}\n", res.remote_address);
    (void)handle_connection(std::move(res.connection), std::move(res.remote_address))
    .handle_exception([] (std::exception_ptr ep) {
      fmt::print(stderr, "Could not handle connection: {}\n", ep);
    });
  }
}

#endif //SEASTAR_PLAYGROUND__ECHO_H_
