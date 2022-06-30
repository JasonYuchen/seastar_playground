//
// Created by jason on 2022/5/8.
//

#include "file.hh"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>

namespace rafter::util {

future<> create_file(
    std::string_view dir, std::string_view name, std::string msg) {
  auto of = open_flags::create | open_flags::wo | open_flags::truncate |
            open_flags::dsync;
  return open_file_dma(std::filesystem::path(dir).append(name).string(), of)
      .then([msg = std::move(msg)](file f) mutable {
        return make_file_output_stream(std::move(f))
            .then([msg = std::move(msg)](output_stream<char>&& out) mutable {
              return do_with(
                  std::move(out),
                  std::move(msg),
                  [](output_stream<char>& out, std::string& msg) {
                    return out.write(msg)
                        .then([&out] { return out.flush(); })
                        .then([&out] { return out.close(); });
                  });
            });
      })
      .then([dir] { return sync_directory(dir); });
}

future<temporary_buffer<char>> read_file(
    std::string_view dir, std::string_view name) {
  return open_file_dma(
             std::filesystem::path(dir).append(name).string(), open_flags::ro)
      .then([](file f) {
        return do_with(std::move(f), [](file& f) {
          return f.size().then([&](size_t size) {
            return do_with(
                make_file_input_stream(f), [size](input_stream<char>& in) {
                  return in.read_exactly(size).finally(
                      [&in] { return in.close(); });
                });
          });
        });
      });
}

future<bool> exist_file(std::string_view dir, std::string_view name) {
  return file_exists(std::filesystem::path(dir).append(name).string());
}

future<> remove_file(std::string_view dir, std::string_view name) {
  return seastar::remove_file(std::filesystem::path(dir).append(name).string())
      .then([dir] { return sync_directory(dir); });
}

}  // namespace rafter::util
