//
// Created by jason on 2021/12/27.
//

#pragma once

#include "util/buffering_queue.hh"
#include "util/error.hh"

namespace rafter::util {

template <typename T>
class worker {
 public:
  explicit worker(std::string name, size_t capacity, seastar::logger& l)
    : _q(std::move(name), capacity), _l(l) {}

  const std::string& name() const { return _q.name(); }

  bool is_open() const { return _q.is_open(); }

  template <typename Func>
    requires requires(Func f, std::vector<T>& data, bool& open) {
               { f(data, open) } -> std::same_as<seastar::future<>>;
             }
  void start(Func func) {
    if (_q.is_open() && !_service) {
      _service = run(std::move(func));
    }
  }

  seastar::future<> close() {
    co_await _q.close();
    if (_service) {
      co_await _service->handle_exception([this](std::exception_ptr e) {
        _l.warn("worker::close: exception in {}: {}", _q.name(), e);
      });
      _service.reset();
    }
    co_return;
  }

  seastar::future<> push_eventually(T&& task) {
    return _q.push_eventually(std::move(task));
  }

  bool push(T& task) { return _q.push(task); }

  bool full() const { return _q.full(); }

  size_t size() const { return _q.size(); }

  size_t waiters() const { return _q.waiters(); }

  size_t task_cnt() const { return _q.task_cnt(); }

  size_t switch_cnt() const { return _q.switch_cnt(); }

 private:
  template <typename Func>
  seastar::future<> run(Func func) {
    while (_q.is_open()) {
      co_await _q.consume(func);
    }
  }

  buffering_queue<T> _q;
  std::optional<seastar::future<>> _service;
  seastar::logger& _l;
};

}  // namespace rafter::util
