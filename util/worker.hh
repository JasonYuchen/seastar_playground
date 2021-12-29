//
// Created by jason on 2021/12/27.
//

#pragma once

#include <list>
#include <optional>
#include <seastar/core/future.hh>
#include <vector>

#include "util/error.hh"

namespace rafter::util {

template <typename T>
class worker {
 public:
  explicit worker(std::string name, size_t capacity)
    : _name(std::move(name)), _capacity(capacity) {
    _q[0].reserve(_capacity);
    _q[1].reserve(_capacity);
  }

  // TODO: add concept, Func subject to `seastar::future<> (std::vector<T>&)`
  template <typename Func>
  requires requires(Func f, std::vector<T>& a) {
    { f(a) } -> std::same_as<seastar::future<>>;
  }
  void start(Func func) {
    if (!_open && !_service) {
      _open = true;
      _service = run(std::move(func));
    }
  }

  seastar::future<> close() {
    _open = false;
    if (_not_empty) {
      _not_empty->set_value();
      _not_empty = std::nullopt;
    }
    while (!_waiter.empty()) {
      auto& e = _waiter.front();
      e.pr.set_exception(std::make_exception_ptr(util::closed_error(_name)));
      _waiter.pop_front();
    }
    if (_service) {
      co_await _service->discard_result();
      _service.reset();
    }
    co_return;
  }

  seastar::future<> push_eventually(T&& task) {
    while (!full() && !_waiter.empty()) {
      auto& e = _waiter.front();
      q().emplace_back(std::move(e.t));
      e.pr.set_value();
      _waiter.pop_front();
      notify_not_empty();
    }
    if (!full()) {
      q().emplace_back(std::move(task));
      notify_not_empty();
      return seastar::make_ready_future<>();
    } else {
      seastar::promise<> pr;
      auto fut = pr.get_future();
      _waiter.emplace_back(std::move(pr), std::move(task));
      return fut;
    }
  }

  bool full() const { return _q[_curr_idx % 2].size() == _capacity; }

  size_t waiters() const { return _waiter.size(); }

  size_t task_cnt() const { return _task_cnt; }

  size_t switch_cnt() const { return _switch_cnt; }

 private:
  struct entry {
    seastar::promise<> pr;
    T t;
    entry(seastar::promise<>&& pr, T&& t)
      : pr(std::move(pr)), t(std::move(t)) {}
  };

  std::vector<T>& q() { return _q[_curr_idx % 2]; }

  void notify_not_empty() {
    if (_not_empty) {
      _not_empty->set_value();
      _not_empty = std::nullopt;
    }
  }

  seastar::future<> not_empty() {
    if (!q().empty()) {
      return seastar::make_ready_future<>();
    } else {
      _not_empty = seastar::promise<>();
      return _not_empty->get_future();
    }
  }

  template <typename Func>
  seastar::future<> run(Func func) {
    while (_open) {
      co_await not_empty();
      if (q().empty()) {
        // empty but waked up, closing the worker
        break;
      }
      _switch_cnt++;
      _task_cnt += q().size();
      // switch q()
      _curr_idx++;
      co_await func(_q[(_curr_idx - 1) % 2]);
      _q[(_curr_idx - 1) % 2].clear();

      while (!full() && !_waiter.empty()) {
        auto& e = _waiter.front();
        q().emplace_back(std::move(e.t));
        e.pr.set_value();
        _waiter.pop_front();
      }
    }
  }

  std::string _name;
  bool _open = false;
  size_t _switch_cnt = 0;
  size_t _task_cnt = 0;
  size_t _capacity = 0;
  uint64_t _curr_idx = 0;
  std::vector<T> _q[2];
  std::optional<seastar::promise<>> _not_empty;
  std::list<entry> _waiter;
  std::optional<seastar::future<>> _service;
};

}  // namespace rafter::util
