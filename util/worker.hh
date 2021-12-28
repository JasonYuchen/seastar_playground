//
// Created by jason on 2021/12/27.
//

#pragma once

#include <list>
#include <seastar/core/future.hh>
#include <seastar/core/queue.hh>

#include "util/error.hh"

namespace rafter::util {

// TODO: use double-buffering
template <typename T>
class worker {
 public:
  explicit worker(std::string name, size_t capacity)
    : _name(std::move(name)), _q(capacity) {}

  template <typename Func>
  void start(Func func) {
    _open = true;
    _service = run(std::move(func));
  }

  seastar::future<> close() {
    _open = false;
    _q.abort(std::make_exception_ptr(util::closed_error(_name)));
    while (!_waiter.empty()) {
      auto& e = _waiter.front();
      e.pr.set_exception(std::make_exception_ptr(util::closed_error(_name)));
      _waiter.pop_front();
    }
    if (_service) {
      co_return co_await _service->discard_result();
    }
    co_return;
  }

  seastar::future<> push_eventually(T&& task) {
    while (!_q.full() && !_waiter.empty()) {
      auto& e = _waiter.front();
      bool queued = _q.push(std::move(e.t));
      assert(queued);
      e.pr.set_value();
      _waiter.pop_front();
    }
    if (!_q.full()) {
      bool queued = _q.push(std::move(task));
      assert(queued);
      return seastar::make_ready_future<>();
    } else {
      seastar::promise<> pr;
      auto fut = pr.get_future();
      _waiter.push_back(entry{std::move(pr), std::move(task)});
      return fut;
    }
  }

  size_t waiters() const { return _waiter.size(); }

  size_t task_cnt() const { return _task_cnt; }

 private:
  struct entry {
    seastar::promise<> pr;
    T t;
    entry(seastar::promise<>&& pr, T&& t)
      : pr(std::move(pr)), t(std::move(t)) {}
  };

  template <typename Func>
  seastar::future<> run(Func func) {
    while (_open) {
      try {
        co_await _q.not_empty();
      } catch (util::closed_error&) {
        co_return;
      }

      while (!_q.empty()) {
        auto item = _q.pop();
        co_await func(std::move(item));
        _task_cnt++;
        // TODO: check need_preempt
      }

      while (!_q.full() && !_waiter.empty()) {
        auto& e = _waiter.front();
        bool queued = _q.push(std::move(e.t));
        assert(queued);
        e.pr.set_value();
        _waiter.pop_front();
      }
    }
  }

  std::string _name;
  bool _open = false;
  size_t _task_cnt = 0;
  seastar::queue<T> _q;
  std::list<entry> _waiter;
  std::optional<seastar::future<>> _service;
};

}  // namespace rafter::util
