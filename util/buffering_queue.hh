//
// Created by jason on 2022/4/13.
//

#pragma once

#include <list>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>
#include <vector>

#include "util/error.hh"

namespace rafter::util {

template <typename T>
class buffering_queue {
 public:
  explicit buffering_queue(std::string name, size_t capacity)
    : _name(std::move(name)), _capacity(capacity) {
    _q[0].reserve(_capacity);
    _q[1].reserve(_capacity);
  }

  const std::string& name() const { return _name; }

  bool is_open() const { return _open; }

  template <typename Func>
    requires requires(Func f, std::vector<T>& data, bool& open) {
               { f(data, open) } -> std::same_as<seastar::future<>>;
             }
  seastar::future<> consume(Func&& func) {
    if (_ex) {
      std::rethrow_exception(_ex);
    }
    co_await not_empty();
    if (!q().empty()) {
      _switch_cnt++;
      _task_cnt += q().size();
      // switch q()
      _curr_idx++;
      co_await std::invoke(
          std::forward<Func>(func),
          std::ref(_q[(_curr_idx - 1) % 2]),
          std::ref(_open));
      _q[(_curr_idx - 1) % 2].clear();
    }
    try_release_waiter();
  }

  seastar::future<> close() {
    _open = false;
    _ex = std::make_exception_ptr(util::closed_error{_name});
    notify_not_empty();
    try_release_waiter();
    co_return;
  }

  seastar::future<> push_eventually(T&& task) {
    if (_ex) {
      std::rethrow_exception(_ex);
    }
    try_release_waiter();
    if (!full()) {
      q().emplace_back(std::move(task));
      notify_not_empty();
      return seastar::make_ready_future<>();
    }
    seastar::promise<> pr;
    auto fut = pr.get_future();
    _waiter.emplace_back(std::move(pr), std::move(task));
    return fut;
  }

  bool push(T& task) {
    if (_ex) {
      std::rethrow_exception(_ex);
    }
    if (!full()) {
      q().emplace_back(std::move(task));
      notify_not_empty();
      return true;
    }
    return false;
  }

  bool full() const { return size() == _capacity; }

  size_t size() const { return q().size(); }

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

  const std::vector<T>& q() const { return _q[_curr_idx % 2]; }

  void notify_not_empty() {
    if (_not_empty) {
      _not_empty->set_value();
      _not_empty = std::nullopt;
    }
  }

  seastar::future<> not_empty() {
    if (!q().empty()) {
      return seastar::make_ready_future<>();
    }
    _not_empty = seastar::promise<>();
    return _not_empty->get_future();
  }

  void try_release_waiter() {
    if (_ex) {
      while (!_waiter.empty()) {
        _waiter.front().pr.set_exception(_ex);
        _waiter.pop_front();
      }
    } else {
      while (!full() && !_waiter.empty()) {
        auto& e = _waiter.front();
        q().emplace_back(std::move(e.t));
        e.pr.set_value();
        _waiter.pop_front();
        notify_not_empty();
      }
    }
  }

  std::string _name;
  bool _open = true;
  size_t _switch_cnt = 0;
  size_t _task_cnt = 0;
  size_t _capacity = 0;
  uint64_t _curr_idx = 0;
  std::vector<T> _q[2];
  std::optional<seastar::promise<>> _not_empty;
  std::list<entry> _waiter;
  std::exception_ptr _ex = nullptr;
};

}  // namespace rafter::util
