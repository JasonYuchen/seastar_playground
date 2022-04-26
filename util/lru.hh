//
// Created by jason on 2022/4/26.
//

#pragma once

#include <stdint.h>

#include <functional>
#include <list>
#include <stdexcept>
#include <unordered_map>

namespace rafter::util {

template <typename K, typename V>
class lru {
 public:
  using entry = std::pair<K, V>;
  using node = typename std::list<entry>::iterator;
  using on_eviction = std::function<void(entry&&)>;

  explicit lru(uint64_t capacity) : _capacity(capacity) {
    _map.reserve(capacity);
  }

  lru(uint64_t capacity, on_eviction cb)
    : _capacity(capacity), _on_eviction(std::move(cb)) {}

  // TODO(jyc): support emplace

  std::pair<node, bool> insert(const K& k, const V& v) {
    auto it = _map.find(k);
    if (it != _map.end()) {
      return {_list.begin(), false};
    }
    _list.emplace_front(k, v);
    _map[k] = _list.begin();
    expire();
    return {_list.begin(), true};
  }

  std::pair<node, bool> insert(const K& k, V&& v) {
    auto it = _map.find(k);
    if (it != _map.end()) {
      return {_list.begin(), false};
    }
    _list.emplace_front(k, std::move(v));
    _map[k] = _list.begin();
    expire();
    return {_list.begin(), true};
  }

  std::pair<node, bool> upsert(const K& k, const V& v) {
    auto it = _map.find(k);
    if (it != _map.end()) {
      it->second->second = v;
      _list.splice(_list.begin(), _list, it->second);
      return {_list.begin(), false};
    }
    _list.emplace_front(k, v);
    _map[k] = _list.begin();
    expire();
    return {_list.begin(), true};
  }

  std::pair<node, bool> upsert(const K& k, V&& v) {
    auto it = _map.find(k);
    if (it != _map.end()) {
      it->second->second = std::move(v);
      _list.splice(_list.begin(), _list, it->second);
      return {_list.begin(), false};
    }
    _list.emplace_front(k, std::move(v));
    _map[k] = _list.begin();
    expire();
    return {_list.begin(), true};
  }

  bool erase(const K& k) {
    auto it = _map.find(k);
    if (it == _map.end()) {
      return false;
    }
    _list.erase(it->second);
    _map.erase(it);
    return true;
  }

  V& operator[](const K& k) {
    auto it = _map.find(k);
    if (it == _map.end()) {
      throw std::out_of_range("key not found");
    }
    _list.splice(_list.begin(), _list, it->second);
    return it->second->second;
  }

  node find(const K& k) {
    auto it = _map.find(k);
    if (it == _map.end()) {
      return _list.end();
    }
    _list.splice(_list.begin(), _list, it->second);
    return it->second;
  }

  node end() { return _list.end(); }

  void contain(const K& k) { return _map.contains(k); }

  size_t size() const { return _map.size(); }

  size_t capacity() const { return _capacity; }

 private:
  bool expire() {
    if (_map.size() > _capacity) {
      _map.erase(_list.back().first);
      if (_on_eviction) {
        _on_eviction(std::move(_list.back()));
      }
      _list.pop_back();
      return true;
    }
    return false;
  }

  uint64_t _capacity;
  std::function<void(entry&&)> _on_eviction;
  std::list<entry> _list;
  std::unordered_map<K, node> _map;
};

}  // namespace rafter::util
