//
// Created by jason on 2022/4/25.
//

#include "session.hh"

#include "util/error.hh"

namespace rafter::rsm {

using namespace protocol;

session::session(uint64_t client_id) : _client_id(client_id) {}

void session::response(uint64_t series_id, rsm_result result) {
  auto it = _history.find(series_id);
  if (it != _history.end()) [[unlikely]] {
    // throw: duplicate
    throw util::failed_precondition_error("duplicate series id");
  }
  _history.emplace_hint(it, series_id, std::move(result));
}

std::optional<rsm_result> session::response(uint64_t series_id) const {
  auto it = _history.find(series_id);
  if (it == _history.end()) {
    return std::nullopt;
  }
  return it->second;
}

bool session::has_response(uint64_t series_id) const {
  return _history.contains(series_id);
}

void session::clear_to(uint64_t series_id) {
  if (series_id <= _responded) [[unlikely]] {
    return;
  }
  if (series_id == _responded + 1) {
    _responded = series_id;
    _history.erase(series_id);
    return;
  }
  _responded = series_id;
  std::erase_if(_history, [series_id](const auto& item) {
    return item.first <= series_id;
  });
}

bool session::has_responded(uint64_t series_id) const {
  return series_id <= _responded;
}

}  // namespace rafter::rsm
