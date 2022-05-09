//
// Created by jason on 2022/4/28.
//

#include "client.hh"

#include <chrono>
#include <random>

#include "util/error.hh"

namespace rafter::protocol {

session::session(uint64_t cluster_id, bool noop)
  : cluster_id(cluster_id)
  , client_id(next_client_id())
  , series_id(noop ? NOOP_SERIES_ID : NOOP_SERIES_ID + 1) {}

uint64_t session::next_client_id() {
  static std::mt19937_64 random_engine(
      std::chrono::system_clock::now().time_since_epoch().count());
  uint64_t id = NOT_MANAGED_ID;
  while (id == NOT_MANAGED_ID) {
    id = random_engine();
  }
  return id;
}

void session::prepare_for_register() {
  assert_regular();
  series_id = REGISTRATION_SERIES_ID;
}

void session::prepare_for_unregister() {
  assert_regular();
  series_id = UNREGISTRATION_SERIES_ID;
}

void session::prepare_for_propose() {
  assert_regular();
  series_id = INITIAL_SERIES_ID;
}

void session::proposal_completed() {
  assert_regular();
  if (series_id == responded_to + 1) {
    responded_to = series_id;
    series_id++;
  } else {
    throw util::request_error("id mismatch");
  }
}

bool session::is_valid_for_proposal(uint64_t cluster) const {
  if (series_id == NOOP_SERIES_ID && client_id == NOT_MANAGED_ID) {
    return false;
  }
  if (cluster_id != cluster) {
    return false;
  }
  if (client_id == NOT_MANAGED_ID) {
    return false;
  }
  if (series_id == REGISTRATION_SERIES_ID ||
      series_id == UNREGISTRATION_SERIES_ID) {
    return false;
  }
  if (responded_to > series_id) {
    throw util::request_error("id mismatch");
  }
  return true;
}

bool session::is_valid_for_session_operation(uint64_t cluster) const {
  if (cluster_id != cluster) {
    return false;
  }
  if (client_id == NOT_MANAGED_ID || series_id == NOOP_SERIES_ID) {
    return false;
  }
  if (series_id == REGISTRATION_SERIES_ID ||
      series_id == UNREGISTRATION_SERIES_ID) {
    return true;
  }
  return false;
}

void session::assert_regular() const {
  if (client_id == NOT_MANAGED_ID || series_id == NOOP_SERIES_ID) {
    throw util::request_error("not a regular session");
  }
}

}  // namespace rafter::protocol
