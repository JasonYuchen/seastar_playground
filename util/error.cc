//
// Created by jason on 2021/11/26.
//

#include "error.hh"

namespace rafter::util {

const char* status_string(enum code e) {
  static const char* s[] = {
      "ok",
      "configuration",
      "serialization",
      "short_read",
      "short_write",
      "peer_not_found",
      "compacted",
      "unavailable",
      "out_of_range",
      "out_of_date",
      "closed",
      "timed_out",
      "cancelled",
      "corruption",
      "failed_precondition",
      "failed_postcondition",
      "invalid_argument",
      "invalid_raft_state",
      "no_data",
      "exhausted",
      "unknown"};
  static_assert(
      sizeof(s) / sizeof(s[0]) == static_cast<int>(code::num_of_codes));
  return s[static_cast<uint8_t>(e)];
}

}  // namespace rafter::util
