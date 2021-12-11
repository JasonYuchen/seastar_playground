//
// Created by jason on 2021/11/26.
//

#include "error.hh"

namespace rafter::util {

const char* status_string(enum code e) {
  static const char* s[] = {
      "ok",
      "short_read",
      "short_write",
      "compacted",
      "unavailable",
      "out_of_range",
      "closed",
      "timed_out",
      "cancelled",
      "corruption",
      "failed_precondition",
      "invalid",
      "no_data",
      "unknown"
  };
  static_assert(sizeof(s)/sizeof(s[0]) == static_cast<int>(code::num_of_codes));
  return s[static_cast<uint8_t>(e)];
}

}  // namespace rafter::util
