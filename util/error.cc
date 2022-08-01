//
// Created by jason on 2021/11/26.
//

#include "error.hh"

#include "seastar/util/backtrace.hh"
#include "util/seastarx.hh"

namespace rafter::util {

std::string_view status_string(enum code e) {
  static std::string_view s[] = {
      "ok",
      "panic",
      "aborted",
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
  static_assert(std::size(s) == static_cast<int>(code::num_of_codes));
  return s[static_cast<uint8_t>(e)];
}

void panic::panic_with_backtrace(std::string_view msg) {
  throw_with_backtrace<panic>(msg);
}

std::exception_ptr panic::panic_ptr_with_backtrace(std::string_view msg) {
  return make_backtraced_exception_ptr<panic>(msg);
}

}  // namespace rafter::util
