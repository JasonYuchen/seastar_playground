//
// Created by jason on 2021/12/12.
//

#include "stats.hh"

namespace rafter::storage {

void stats::operator+=(const stats &rhs) {
  _append += rhs._append;
  _append_snap += rhs._append_snap;
  _append_state += rhs._append_state;
  _append_entry += rhs._append_entry;
  _remove += rhs._remove;
  _query_snap += rhs._query_snap;
  _query_state += rhs._query_state;
  _query_entry += rhs._query_entry;
  _sync += rhs._sync;
  _new_segment += rhs._new_segment;
  _del_segment += rhs._del_segment;
}

}  // namespace rafter::storage
