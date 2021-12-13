//
// Created by jason on 2021/12/8.
//

#include "config.hh"

#include "rafter/logger.hh"
#include "util/error.hh"

namespace rafter {

void config::validate() const {
  if (data_dir.empty()) {
    throw util::runtime_error(l, util::code::invalid, "empty data_dir");
  }
}

}  // namespace rafter