//
// Created by jason on 2021/12/8.
//

#include "config.hh"

#include "rafter/logger.hh"
#include "util/error.hh"

namespace rafter {

void config::validate() const {
  if (data_dir.empty()) {
    throw util::configuration_error("data_dir", "empty");
  }
}

}  // namespace rafter
