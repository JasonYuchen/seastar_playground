//
// Created by jason on 2022/6/5.
//

#pragma once

#include "storage/index.hh"
#include "test/base.hh"

namespace rafter::test {

// This class is for testing private members of various classes.
// Instead of adding a bunch of friend classes, the private members are
// explicitly exposed via this helper.
class storage_helper {
 public:
  // storage::index
  PUBLISH_VARIABLE(storage::index, _entries);
};

}  // namespace rafter::test
