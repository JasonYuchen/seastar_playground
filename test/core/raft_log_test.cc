//
// Created by jason on 2022/6/5.
//

#include "core/raft_log.hh"

#include "helper.hh"
#include "test/base.hh"
#include "test/test_logdb.hh"

namespace {

using namespace rafter;
using namespace rafter::protocol;

using helper = rafter::test::core_helper;
using rafter::test::l;

class in_memory_log_test : public ::testing::Test {
 protected:
};

}  // namespace
