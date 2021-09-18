//
// Created by jason on 2021/9/15.
//

#pragma once

#include <seastar/testing/test_case.hh>

#define RAFTER_TEST_CASE(name) \
  SEASTAR_TEST_CASE(__file__ ## name)
