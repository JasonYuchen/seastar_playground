//
// Created by jason on 2021/9/15.
//

#pragma once

#include <thread>
#include <future>

#include <gtest/gtest.h>
#include <seastar/core/alien.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#define RAFTER_TEST_(test_suite_name, test_name, parent_class, parent_id)      \
  static_assert(sizeof(GTEST_STRINGIFY_(test_suite_name)) > 1,                 \
                "test_suite_name must not be empty");                          \
  static_assert(sizeof(GTEST_STRINGIFY_(test_name)) > 1,                       \
                "test_name must not be empty");                                \
  class GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)                     \
      : public parent_class {                                                  \
   public:                                                                     \
    GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)() = default;            \
    ~GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)() override = default;  \
    GTEST_DISALLOW_COPY_AND_ASSIGN_(GTEST_TEST_CLASS_NAME_(test_suite_name,    \
                                                           test_name));        \
    GTEST_DISALLOW_MOVE_AND_ASSIGN_(GTEST_TEST_CLASS_NAME_(test_suite_name,    \
                                                           test_name));        \
                                                                               \
   private:                                                                    \
    void TestBody() override;                                                  \
    seastar::future<> SeastarBody();                                           \
    static ::testing::TestInfo* const test_info_ GTEST_ATTRIBUTE_UNUSED_;      \
  };                                                                           \
                                                                               \
  ::testing::TestInfo* const GTEST_TEST_CLASS_NAME_(test_suite_name,           \
                                                    test_name)::test_info_ =   \
      ::testing::internal::MakeAndRegisterTestInfo(                            \
          #test_suite_name, #test_name, nullptr, nullptr,                      \
          ::testing::internal::CodeLocation(__FILE__, __LINE__), (parent_id),  \
          ::testing::internal::SuiteApiResolver<                               \
              parent_class>::GetSetUpCaseOrSuite(__FILE__, __LINE__),          \
          ::testing::internal::SuiteApiResolver<                               \
              parent_class>::GetTearDownCaseOrSuite(__FILE__, __LINE__),       \
          new ::testing::internal::TestFactoryImpl<GTEST_TEST_CLASS_NAME_(     \
              test_suite_name, test_name)>);                                   \
  void GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)::TestBody() {        \
    auto fut = seastar::alien::submit_to(                                      \
        *seastar::alien::internal::default_instance,                           \
        0,                                                                     \
        [this] { return this->SeastarBody(); });                               \
    fut.wait();                                                                \
  }                                                                            \
  seastar::future<>                                                            \
  GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)::SeastarBody()

#define RAFTER_TEST_F(test_fixture, test_name)                                 \
  RAFTER_TEST_(test_fixture, test_name, test_fixture,                          \
               ::testing::internal::GetTypeId<test_fixture>())

class rafter_test_base : public ::testing::Test {
 public:
  // dummy args, use actual argc, argv instead
  inline static const int argc = 3;
  inline static const char *argv[] = {"rafter_test_base", "-c2", "-m1G"};

  static void SetUpTestSuite();

  static void TearDownTestSuite();

 protected:
  inline static std::thread _engine_thread;
  inline static std::unique_ptr<seastar::app_template> _app;
  inline static seastar::logger l = seastar::logger("rafter_test");
};