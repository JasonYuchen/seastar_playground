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

// TODO(jason): support parallel

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

#define RAFTER_TEST(test_suite_name, test_name)                                \
  RAFTER_TEST_(test_suite_name, test_name, ::testing::Test,                    \
               ::testing::internal::GetTestTypeId())

#define RAFTER_TEST_F(test_fixture, test_name)                                 \
  RAFTER_TEST_(test_fixture, test_name, test_fixture,                          \
               ::testing::internal::GetTypeId<test_fixture>())

#define RAFTER_TEST_P(test_suite_name, test_name) \
  class GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)                     \
      : public test_suite_name {                                               \
   public:                                                                     \
    GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)() {}                    \
    void TestBody() override;                                                  \
    seastar::future<> SeastarBody();                                           \
                                                                               \
   private:                                                                    \
    static int AddToRegistry() {                                               \
      ::testing::UnitTest::GetInstance()                                       \
          ->parameterized_test_registry()                                      \
          .GetTestSuitePatternHolder<test_suite_name>(                         \
              GTEST_STRINGIFY_(test_suite_name),                               \
              ::testing::internal::CodeLocation(__FILE__, __LINE__))           \
          ->AddTestPattern(                                                    \
              GTEST_STRINGIFY_(test_suite_name), GTEST_STRINGIFY_(test_name),  \
              new ::testing::internal::TestMetaFactory<GTEST_TEST_CLASS_NAME_( \
                  test_suite_name, test_name)>(),                              \
              ::testing::internal::CodeLocation(__FILE__, __LINE__));          \
      return 0;                                                                \
    }                                                                          \
    static int gtest_registering_dummy_ GTEST_ATTRIBUTE_UNUSED_;               \
    GTEST_DISALLOW_COPY_AND_ASSIGN_(GTEST_TEST_CLASS_NAME_(test_suite_name,    \
                                                           test_name));        \
  };                                                                           \
  int GTEST_TEST_CLASS_NAME_(test_suite_name,                                  \
                             test_name)::gtest_registering_dummy_ =            \
      GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)::AddToRegistry();     \
  void GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)::TestBody() {        \
    auto fut = seastar::alien::submit_to(                                      \
        *seastar::alien::internal::default_instance,                           \
        0,                                                                     \
        [this] { return this->SeastarBody(); });                               \
    fut.wait();                                                                \
  }                                                                            \
  seastar::future<>                                                            \
  GTEST_TEST_CLASS_NAME_(test_suite_name, test_name)::SeastarBody()

namespace rafter::test {

class base : public ::testing::Environment {
 public:
  base(int argc, char** argv) : _argc(argc), _argv(argv) {}

  void SetUp() override;

  void TearDown() override;

 protected:
  int _argc;
  char** _argv;
  std::thread _engine_thread;
  std::unique_ptr<seastar::app_template> _app;
};

extern seastar::logger l;

}  // namespace rafter::test
