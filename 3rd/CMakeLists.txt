include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)

FetchContent_Declare(
    simdjson
    URL https://github.com/simdjson/simdjson/archive/refs/tags/v1.0.2.zip
)

FetchContent_MakeAvailable(googletest)
# TODO(jyc): remove this for simdjson
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-ambiguous-reversed-operator")
FetchContent_MakeAvailable(simdjson)
