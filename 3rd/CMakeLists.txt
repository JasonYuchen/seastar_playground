include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/release-1.11.0.zip
)

FetchContent_Declare(
    simdjson
    URL https://github.com/simdjson/simdjson/archive/refs/tags/v2.2.2.zip
)

FetchContent_MakeAvailable(googletest)
FetchContent_MakeAvailable(simdjson)
