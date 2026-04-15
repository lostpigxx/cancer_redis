cmake_minimum_required(VERSION 3.14)
project(valkey_hash LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Header-only library (hashtable.h + bucket/simd/siphash headers)
add_library(valkey_hash STATIC src/hashtable.cpp)
target_include_directories(valkey_hash PUBLIC src)
target_compile_options(valkey_hash PRIVATE -Wall -Wextra -Wpedantic)

# Test binary (standalone, no dependencies)
add_executable(hashtable_test src/hashtable_test.cpp)
target_link_libraries(hashtable_test PRIVATE valkey_hash)
target_compile_options(hashtable_test PRIVATE -Wall -Wextra -Wpedantic)

# Example binary
add_executable(example src/example.cpp)
target_link_libraries(example PRIVATE valkey_hash)
target_compile_options(example PRIVATE -Wall -Wextra -Wpedantic)

# Benchmark binary
add_executable(benchmark src/benchmark.cpp)
target_link_libraries(benchmark PRIVATE valkey_hash)
target_compile_options(benchmark PRIVATE -Wall -Wextra -Wpedantic)

# GTest-based unit tests
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(hashtable_gtest src/hashtable_gtest.cpp)
target_link_libraries(hashtable_gtest PRIVATE valkey_hash GTest::gtest_main)
target_compile_options(hashtable_gtest PRIVATE -Wall -Wextra -Wpedantic)

include(GoogleTest)
gtest_discover_tests(hashtable_gtest)
