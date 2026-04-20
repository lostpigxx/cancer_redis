cmake_minimum_required(VERSION 3.14)
project(redis_bloom LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Redis module shared library
add_library(redis_bloom SHARED
  src/redis_bloom_module.cc
  src/bloom_filter.cc
  src/sb_chain.cc
  src/bloom_commands.cc
  src/bloom_rdb.cc
  src/bloom_config.cc
  src/murmur2.cc
)

target_include_directories(redis_bloom PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(redis_bloom PRIVATE -Wall -Wextra -Wpedantic -fno-exceptions -Wno-c++20-designator)

set_target_properties(redis_bloom PROPERTIES
  PREFIX ""
  SUFFIX ".so"
)

# GTest-based unit tests
option(BUILD_TESTS "Build unit tests" OFF)
if(BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../valkey_hash/build/_deps/googletest-src
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  enable_testing()

  add_executable(bloom_filter_test
    test/bloom_filter_test.cc
    src/bloom_filter.cc
    src/murmur2.cc
  )
  target_include_directories(bloom_filter_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  target_compile_definitions(bloom_filter_test PRIVATE REDIS_BLOOM_TESTING)
  target_link_libraries(bloom_filter_test PRIVATE GTest::gtest_main)
  target_compile_options(bloom_filter_test PRIVATE -Wall -Wextra -Wpedantic)

  add_executable(sb_chain_test
    test/sb_chain_test.cc
    src/sb_chain.cc
    src/bloom_filter.cc
    src/murmur2.cc
  )
  target_include_directories(sb_chain_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  target_compile_definitions(sb_chain_test PRIVATE REDIS_BLOOM_TESTING)
  target_link_libraries(sb_chain_test PRIVATE GTest::gtest_main)
  target_compile_options(sb_chain_test PRIVATE -Wall -Wextra -Wpedantic)

  include(GoogleTest)
  gtest_discover_tests(bloom_filter_test)
  gtest_discover_tests(sb_chain_test)
endif()
