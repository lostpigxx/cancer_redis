cmake_minimum_required(VERSION 3.14)
project(valkey_hash LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Header-only library (hashtable.h + bucket/simd/siphash headers)
add_library(valkey_hash STATIC src/hashtable.cpp)
target_include_directories(valkey_hash PUBLIC src)
target_compile_options(valkey_hash PRIVATE -Wall -Wextra -Wpedantic)

# Test binary
add_executable(hashtable_test src/hashtable_test.cpp)
target_link_libraries(hashtable_test PRIVATE valkey_hash)
target_compile_options(hashtable_test PRIVATE -Wall -Wextra -Wpedantic)

# Example binary
add_executable(example src/example.cpp)
target_link_libraries(example PRIVATE valkey_hash)
target_compile_options(example PRIVATE -Wall -Wextra -Wpedantic)
