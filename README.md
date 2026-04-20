# cancer_redis

A Redis module that implements Bloom Filter data structures.

## Build

Requires CMake 3.14+ and a C++17 compiler.

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

The output is `build/redis_bloom.so`.

## Load Module

```bash
redis-server --loadmodule ./build/redis_bloom.so
```

## Run Tests

Tests use Google Test. Enable with `-DBUILD_TESTS=ON`:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON
cmake --build build -j$(nproc)
cd build && ctest --output-on-failure
```