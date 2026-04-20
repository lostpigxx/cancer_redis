#pragma once

#include <cstdint>
#include <cstddef>

struct HashPair {
  uint64_t primary;
  uint64_t secondary;
};

HashPair ComputeHash32(const void* data, int length);
HashPair ComputeHash64(const void* data, int length);

// A single bloom filter layer (bit array + hash parameters).
// Implements the standard bloom filter algorithm from Burton Bloom (1970).
struct BloomLayer {
  uint32_t hashCount = 0;
  uint8_t prefer64 = 0;
  uint8_t log2Bits = 0;
  uint64_t capacity = 0;
  double fpRate = 0.0;
  double bitsPerEntry = 0.0;
  uint8_t* bitArray = nullptr;
  uint64_t dataSize = 0;
  uint64_t totalBits = 0;

  int Setup(uint64_t cap, double falsePositiveRate, unsigned flags);
  void Teardown();

  bool Test(const HashPair& hp) const;
  bool Insert(const HashPair& hp);
};
