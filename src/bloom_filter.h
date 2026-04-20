#pragma once

#include <cstdint>

struct BloomHashVal {
  uint64_t a;
  uint64_t b;
};

BloomHashVal CalcHash(const void* buf, int len);
BloomHashVal CalcHash64(const void* buf, int len);

struct BloomFilter {
  uint32_t hashes = 0;
  uint8_t force64 = 0;
  uint8_t n2 = 0;
  uint64_t entries = 0;
  double error = 0.0;
  double bpe = 0.0;
  uint8_t* bf = nullptr;
  uint64_t bytes = 0;
  uint64_t bits = 0;

  int Init(uint64_t entries, double error, unsigned options);
  void Destroy();

  bool Check(const BloomHashVal& hv) const;
  bool Add(const BloomHashVal& hv);
};
