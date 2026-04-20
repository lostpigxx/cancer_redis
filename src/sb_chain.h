#pragma once

#include "bloom_filter.h"
#include <cstddef>

// Flags controlling bloom filter behavior.
// Values are fixed for RDB serialization compatibility.
enum BloomFlags : unsigned {
  kNoRound    = 1,
  kRawBits    = 2,
  kUse64Bit   = 4,
  kFixedSize  = 8,
};

constexpr double kTighteningRatio = 0.5;

// One layer in a scaling bloom filter.
struct FilterLayer {
  BloomLayer bloom;
  size_t itemCount = 0;
};

// Scaling bloom filter: a chain of FilterLayer instances that grows
// automatically when capacity is exceeded.
// Based on "Scalable Bloom Filters" by Almeida, Baquero et al. (2007).
struct ScalingBloomFilter {
  FilterLayer* layers = nullptr;
  size_t totalItems = 0;
  size_t numLayers = 0;
  unsigned flags = 0;
  unsigned expansionFactor = 2;

  static ScalingBloomFilter* New(uint64_t initialCapacity, double errorRate,
                                  unsigned flags, unsigned expansion);
  void Free();

  // Returns: 1 = inserted, 0 = duplicate, -1 = full (fixed-size mode)
  int Put(const void* data, size_t length);
  int Contains(const void* data, size_t length) const;

  uint64_t TotalCapacity() const;
  size_t BytesUsed() const;
};

// Wire-format structures for SCANDUMP/LOADCHUNK interoperability.
// Layout must match Redis's expected format for cross-module compatibility.
#pragma pack(push, 1)
struct WireLayerMeta {
  uint64_t dataSize;
  uint64_t totalBits;
  uint64_t itemCount;
  double fpRate;
  double bitsPerEntry;
  uint32_t hashCount;
  uint64_t capacity;
  uint8_t log2Bits;
};

struct WireFilterHeader {
  uint64_t totalItems;
  uint32_t numLayers;
  uint32_t flags;
  uint32_t expansionFactor;
};
#pragma pack(pop)

size_t ComputeHeaderSize(const ScalingBloomFilter* filter);
size_t SerializeHeader(const ScalingBloomFilter* filter, void* output);
ScalingBloomFilter* DeserializeHeader(const void* data, size_t length);
