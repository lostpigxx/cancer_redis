#pragma once

#include "bloom_filter.h"

#include <algorithm>
#include <cstddef>
#include <numeric>
#include <optional>
#include <span>

constexpr double kTighteningRatio = 0.5;

// One layer in a scaling bloom filter.
struct FilterLayer {
  BloomLayer bloom;
  size_t itemCount = 0;
};

// Scaling bloom filter with RAII lifetime management.
// Layers grow automatically when capacity is exceeded.
// Based on "Scalable Bloom Filters" by Almeida, Baquero et al. (2007).
class ScalingBloomFilter {
public:
  ScalingBloomFilter(uint64_t initialCapacity, double errorRate,
                      BloomFlags flags, unsigned expansion);
  ~ScalingBloomFilter();

  ScalingBloomFilter(const ScalingBloomFilter&) = delete;
  ScalingBloomFilter& operator=(const ScalingBloomFilter&) = delete;
  ScalingBloomFilter(ScalingBloomFilter&&) noexcept;
  ScalingBloomFilter& operator=(ScalingBloomFilter&&) noexcept;

  bool IsValid() const { return layers_ != nullptr; }

  // Returns: true = inserted, false = duplicate, nullopt = full (fixed-size mode)
  std::optional<bool> Put(std::span<const std::byte> data);
  bool Contains(std::span<const std::byte> data) const;

  uint64_t TotalCapacity() const;
  size_t BytesUsed() const;

  // Layer access for RDB / SCANDUMP
  std::span<FilterLayer> Layers() { return {layers_, numLayers_}; }
  std::span<const FilterLayer> Layers() const { return {layers_, numLayers_}; }
  size_t NumLayers() const { return numLayers_; }
  size_t TotalItems() const { return totalItems_; }
  BloomFlags Flags() const { return flags_; }
  unsigned ExpansionFactor() const { return expansionFactor_; }

  // RDB serialization: the filter writes/reads its own structure
  void WriteTo(RdbWriter& w) const;
  static ScalingBloomFilter* ReadFrom(RdbReader& r, int encver);

  // For SCANDUMP deserialization: construct empty shell, then populate layers
  struct RdbShell {
    size_t totalItems;
    size_t numLayers;
    BloomFlags flags;
    unsigned expansionFactor;
  };
  static ScalingBloomFilter* FromRdbShell(RdbShell shell);
  void SetLayer(size_t index, FilterLayer&& layer);

private:
  HashPair ComputeHash(std::span<const std::byte> data) const;
  bool AppendLayer(uint64_t cap, double rate);

  FilterLayer* layers_ = nullptr;
  size_t totalItems_ = 0;
  size_t numLayers_ = 0;
  size_t layerCapacity_ = 0;
  BloomFlags flags_ = BloomFlags::None;
  unsigned expansionFactor_ = 2;
};

// Wire-format structures for SCANDUMP/LOADCHUNK interoperability.
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

size_t ComputeHeaderSize(const ScalingBloomFilter& filter);
size_t SerializeHeader(const ScalingBloomFilter& filter, void* output);
ScalingBloomFilter* DeserializeHeader(const void* data, size_t length);
