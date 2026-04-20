#pragma once

#include "bloom_filter.h"
#include <cstddef>

constexpr unsigned kBloomOptNoRound = 1;
constexpr unsigned kBloomOptEntsIsBits = 2;
constexpr unsigned kBloomOptForce64 = 4;
constexpr unsigned kBloomOptNoScaling = 8;

constexpr double kErrorTighteningRatio = 0.5;

struct SBLink {
  BloomFilter inner;
  size_t size = 0;
};

struct SBChain {
  SBLink* filters = nullptr;
  size_t size = 0;
  size_t nfilters = 0;
  unsigned options = 0;
  unsigned growth = 2;

  static SBChain* Create(uint64_t capacity, double errorRate,
                          unsigned options, unsigned growth);
  void Destroy();

  // Returns: 1 = newly added, 0 = already existed, -1 = error
  int Add(const void* buf, size_t len);
  int Check(const void* buf, size_t len) const;

  uint64_t Capacity() const;
  size_t MemUsage() const;
};

#pragma pack(push, 1)
struct DumpedChainLink {
  uint64_t bytes;
  uint64_t bits;
  uint64_t size;
  double error;
  double bpe;
  uint32_t hashes;
  uint64_t entries;
  uint8_t n2;
};

struct DumpedChainHeader {
  uint64_t size;
  uint32_t nfilters;
  uint32_t options;
  uint32_t growth;
};
#pragma pack(pop)

// SCANDUMP/LOADCHUNK helpers
size_t SBChainDumpHeaderSize(const SBChain* chain);
size_t SBChainDumpHeader(const SBChain* chain, void* buf);
SBChain* SBChainLoadHeader(const void* buf, size_t len);
