#include "bloom_filter.h"
#include "murmur2.h"
#include "rm_alloc.h"

#include <cmath>
#include <cstring>

static constexpr unsigned kFlagNoRound = 1;
static constexpr unsigned kFlagRawBits = 2;
static constexpr unsigned kFlag64Bit = 4;

HashPair ComputeHash32(const void* data, int length) {
  HashPair hp;
  hp.primary = MurmurHash2(data, length, 0x9747b28c);
  hp.secondary = MurmurHash2(data, length, static_cast<uint32_t>(hp.primary));
  return hp;
}

HashPair ComputeHash64(const void* data, int length) {
  HashPair hp;
  hp.primary = MurmurHash64A(data, length, 0xc6a4a7935bd1e995ULL);
  hp.secondary = MurmurHash64A(data, length, hp.primary);
  return hp;
}

static double CalcBitsPerEntry(double falsePositiveRate) {
  double ln2 = std::log(2.0);
  return -(std::log(falsePositiveRate) / (ln2 * ln2));
}

static uint32_t CalcOptimalHashCount(double bitsPerEntry) {
  uint32_t k = static_cast<uint32_t>(std::ceil(std::log(2.0) * bitsPerEntry));
  return k > 0 ? k : 1;
}

static uint8_t CeilLog2(uint64_t value) {
  uint8_t power = 0;
  uint64_t v = 1;
  while (v < value) {
    v <<= 1;
    power++;
  }
  return power;
}

int BloomLayer::Setup(uint64_t cap, double falsePositiveRate, unsigned flags) {
  capacity = cap;
  fpRate = falsePositiveRate;
  prefer64 = (flags & kFlag64Bit) ? 1 : 0;

  if (flags & kFlagRawBits) {
    bitsPerEntry = 0;
    totalBits = cap;
    hashCount = 0;
  } else {
    bitsPerEntry = CalcBitsPerEntry(falsePositiveRate);
    double rawBits = static_cast<double>(cap) * bitsPerEntry;
    totalBits = static_cast<uint64_t>(rawBits < 1024.0 ? 1024.0 : rawBits);
    hashCount = CalcOptimalHashCount(bitsPerEntry);
  }

  if (!(flags & kFlagNoRound)) {
    log2Bits = CeilLog2(totalBits);
    totalBits = 1ULL << log2Bits;
  }

  dataSize = totalBits / 8;
  if (dataSize == 0) return -1;

  bitArray = static_cast<uint8_t*>(RMCalloc(dataSize, 1));
  return bitArray ? 0 : -1;
}

void BloomLayer::Teardown() {
  if (bitArray) {
    RMFree(bitArray);
    bitArray = nullptr;
  }
}

// Kirsch-Mitzenmacher optimization: derive k hash positions from two base hashes.
// h_i(x) = primary + i * secondary (mod totalBits)
bool BloomLayer::Test(const HashPair& hp) const {
  uint64_t mask = (log2Bits > 0) ? ((1ULL << log2Bits) - 1) : 0;
  for (uint32_t i = 0; i < hashCount; i++) {
    uint64_t pos = hp.primary + i * hp.secondary;
    pos = (log2Bits > 0) ? (pos & mask) : (pos % totalBits);
    if (!(bitArray[pos >> 3] & (1 << (pos & 7)))) {
      return false;
    }
  }
  return true;
}

bool BloomLayer::Insert(const HashPair& hp) {
  bool allBitsSet = true;
  uint64_t mask = (log2Bits > 0) ? ((1ULL << log2Bits) - 1) : 0;
  for (uint32_t i = 0; i < hashCount; i++) {
    uint64_t pos = hp.primary + i * hp.secondary;
    pos = (log2Bits > 0) ? (pos & mask) : (pos % totalBits);
    uint8_t bit = 1 << (pos & 7);
    if (!(bitArray[pos >> 3] & bit)) {
      allBitsSet = false;
      bitArray[pos >> 3] |= bit;
    }
  }
  return !allBitsSet;
}
