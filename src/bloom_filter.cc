#include "bloom_filter.h"
#include "murmur2.h"
#include "rm_alloc.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <numbers>

// --- Hash policies ---
// Seeds are required for data-level interoperability: they determine which
// bits are set in the persisted bit array. Changing them would cause false
// negatives when querying filters loaded from existing RDB files.

HashPair Hash32Policy::Compute(std::span<const std::byte> data) {
  auto* ptr = reinterpret_cast<const void*>(data.data());
  auto len = static_cast<int>(data.size());
  uint32_t h1 = MurmurHash2(ptr, len, 0x9747b28c);
  return {h1, MurmurHash2(ptr, len, h1)};
}

HashPair Hash64Policy::Compute(std::span<const std::byte> data) {
  auto* ptr = reinterpret_cast<const void*>(data.data());
  auto len = static_cast<int>(data.size());
  uint64_t h1 = MurmurHash64A(ptr, len, 0xc6a4a7935bd1e995ULL);
  return {h1, MurmurHash64A(ptr, len, h1)};
}

// --- BloomLayer lifecycle ---

BloomLayer::~BloomLayer() {
  if (bitArray_) {
    RMFree(bitArray_);
    bitArray_ = nullptr;
  }
}

BloomLayer::BloomLayer(BloomLayer&& other) noexcept
    : hashCount_(other.hashCount_),
      log2Bits_(other.log2Bits_),
      use64Bit_(other.use64Bit_),
      capacity_(other.capacity_),
      fpRate_(other.fpRate_),
      bitsPerEntry_(other.bitsPerEntry_),
      bitArray_(other.bitArray_),
      dataSize_(other.dataSize_),
      totalBits_(other.totalBits_) {
  other.bitArray_ = nullptr;
}

BloomLayer& BloomLayer::operator=(BloomLayer&& other) noexcept {
  if (this != &other) {
    if (bitArray_) RMFree(bitArray_);
    hashCount_ = other.hashCount_;
    log2Bits_ = other.log2Bits_;
    use64Bit_ = other.use64Bit_;
    capacity_ = other.capacity_;
    fpRate_ = other.fpRate_;
    bitsPerEntry_ = other.bitsPerEntry_;
    bitArray_ = other.bitArray_;
    dataSize_ = other.dataSize_;
    totalBits_ = other.totalBits_;
    other.bitArray_ = nullptr;
  }
  return *this;
}

// --- Optimal parameter computation ---
// Formulas from Mitzenmacher & Upfal, "Probability and Computing" (2005):
//   m/n = -log2(p) / ln(2)       (bits per entry)
//   k   = (m/n) * ln(2)           (optimal hash count)

static double OptimalBitsPerEntry(double fpRate) {
  constexpr double kLn2Squared = std::numbers::ln2 * std::numbers::ln2;
  return -std::log(fpRate) / kLn2Squared;
}

static uint32_t OptimalHashCount(double bitsPerEntry) {
  return std::max(1u,
    static_cast<uint32_t>(std::ceil(std::numbers::ln2 * bitsPerEntry)));
}

std::optional<BloomLayer> BloomLayer::Create(uint64_t cap, double falsePositiveRate,
                                              BloomFlags flags) {
  BloomLayer layer;
  layer.capacity_ = cap;
  layer.fpRate_ = falsePositiveRate;
  layer.use64Bit_ = HasFlag(flags, BloomFlags::Use64Bit);

  if (HasFlag(flags, BloomFlags::RawBits)) {
    layer.bitsPerEntry_ = 0;
    layer.totalBits_ = cap;
    layer.hashCount_ = 0;
  } else {
    layer.bitsPerEntry_ = OptimalBitsPerEntry(falsePositiveRate);
    auto rawBits = static_cast<double>(cap) * layer.bitsPerEntry_;
    layer.totalBits_ = static_cast<uint64_t>(std::max(rawBits, 1024.0));
    layer.hashCount_ = OptimalHashCount(layer.bitsPerEntry_);
  }

  if (!HasFlag(flags, BloomFlags::NoRound)) {
    layer.totalBits_ = std::bit_ceil(layer.totalBits_);
    layer.log2Bits_ = static_cast<uint8_t>(std::bit_width(layer.totalBits_) - 1);
  }

  layer.dataSize_ = layer.totalBits_ / 8;
  if (layer.dataSize_ == 0) return std::nullopt;

  layer.bitArray_ = static_cast<uint8_t*>(RMCalloc(layer.dataSize_, 1));
  if (!layer.bitArray_) return std::nullopt;

  return layer;
}

// --- Bit-level operations ---

uint64_t BloomLayer::ComputeModuloMask() const {
  return (1ULL << log2Bits_) - 1;
}

bool BloomLayer::TestBit(uint64_t bitIndex) const {
  auto [byteOff, mask] = ResolveBit(bitIndex);
  return (bitArray_[byteOff] & mask) != 0;
}

void BloomLayer::SetBit(uint64_t bitIndex) {
  auto [byteOff, mask] = ResolveBit(bitIndex);
  bitArray_[byteOff] |= mask;
}

// --- Membership queries ---
// Uses Kirsch-Mitzenmacher enhanced double hashing to derive k probe
// positions from a single HashPair. Reference: Kirsch & Mitzenmacher,
// "Less Hashing, Same Performance" (ESA 2006).

bool BloomLayer::Test(const HashPair& hp) const {
  bool isPow2 = IsPowerOfTwo();
  uint64_t mask = isPow2 ? ComputeModuloMask() : 0;

  for (uint32_t probe = 0; probe < hashCount_; probe++) {
    uint64_t pos = ProbePosition(hp, probe, mask, totalBits_, isPow2);
    if (!TestBit(pos)) return false;
  }
  return true;
}

bool BloomLayer::Insert(const HashPair& hp) {
  bool isPow2 = IsPowerOfTwo();
  uint64_t mask = isPow2 ? ComputeModuloMask() : 0;
  bool anyNew = false;

  for (uint32_t probe = 0; probe < hashCount_; probe++) {
    uint64_t pos = ProbePosition(hp, probe, mask, totalBits_, isPow2);
    if (!TestBit(pos)) {
      SetBit(pos);
      anyNew = true;
    }
  }
  return anyNew;
}
