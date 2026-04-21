#include "bloom_filter.h"
#include "murmur2.h"
#include "rm_alloc.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>

// --- Hash policies ---

HashPair Hash32Policy::Compute(std::span<const std::byte> data) {
  auto* ptr = reinterpret_cast<const void*>(data.data());
  auto len = static_cast<int>(data.size());
  uint32_t h1 = MurmurHash2(ptr, len, 0x9747b28c);
  uint32_t h2 = MurmurHash2(ptr, len, h1);
  return {h1, h2};
}

HashPair Hash64Policy::Compute(std::span<const std::byte> data) {
  auto* ptr = reinterpret_cast<const void*>(data.data());
  auto len = static_cast<int>(data.size());
  uint64_t h1 = MurmurHash64A(ptr, len, 0xc6a4a7935bd1e995ULL);
  uint64_t h2 = MurmurHash64A(ptr, len, h1);
  return {h1, h2};
}

// --- BloomLayer ---

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
    double ln2 = std::log(2.0);
    layer.bitsPerEntry_ = -(std::log(falsePositiveRate) / (ln2 * ln2));
    double rawBits = static_cast<double>(cap) * layer.bitsPerEntry_;
    layer.totalBits_ = static_cast<uint64_t>(std::max(rawBits, 1024.0));
    layer.hashCount_ = std::max(1u,
      static_cast<uint32_t>(std::ceil(ln2 * layer.bitsPerEntry_)));
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

BloomLayer BloomLayer::FromRdb(RdbParams p) {
  BloomLayer layer;
  layer.hashCount_ = p.hashCount;
  layer.log2Bits_ = p.log2Bits;
  layer.use64Bit_ = p.use64Bit;
  layer.capacity_ = p.capacity;
  layer.fpRate_ = p.fpRate;
  layer.bitsPerEntry_ = p.bitsPerEntry;
  layer.totalBits_ = p.totalBits;
  layer.dataSize_ = p.dataSize;
  layer.bitArray_ = p.bitArray;
  return layer;
}

bool BloomLayer::Test(const HashPair& hp) const {
  if (log2Bits_ > 0) {
    uint64_t mask = (1ULL << log2Bits_) - 1;
    for (uint32_t i = 0; i < hashCount_; i++) {
      uint64_t pos = (hp.primary + i * hp.secondary) & mask;
      if (!(bitArray_[pos >> 3] & (1 << (pos & 7)))) return false;
    }
  } else {
    for (uint32_t i = 0; i < hashCount_; i++) {
      uint64_t pos = (hp.primary + i * hp.secondary) % totalBits_;
      if (!(bitArray_[pos >> 3] & (1 << (pos & 7)))) return false;
    }
  }
  return true;
}

bool BloomLayer::Insert(const HashPair& hp) {
  bool wasNew = false;
  if (log2Bits_ > 0) {
    uint64_t mask = (1ULL << log2Bits_) - 1;
    for (uint32_t i = 0; i < hashCount_; i++) {
      uint64_t pos = (hp.primary + i * hp.secondary) & mask;
      uint8_t bit = 1 << (pos & 7);
      if (!(bitArray_[pos >> 3] & bit)) {
        wasNew = true;
        bitArray_[pos >> 3] |= bit;
      }
    }
  } else {
    for (uint32_t i = 0; i < hashCount_; i++) {
      uint64_t pos = (hp.primary + i * hp.secondary) % totalBits_;
      uint8_t bit = 1 << (pos & 7);
      if (!(bitArray_[pos >> 3] & bit)) {
        wasNew = true;
        bitArray_[pos >> 3] |= bit;
      }
    }
  }
  return wasNew;
}
