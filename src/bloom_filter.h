#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <type_traits>

// --- Bloom filter flags (values fixed for RDB wire compatibility) ---
enum class BloomFlags : unsigned {
  None      = 0,
  NoRound   = 1,
  RawBits   = 2,
  Use64Bit  = 4,
  FixedSize = 8,
};

constexpr BloomFlags operator|(BloomFlags a, BloomFlags b) {
  return static_cast<BloomFlags>(
    static_cast<unsigned>(a) | static_cast<unsigned>(b));
}

constexpr BloomFlags operator&(BloomFlags a, BloomFlags b) {
  return static_cast<BloomFlags>(
    static_cast<unsigned>(a) & static_cast<unsigned>(b));
}

constexpr bool HasFlag(BloomFlags set, BloomFlags flag) {
  return static_cast<unsigned>(set & flag) != 0;
}

constexpr unsigned ToUnderlying(BloomFlags f) {
  return static_cast<unsigned>(f);
}

constexpr BloomFlags FromUnderlying(unsigned v) {
  return static_cast<BloomFlags>(v);
}

// --- Hash pair produced by double-hashing ---
struct HashPair {
  uint64_t primary;
  uint64_t secondary;
};

// Hash policy types — used as template arguments to eliminate runtime branching
struct Hash32Policy {
  static HashPair Compute(std::span<const std::byte> data);
};

struct Hash64Policy {
  static HashPair Compute(std::span<const std::byte> data);
};

// Convenience: convert (const char*, size_t) to span
inline std::span<const std::byte> AsBytes(const char* data, size_t len) {
  return {reinterpret_cast<const std::byte*>(data), len};
}

inline std::span<const std::byte> AsBytes(const void* data, size_t len) {
  return {static_cast<const std::byte*>(data), len};
}

// Forward declarations for serialization
class RdbWriter;
class RdbReader;
struct WireLayerMeta;

// --- Single bloom filter layer (RAII) ---
class BloomLayer {
public:
  BloomLayer() = default;
  ~BloomLayer();

  BloomLayer(const BloomLayer&) = delete;
  BloomLayer& operator=(const BloomLayer&) = delete;

  BloomLayer(BloomLayer&& other) noexcept;
  BloomLayer& operator=(BloomLayer&& other) noexcept;

  static std::optional<BloomLayer> Create(uint64_t cap, double falsePositiveRate,
                                           BloomFlags flags);

  // Serialization: each object knows how to persist itself
  void WriteTo(RdbWriter& w) const;
  static std::optional<BloomLayer> ReadFrom(RdbReader& r, BloomFlags filterFlags);

  // Wire format conversion for SCANDUMP/LOADCHUNK
  WireLayerMeta ToWireMeta(size_t itemCount) const;
  static BloomLayer FromWireMeta(const WireLayerMeta& meta, BloomFlags filterFlags);

  bool Test(const HashPair& hp) const;
  bool Insert(const HashPair& hp);

  // Accessors
  uint32_t GetHashCount() const { return hashCount_; }
  uint8_t GetLog2Bits() const { return log2Bits_; }
  uint64_t GetCapacity() const { return capacity_; }
  double GetFpRate() const { return fpRate_; }
  double GetBitsPerEntry() const { return bitsPerEntry_; }
  uint64_t GetTotalBits() const { return totalBits_; }
  uint64_t GetDataSize() const { return dataSize_; }
  bool IsUse64Bit() const { return use64Bit_; }
  const uint8_t* GetBitArray() const { return bitArray_; }
  uint8_t* GetBitArray() { return bitArray_; }

private:
  uint32_t hashCount_ = 0;
  uint8_t log2Bits_ = 0;
  bool use64Bit_ = false;
  uint64_t capacity_ = 0;
  double fpRate_ = 0.0;
  double bitsPerEntry_ = 0.0;
  uint8_t* bitArray_ = nullptr;
  uint64_t dataSize_ = 0;
  uint64_t totalBits_ = 0;
};
