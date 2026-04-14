#pragma once

// Bucket structure and compile-time constants for the cache-line-aware hashtable.
// Each bucket is exactly 64 bytes (one cache line) and holds up to 7 entries (64-bit)
// or 12 entries (32-bit).

#include <cassert>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "hashtable_simd.h"

namespace cancer_redis {

// ============================================================================
// Compile-time constants
// ============================================================================

#if UINTPTR_MAX == UINT64_MAX
inline constexpr int kEntriesPerBucket = 7;
using BucketBitsType = uint8_t;
inline constexpr int kBucketFactor = 5;
inline constexpr int kBucketDivisor = 32;
#elif UINTPTR_MAX == UINT32_MAX
inline constexpr int kEntriesPerBucket = 12;
using BucketBitsType = uint16_t;
inline constexpr int kBucketFactor = 3;
inline constexpr int kBucketDivisor = 32;
#else
#error "Only 64-bit or 32-bit architectures are supported"
#endif

inline constexpr size_t kBucketSize = 64;

// Fill factor constants (percentages)
inline constexpr unsigned kMaxFillPercentSoft = 100;
inline constexpr unsigned kMaxFillPercentHard = 500;
inline constexpr unsigned kMinFillPercentSoft = 13;
inline constexpr unsigned kMinFillPercentHard = 3;

// Rehash batching constants
inline constexpr int kFetchBucketCountOnExpand = 4;
inline constexpr int kFetchEntryBufferSize = kFetchBucketCountOnExpand * kEntriesPerBucket;

// Stats vector length
inline constexpr int kStatsVectLen = 50;

// ============================================================================
// Hash helper
// ============================================================================

inline uint8_t HighBits(uint64_t hash) {
  return static_cast<uint8_t>(hash >> 56);
}

// ============================================================================
// Bucket structure
// ============================================================================

template <typename Entry>
struct alignas(kBucketSize) Bucket {
  BucketBitsType chained : 1;
  BucketBitsType presence : kEntriesPerBucket;
  uint8_t hashes[kEntriesPerBucket];
  Entry *entries[kEntriesPerBucket];

  int NumPositions() const {
    return kEntriesPerBucket - (chained ? 1 : 0);
  }

  bool IsFull() const {
    int n = NumPositions();
    BucketBitsType full_mask = static_cast<BucketBitsType>((1 << n) - 1);
    return (presence & full_mask) == full_mask;
  }

  bool IsPositionFilled(int pos) const {
    return (presence & (1 << pos)) != 0;
  }

  void SetPositionFilled(int pos) {
    presence |= static_cast<BucketBitsType>(1 << pos);
  }

  void ClearPosition(int pos) {
    presence &= static_cast<BucketBitsType>(~(1 << pos));
  }

  Bucket *ChildBucket() const {
    if (!chained) return nullptr;
    return reinterpret_cast<Bucket *>(entries[kEntriesPerBucket - 1]);
  }

  void SetChildBucket(Bucket *child) {
    entries[kEntriesPerBucket - 1] = reinterpret_cast<Entry *>(child);
  }

  static void MoveEntry(Bucket &to, int pos_to, Bucket &from, int pos_from) {
    assert(!to.IsPositionFilled(pos_to));
    assert(from.IsPositionFilled(pos_from));
    to.entries[pos_to] = from.entries[pos_from];
    to.hashes[pos_to] = from.hashes[pos_from];
    to.SetPositionFilled(pos_to);
    from.ClearPosition(pos_from);
  }

  void Reset() {
    chained = 0;
    presence = 0;
  }

  int FindFreePosition() const {
    int n = NumPositions();
    for (int i = 0; i < n; ++i) {
      if (!IsPositionFilled(i)) return i;
    }
    return -1;
  }

  int FilledCount() const {
    BucketBitsType mask = static_cast<BucketBitsType>((1 << kEntriesPerBucket) - 1);
    BucketBitsType p = presence & mask;
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_popcount(p);
#else
    int count = 0;
    while (p) {
      count += (p & 1);
      p >>= 1;
    }
    return count;
#endif
  }

  uint16_t MatchHash(uint8_t h2) const {
    BucketBitsType pmask = static_cast<BucketBitsType>(
        presence & ((1 << kEntriesPerBucket) - 1));
    return simd::MatchHash(hashes, h2, pmask, kEntriesPerBucket);
  }
};

// Critical compile-time verification
static_assert(sizeof(Bucket<void *>) == kBucketSize,
              "Bucket must be exactly one cache line (64 bytes)");

// ============================================================================
// Bucket size calculations
// ============================================================================

inline size_t NumBuckets(int8_t exp) {
  if (exp < 0) return 0;
  return static_cast<size_t>(1) << exp;
}

inline size_t ExpToMask(int8_t exp) {
  if (exp < 0) return 0;
  return NumBuckets(exp) - 1;
}

inline int8_t NextBucketExp(size_t min_capacity) {
  if (min_capacity == 0) return -1;
  size_t min_buckets = (min_capacity * kBucketFactor - 1) / kBucketDivisor + 1;
  if (min_buckets >= SIZE_MAX / 2) {
    return static_cast<int8_t>(CHAR_BIT * sizeof(size_t) - 1);
  }
  if (min_buckets == 1) return 0;
#if defined(__GNUC__) || defined(__clang__)
  return static_cast<int8_t>(CHAR_BIT * sizeof(size_t) -
                             __builtin_clzl(min_buckets - 1));
#else
  int8_t exp = 0;
  size_t v = min_buckets - 1;
  while (v >>= 1) ++exp;
  return exp + 1;
#endif
}

inline size_t ReverseBits(size_t v) {
  size_t r = 0;
  for (size_t i = 0; i < sizeof(size_t) * CHAR_BIT; ++i) {
    r = (r << 1) | (v & 1);
    v >>= 1;
  }
  return r;
}

inline size_t NextCursor(size_t cursor, size_t mask) {
  cursor |= ~mask;
  cursor = ReverseBits(cursor);
  cursor++;
  cursor = ReverseBits(cursor);
  return cursor;
}

} // namespace cancer_redis
