#pragma once

// SIMD abstraction layer for hashtable bucket lookup.
// Provides compile-time dispatched MatchHash() for SSE2, NEON, and scalar fallback.

#include <cstdint>

// Compile-time SIMD detection
#if defined(__x86_64__) || defined(_M_X64)
#define CANCER_REDIS_HAS_SSE2 1
#include <immintrin.h>
#elif defined(__aarch64__) || defined(_M_ARM64)
#define CANCER_REDIS_HAS_NEON 1
#include <arm_neon.h>
#endif

namespace cancer_redis {
namespace simd {

// MatchHash: compare all hash fingerprints in a bucket against target h2.
// Returns a bitmask where bit i is set if hashes[i] == h2 AND presence bit i is set.
// Only the lowest `count` bits are meaningful.

#if defined(CANCER_REDIS_HAS_SSE2)

inline uint16_t MatchHash(const uint8_t *hashes, uint8_t h2, uint8_t presence_mask, int count) {
  (void)count;
  // Load 16 bytes from hashes array. We may read past the 7 hash bytes,
  // but we stay within the 64-byte aligned bucket struct, so this is safe.
  __m128i hash_vec = _mm_loadu_si128(reinterpret_cast<const __m128i *>(hashes));
  __m128i h2_vec = _mm_set1_epi8(static_cast<char>(h2));
  __m128i cmp = _mm_cmpeq_epi8(hash_vec, h2_vec);
  uint16_t mask = static_cast<uint16_t>(_mm_movemask_epi8(cmp));
  return mask & presence_mask;
}

#elif defined(CANCER_REDIS_HAS_NEON)

inline uint16_t MatchHash(const uint8_t *hashes, uint8_t h2, uint8_t presence_mask, int count) {
  (void)count;
  // Load 8 bytes (covers up to 7 entries on 64-bit, 8 on some configs)
  uint8x8_t hash_vec = vld1_u8(hashes);
  uint8x8_t h2_vec = vdup_n_u8(h2);
  uint8x8_t eq = vceq_u8(hash_vec, h2_vec);

  // Extract per-byte match results into a bitmask.
  uint64_t matches = vget_lane_u64(vreinterpret_u64_u8(eq), 0);

  // Extract the high bit of each byte to form a bitmask
  uint16_t result = 0;
  for (int i = 0; i < 8; ++i) {
    if ((matches >> (i * 8)) & 0x80) {
      result |= (1 << i);
    }
  }

  return result & presence_mask;
}

#else // Scalar fallback

inline uint16_t MatchHash(const uint8_t *hashes, uint8_t h2, uint8_t presence_mask, int count) {
  uint16_t result = 0;
  for (int i = 0; i < count; ++i) {
    if ((presence_mask & (1 << i)) && hashes[i] == h2) {
      result |= (1 << i);
    }
  }
  return result;
}

#endif

// Prefetch hint wrapper
template <int Locality = 3> // 0=NTA, 1=L3, 2=L2, 3=L1 (highest temporal locality)
inline void Prefetch(const void *addr) {
#if defined(__GNUC__) || defined(__clang__)
  __builtin_prefetch(addr, 0, Locality);
#elif defined(_MSC_VER) && defined(CANCER_REDIS_HAS_SSE2)
  _mm_prefetch(static_cast<const char *>(addr), Locality);
#else
  (void)addr;
#endif
}

// Prefetch for write
template <int Locality = 3>
inline void PrefetchW(const void *addr) {
#if defined(__GNUC__) || defined(__clang__)
  __builtin_prefetch(addr, 1, Locality);
#else
  (void)addr;
#endif
}

} // namespace simd
} // namespace cancer_redis
