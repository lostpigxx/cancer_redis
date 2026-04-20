#pragma once

// SipHash-1-2 implementation, ported from Valkey's siphash.c (CC0 licensed).
// This is the reduced-round variant used in Redis/Valkey for hash table hashing.

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace cancer_redis {

namespace detail {

inline uint64_t Rotl64(uint64_t x, int b) {
  return (x << b) | (x >> (64 - b));
}

inline uint64_t LoadLE64(const uint8_t *p) {
#if defined(__x86_64__) || defined(_M_X64) || defined(__aarch64__) || defined(_M_ARM64) || \
    (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
  uint64_t v;
  std::memcpy(&v, p, sizeof(v));
  return v;
#else
  return static_cast<uint64_t>(p[0]) | (static_cast<uint64_t>(p[1]) << 8) |
         (static_cast<uint64_t>(p[2]) << 16) | (static_cast<uint64_t>(p[3]) << 24) |
         (static_cast<uint64_t>(p[4]) << 32) | (static_cast<uint64_t>(p[5]) << 40) |
         (static_cast<uint64_t>(p[6]) << 48) | (static_cast<uint64_t>(p[7]) << 56);
#endif
}

#define SIPROUND           \
  do {                     \
    v0 += v1;              \
    v1 = Rotl64(v1, 13);  \
    v1 ^= v0;             \
    v0 = Rotl64(v0, 32);  \
    v2 += v3;              \
    v3 = Rotl64(v3, 16);  \
    v3 ^= v2;             \
    v0 += v3;              \
    v3 = Rotl64(v3, 21);  \
    v3 ^= v0;             \
    v2 += v1;              \
    v1 = Rotl64(v1, 17);  \
    v1 ^= v2;             \
    v2 = Rotl64(v2, 32);  \
  } while (0)

} // namespace detail

// SipHash-1-2: 1 compression round, 2 finalization rounds.
// Matches the variant used in Valkey/Redis.
inline uint64_t SipHash(const uint8_t *in, size_t inlen, const uint8_t k[16]) {
  using namespace detail;

  uint64_t k0 = LoadLE64(k);
  uint64_t k1 = LoadLE64(k + 8);

  uint64_t v0 = UINT64_C(0x736f6d6570736575) ^ k0;
  uint64_t v1 = UINT64_C(0x646f72616e646f6d) ^ k1;
  uint64_t v2 = UINT64_C(0x6c7967656e657261) ^ k0;
  uint64_t v3 = UINT64_C(0x7465646279746573) ^ k1;

  const uint8_t *end = in + inlen - (inlen % 8);
  uint64_t b = static_cast<uint64_t>(inlen) << 56;

  for (; in != end; in += 8) {
    uint64_t m = LoadLE64(in);
    v3 ^= m;
    SIPROUND;
    v0 ^= m;
  }

  // Tail bytes
  switch (inlen & 7) {
  case 7: b |= static_cast<uint64_t>(in[6]) << 48; [[fallthrough]];
  case 6: b |= static_cast<uint64_t>(in[5]) << 40; [[fallthrough]];
  case 5: b |= static_cast<uint64_t>(in[4]) << 32; [[fallthrough]];
  case 4: b |= static_cast<uint64_t>(in[3]) << 24; [[fallthrough]];
  case 3: b |= static_cast<uint64_t>(in[2]) << 16; [[fallthrough]];
  case 2: b |= static_cast<uint64_t>(in[1]) << 8; [[fallthrough]];
  case 1: b |= static_cast<uint64_t>(in[0]); break;
  case 0: break;
  }

  v3 ^= b;
  SIPROUND;
  v0 ^= b;

  v2 ^= 0xff;
  SIPROUND;
  SIPROUND;

  return v0 ^ v1 ^ v2 ^ v3;
}

// Case-insensitive SipHash-1-2 variant.
// Converts each byte to lowercase before hashing.
inline uint64_t SipHashNoCase(const uint8_t *in, size_t inlen, const uint8_t k[16]) {
  using namespace detail;

  uint64_t k0 = LoadLE64(k);
  uint64_t k1 = LoadLE64(k + 8);

  uint64_t v0 = UINT64_C(0x736f6d6570736575) ^ k0;
  uint64_t v1 = UINT64_C(0x646f72616e646f6d) ^ k1;
  uint64_t v2 = UINT64_C(0x6c7967656e657261) ^ k0;
  uint64_t v3 = UINT64_C(0x7465646279746573) ^ k1;

  uint64_t b = static_cast<uint64_t>(inlen) << 56;

  const uint8_t *end = in + inlen - (inlen % 8);

  for (; in != end; in += 8) {
    uint64_t m = 0;
    for (int i = 0; i < 8; ++i) {
      uint8_t c = in[i];
      if (c >= 'A' && c <= 'Z') c += ('a' - 'A');
      m |= static_cast<uint64_t>(c) << (i * 8);
    }
    v3 ^= m;
    SIPROUND;
    v0 ^= m;
  }

  // Tail bytes
  size_t left = inlen & 7;
  for (size_t i = 0; i < left; ++i) {
    uint8_t c = in[i];
    if (c >= 'A' && c <= 'Z') c += ('a' - 'A');
    b |= static_cast<uint64_t>(c) << (i * 8);
  }

  v3 ^= b;
  SIPROUND;
  v0 ^= b;

  v2 ^= 0xff;
  SIPROUND;
  SIPROUND;

  return v0 ^ v1 ^ v2 ^ v3;
}

#undef SIPROUND

} // namespace cancer_redis
