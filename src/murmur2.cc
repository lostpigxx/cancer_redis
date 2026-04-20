// MurmurHash2 by Austin Appleby — public domain.
// Reference: https://github.com/aappleby/smhasher

#include "murmur2.h"

uint32_t MurmurHash2(const void* key, int len, uint32_t seed) {
  const uint32_t m = 0x5bd1e995;
  const int r = 24;

  uint32_t h = seed ^ static_cast<uint32_t>(len);
  const auto* data = static_cast<const unsigned char*>(key);

  while (len >= 4) {
    uint32_t k;
    k  = static_cast<uint32_t>(data[0]);
    k |= static_cast<uint32_t>(data[1]) << 8;
    k |= static_cast<uint32_t>(data[2]) << 16;
    k |= static_cast<uint32_t>(data[3]) << 24;

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  switch (len) {
    case 3: h ^= static_cast<uint32_t>(data[2]) << 16; [[fallthrough]];
    case 2: h ^= static_cast<uint32_t>(data[1]) << 8; [[fallthrough]];
    case 1: h ^= static_cast<uint32_t>(data[0]); h *= m;
  }

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;
  return h;
}

uint64_t MurmurHash64A(const void* key, int len, uint64_t seed) {
  const uint64_t m = 0xc6a4a7935bd1e995ULL;
  const int r = 47;

  uint64_t h = seed ^ (static_cast<uint64_t>(len) * m);
  const auto* data = static_cast<const unsigned char*>(key);

  while (len >= 8) {
    uint64_t k;
    k  = static_cast<uint64_t>(data[0]);
    k |= static_cast<uint64_t>(data[1]) << 8;
    k |= static_cast<uint64_t>(data[2]) << 16;
    k |= static_cast<uint64_t>(data[3]) << 24;
    k |= static_cast<uint64_t>(data[4]) << 32;
    k |= static_cast<uint64_t>(data[5]) << 40;
    k |= static_cast<uint64_t>(data[6]) << 48;
    k |= static_cast<uint64_t>(data[7]) << 56;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;

    data += 8;
    len -= 8;
  }

  switch (len) {
    case 7: h ^= static_cast<uint64_t>(data[6]) << 48; [[fallthrough]];
    case 6: h ^= static_cast<uint64_t>(data[5]) << 40; [[fallthrough]];
    case 5: h ^= static_cast<uint64_t>(data[4]) << 32; [[fallthrough]];
    case 4: h ^= static_cast<uint64_t>(data[3]) << 24; [[fallthrough]];
    case 3: h ^= static_cast<uint64_t>(data[2]) << 16; [[fallthrough]];
    case 2: h ^= static_cast<uint64_t>(data[1]) << 8; [[fallthrough]];
    case 1: h ^= static_cast<uint64_t>(data[0]); h *= m;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}
