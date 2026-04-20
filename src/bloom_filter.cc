#include "bloom_filter.h"
#include "murmur2.h"
#include "rm_alloc.h"

#include <cmath>
#include <cstring>

static constexpr unsigned kOptNoRound = 1;
static constexpr unsigned kOptEntsIsBits = 2;
static constexpr unsigned kOptForce64 = 4;

BloomHashVal CalcHash(const void* buf, int len) {
  BloomHashVal hv;
  hv.a = MurmurHash2(buf, len, 0x9747b28c);
  hv.b = MurmurHash2(buf, len, static_cast<uint32_t>(hv.a));
  return hv;
}

BloomHashVal CalcHash64(const void* buf, int len) {
  BloomHashVal hv;
  hv.a = MurmurHash64A(buf, len, 0xc6a4a7935bd1e995ULL);
  hv.b = MurmurHash64A(buf, len, hv.a);
  return hv;
}

int BloomFilter::Init(uint64_t ents, double err, unsigned options) {
  this->entries = ents;
  this->error = err;

  if (options & kOptEntsIsBits) {
    this->bpe = 0;
  } else {
    this->bpe = -(std::log(err) / (std::log(2.0) * std::log(2.0)));
  }

  if (options & kOptEntsIsBits) {
    this->bits = ents;
  } else {
    double bn = static_cast<double>(ents) * this->bpe;
    if (bn < 1024.0) bn = 1024.0;
    this->bits = static_cast<uint64_t>(bn);
  }

  if (!(options & kOptNoRound)) {
    // Round up bits to next power of 2
    this->n2 = 0;
    uint64_t tmp = this->bits;
    while (tmp > 1) {
      tmp >>= 1;
      this->n2++;
    }
    if (this->bits != (1ULL << this->n2)) {
      this->n2++;
    }
    this->bits = 1ULL << this->n2;
  }

  this->bytes = this->bits / 8;
  if (this->bytes == 0) {
    return -1;
  }

  if (options & kOptEntsIsBits) {
    this->hashes = 0;
  } else {
    this->hashes = static_cast<uint32_t>(std::ceil(0.693147180559945 * this->bpe));
    if (this->hashes == 0) this->hashes = 1;
  }

  this->force64 = (options & kOptForce64) ? 1 : 0;

  this->bf = static_cast<uint8_t*>(RMCalloc(this->bytes, 1));
  if (!this->bf) return -1;

  return 0;
}

void BloomFilter::Destroy() {
  if (this->bf) {
    RMFree(this->bf);
    this->bf = nullptr;
  }
}

bool BloomFilter::Check(const BloomHashVal& hv) const {
  if (this->n2 > 0) {
    uint64_t mod = (1ULL << this->n2) - 1;
    for (uint32_t i = 0; i < this->hashes; i++) {
      uint64_t x = (hv.a + i * hv.b) & mod;
      uint64_t byte_idx = x >> 3;
      uint8_t bit_mask = 1 << (x & 7);
      if (!(this->bf[byte_idx] & bit_mask)) {
        return false;
      }
    }
  } else {
    for (uint32_t i = 0; i < this->hashes; i++) {
      uint64_t x = (hv.a + i * hv.b) % this->bits;
      uint64_t byte_idx = x >> 3;
      uint8_t bit_mask = 1 << (x & 7);
      if (!(this->bf[byte_idx] & bit_mask)) {
        return false;
      }
    }
  }
  return true;
}

bool BloomFilter::Add(const BloomHashVal& hv) {
  bool existing = true;
  if (this->n2 > 0) {
    uint64_t mod = (1ULL << this->n2) - 1;
    for (uint32_t i = 0; i < this->hashes; i++) {
      uint64_t x = (hv.a + i * hv.b) & mod;
      uint64_t byte_idx = x >> 3;
      uint8_t bit_mask = 1 << (x & 7);
      if (!(this->bf[byte_idx] & bit_mask)) {
        existing = false;
        this->bf[byte_idx] |= bit_mask;
      }
    }
  } else {
    for (uint32_t i = 0; i < this->hashes; i++) {
      uint64_t x = (hv.a + i * hv.b) % this->bits;
      uint64_t byte_idx = x >> 3;
      uint8_t bit_mask = 1 << (x & 7);
      if (!(this->bf[byte_idx] & bit_mask)) {
        existing = false;
        this->bf[byte_idx] |= bit_mask;
      }
    }
  }
  return !existing;
}
