#include "sb_chain.h"
#include "rm_alloc.h"

#include <cstring>

static int AddLink(SBChain* chain, uint64_t capacity, double error, unsigned options) {
  auto* newFilters = static_cast<SBLink*>(
    RMRealloc(chain->filters, (chain->nfilters + 1) * sizeof(SBLink)));
  if (!newFilters) return -1;

  chain->filters = newFilters;
  SBLink& link = chain->filters[chain->nfilters];
  link.size = 0;
  std::memset(&link.inner, 0, sizeof(BloomFilter));

  if (link.inner.Init(capacity, error, options) != 0) {
    return -1;
  }
  chain->nfilters++;
  return 0;
}

SBChain* SBChain::Create(uint64_t capacity, double errorRate,
                          unsigned options, unsigned growth) {
  auto* chain = static_cast<SBChain*>(RMCalloc(1, sizeof(SBChain)));
  if (!chain) return nullptr;

  chain->filters = nullptr;
  chain->size = 0;
  chain->nfilters = 0;
  chain->options = options;
  chain->growth = growth;

  double firstError = (options & kBloomOptNoScaling)
    ? errorRate
    : errorRate * kErrorTighteningRatio;

  if (AddLink(chain, capacity, firstError, options) != 0) {
    RMFree(chain);
    return nullptr;
  }
  return chain;
}

void SBChain::Destroy() {
  for (size_t i = 0; i < this->nfilters; i++) {
    this->filters[i].inner.Destroy();
  }
  if (this->filters) {
    RMFree(this->filters);
  }
  RMFree(this);
}

int SBChain::Add(const void* buf, size_t len) {
  BloomHashVal hv;
  if (this->nfilters > 0 && this->filters[0].inner.force64) {
    hv = CalcHash64(buf, static_cast<int>(len));
  } else {
    hv = CalcHash(buf, static_cast<int>(len));
  }

  // Check all existing layers (newest first) for duplicates
  for (size_t ii = this->nfilters; ii > 0; ii--) {
    if (this->filters[ii - 1].inner.Check(hv)) {
      return 0;
    }
  }

  SBLink* cur = &this->filters[this->nfilters - 1];

  if (cur->size >= cur->inner.entries) {
    if (this->options & kBloomOptNoScaling) {
      return -1;
    }
    uint64_t newCapacity = cur->inner.entries * this->growth;
    double newError = cur->inner.error * kErrorTighteningRatio;
    if (newError <= 0) {
      return -1;
    }
    if (AddLink(this, newCapacity, newError, this->options) != 0) {
      return -1;
    }
    cur = &this->filters[this->nfilters - 1];
  }

  cur->inner.Add(hv);
  cur->size++;
  this->size++;
  return 1;
}

int SBChain::Check(const void* buf, size_t len) const {
  BloomHashVal hv;
  if (this->nfilters > 0 && this->filters[0].inner.force64) {
    hv = CalcHash64(buf, static_cast<int>(len));
  } else {
    hv = CalcHash(buf, static_cast<int>(len));
  }

  for (size_t ii = this->nfilters; ii > 0; ii--) {
    if (this->filters[ii - 1].inner.Check(hv)) {
      return 1;
    }
  }
  return 0;
}

uint64_t SBChain::Capacity() const {
  uint64_t cap = 0;
  for (size_t i = 0; i < this->nfilters; i++) {
    cap += this->filters[i].inner.entries;
  }
  return cap;
}

size_t SBChain::MemUsage() const {
  size_t mem = sizeof(SBChain);
  mem += this->nfilters * sizeof(SBLink);
  for (size_t i = 0; i < this->nfilters; i++) {
    mem += this->filters[i].inner.bytes;
  }
  return mem;
}

// --- SCANDUMP / LOADCHUNK helpers ---

size_t SBChainDumpHeaderSize(const SBChain* chain) {
  return sizeof(DumpedChainHeader) + chain->nfilters * sizeof(DumpedChainLink);
}

size_t SBChainDumpHeader(const SBChain* chain, void* buf) {
  auto* hdr = static_cast<DumpedChainHeader*>(buf);
  hdr->size = chain->size;
  hdr->nfilters = static_cast<uint32_t>(chain->nfilters);
  hdr->options = chain->options;
  hdr->growth = chain->growth;

  auto* links = reinterpret_cast<DumpedChainLink*>(
    static_cast<char*>(buf) + sizeof(DumpedChainHeader));

  for (size_t i = 0; i < chain->nfilters; i++) {
    const SBLink& sl = chain->filters[i];
    links[i].bytes = sl.inner.bytes;
    links[i].bits = sl.inner.bits;
    links[i].size = sl.size;
    links[i].error = sl.inner.error;
    links[i].bpe = sl.inner.bpe;
    links[i].hashes = sl.inner.hashes;
    links[i].entries = sl.inner.entries;
    links[i].n2 = sl.inner.n2;
  }

  return sizeof(DumpedChainHeader) + chain->nfilters * sizeof(DumpedChainLink);
}

SBChain* SBChainLoadHeader(const void* buf, size_t len) {
  if (len < sizeof(DumpedChainHeader)) return nullptr;

  const auto* hdr = static_cast<const DumpedChainHeader*>(buf);
  size_t expected = sizeof(DumpedChainHeader) + hdr->nfilters * sizeof(DumpedChainLink);
  if (len < expected) return nullptr;

  auto* chain = static_cast<SBChain*>(RMCalloc(1, sizeof(SBChain)));
  if (!chain) return nullptr;

  chain->size = hdr->size;
  chain->nfilters = hdr->nfilters;
  chain->options = hdr->options;
  chain->growth = hdr->growth;

  chain->filters = static_cast<SBLink*>(RMCalloc(chain->nfilters, sizeof(SBLink)));
  if (!chain->filters) {
    RMFree(chain);
    return nullptr;
  }

  const auto* links = reinterpret_cast<const DumpedChainLink*>(
    static_cast<const char*>(buf) + sizeof(DumpedChainHeader));

  for (size_t i = 0; i < chain->nfilters; i++) {
    SBLink& sl = chain->filters[i];
    sl.size = links[i].size;
    sl.inner.bytes = links[i].bytes;
    sl.inner.bits = links[i].bits;
    sl.inner.error = links[i].error;
    sl.inner.bpe = links[i].bpe;
    sl.inner.hashes = links[i].hashes;
    sl.inner.entries = links[i].entries;
    sl.inner.n2 = links[i].n2;
    sl.inner.force64 = (chain->options & kBloomOptForce64) ? 1 : 0;

    sl.inner.bf = static_cast<uint8_t*>(RMCalloc(sl.inner.bytes, 1));
    if (!sl.inner.bf) {
      for (size_t j = 0; j < i; j++) {
        chain->filters[j].inner.Destroy();
      }
      RMFree(chain->filters);
      RMFree(chain);
      return nullptr;
    }
  }

  return chain;
}
