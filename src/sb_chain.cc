#include "sb_chain.h"
#include "rm_alloc.h"

#include <cstring>

static int AppendLayer(ScalingBloomFilter* filter, uint64_t cap, double rate, unsigned flags) {
  size_t newCount = filter->numLayers + 1;
  auto* expanded = static_cast<FilterLayer*>(
    RMRealloc(filter->layers, newCount * sizeof(FilterLayer)));
  if (!expanded) return -1;

  filter->layers = expanded;
  FilterLayer& layer = filter->layers[filter->numLayers];
  layer.itemCount = 0;
  std::memset(&layer.bloom, 0, sizeof(BloomLayer));

  if (layer.bloom.Setup(cap, rate, flags) != 0) return -1;
  filter->numLayers = newCount;
  return 0;
}

ScalingBloomFilter* ScalingBloomFilter::New(uint64_t initialCapacity, double errorRate,
                                             unsigned flg, unsigned expansion) {
  auto* filter = static_cast<ScalingBloomFilter*>(RMCalloc(1, sizeof(ScalingBloomFilter)));
  if (!filter) return nullptr;

  filter->layers = nullptr;
  filter->totalItems = 0;
  filter->numLayers = 0;
  filter->flags = flg;
  filter->expansionFactor = expansion;

  double initialRate = (flg & kFixedSize) ? errorRate : errorRate * kTighteningRatio;

  if (AppendLayer(filter, initialCapacity, initialRate, flg) != 0) {
    RMFree(filter);
    return nullptr;
  }
  return filter;
}

void ScalingBloomFilter::Free() {
  for (size_t i = 0; i < numLayers; i++) {
    layers[i].bloom.Teardown();
  }
  if (layers) RMFree(layers);
  RMFree(this);
}

int ScalingBloomFilter::Put(const void* data, size_t length) {
  bool use64 = (numLayers > 0 && layers[0].bloom.prefer64);
  HashPair hp = use64
    ? ComputeHash64(data, static_cast<int>(length))
    : ComputeHash32(data, static_cast<int>(length));

  for (size_t i = numLayers; i > 0; i--) {
    if (layers[i - 1].bloom.Test(hp)) return 0;
  }

  FilterLayer* top = &layers[numLayers - 1];

  if (top->itemCount >= top->bloom.capacity) {
    if (flags & kFixedSize) return -1;

    uint64_t nextCap = top->bloom.capacity * expansionFactor;
    double nextRate = top->bloom.fpRate * kTighteningRatio;
    if (nextRate <= 0.0) return -1;

    if (AppendLayer(this, nextCap, nextRate, flags) != 0) return -1;
    top = &layers[numLayers - 1];
  }

  top->bloom.Insert(hp);
  top->itemCount++;
  totalItems++;
  return 1;
}

int ScalingBloomFilter::Contains(const void* data, size_t length) const {
  bool use64 = (numLayers > 0 && layers[0].bloom.prefer64);
  HashPair hp = use64
    ? ComputeHash64(data, static_cast<int>(length))
    : ComputeHash32(data, static_cast<int>(length));

  for (size_t i = numLayers; i > 0; i--) {
    if (layers[i - 1].bloom.Test(hp)) return 1;
  }
  return 0;
}

uint64_t ScalingBloomFilter::TotalCapacity() const {
  uint64_t sum = 0;
  for (size_t i = 0; i < numLayers; i++) {
    sum += layers[i].bloom.capacity;
  }
  return sum;
}

size_t ScalingBloomFilter::BytesUsed() const {
  size_t total = sizeof(ScalingBloomFilter) + numLayers * sizeof(FilterLayer);
  for (size_t i = 0; i < numLayers; i++) {
    total += layers[i].bloom.dataSize;
  }
  return total;
}

// --- Wire format serialization for SCANDUMP/LOADCHUNK ---

size_t ComputeHeaderSize(const ScalingBloomFilter* filter) {
  return sizeof(WireFilterHeader) + filter->numLayers * sizeof(WireLayerMeta);
}

size_t SerializeHeader(const ScalingBloomFilter* filter, void* output) {
  auto* hdr = static_cast<WireFilterHeader*>(output);
  hdr->totalItems = filter->totalItems;
  hdr->numLayers = static_cast<uint32_t>(filter->numLayers);
  hdr->flags = filter->flags;
  hdr->expansionFactor = filter->expansionFactor;

  auto* meta = reinterpret_cast<WireLayerMeta*>(
    static_cast<char*>(output) + sizeof(WireFilterHeader));

  for (size_t i = 0; i < filter->numLayers; i++) {
    const FilterLayer& layer = filter->layers[i];
    meta[i].dataSize = layer.bloom.dataSize;
    meta[i].totalBits = layer.bloom.totalBits;
    meta[i].itemCount = layer.itemCount;
    meta[i].fpRate = layer.bloom.fpRate;
    meta[i].bitsPerEntry = layer.bloom.bitsPerEntry;
    meta[i].hashCount = layer.bloom.hashCount;
    meta[i].capacity = layer.bloom.capacity;
    meta[i].log2Bits = layer.bloom.log2Bits;
  }

  return sizeof(WireFilterHeader) + filter->numLayers * sizeof(WireLayerMeta);
}

ScalingBloomFilter* DeserializeHeader(const void* data, size_t length) {
  if (length < sizeof(WireFilterHeader)) return nullptr;

  const auto* hdr = static_cast<const WireFilterHeader*>(data);
  size_t required = sizeof(WireFilterHeader) + hdr->numLayers * sizeof(WireLayerMeta);
  if (length < required) return nullptr;

  auto* filter = static_cast<ScalingBloomFilter*>(RMCalloc(1, sizeof(ScalingBloomFilter)));
  if (!filter) return nullptr;

  filter->totalItems = hdr->totalItems;
  filter->numLayers = hdr->numLayers;
  filter->flags = hdr->flags;
  filter->expansionFactor = hdr->expansionFactor;

  filter->layers = static_cast<FilterLayer*>(RMCalloc(filter->numLayers, sizeof(FilterLayer)));
  if (!filter->layers) {
    RMFree(filter);
    return nullptr;
  }

  const auto* meta = reinterpret_cast<const WireLayerMeta*>(
    static_cast<const char*>(data) + sizeof(WireFilterHeader));

  for (size_t i = 0; i < filter->numLayers; i++) {
    FilterLayer& layer = filter->layers[i];
    layer.itemCount = meta[i].itemCount;
    layer.bloom.dataSize = meta[i].dataSize;
    layer.bloom.totalBits = meta[i].totalBits;
    layer.bloom.fpRate = meta[i].fpRate;
    layer.bloom.bitsPerEntry = meta[i].bitsPerEntry;
    layer.bloom.hashCount = meta[i].hashCount;
    layer.bloom.capacity = meta[i].capacity;
    layer.bloom.log2Bits = meta[i].log2Bits;
    layer.bloom.prefer64 = (filter->flags & kUse64Bit) ? 1 : 0;

    layer.bloom.bitArray = static_cast<uint8_t*>(RMCalloc(layer.bloom.dataSize, 1));
    if (!layer.bloom.bitArray) {
      for (size_t j = 0; j < i; j++) {
        filter->layers[j].bloom.Teardown();
      }
      RMFree(filter->layers);
      RMFree(filter);
      return nullptr;
    }
  }

  return filter;
}
