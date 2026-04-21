#include "sb_chain.h"
#include "rm_alloc.h"

#include <algorithm>
#include <cstring>
#include <ranges>

ScalingBloomFilter::ScalingBloomFilter(uint64_t initialCapacity, double errorRate,
                                        BloomFlags flg, unsigned expansion)
    : flags_(flg), expansionFactor_(expansion) {
  double firstRate = HasFlag(flg, BloomFlags::FixedSize)
    ? errorRate
    : errorRate * kTighteningRatio;

  if (!AppendLayer(initialCapacity, firstRate)) {
    layers_ = nullptr;
    numLayers_ = 0;
  }
}

ScalingBloomFilter::~ScalingBloomFilter() {
  for (size_t i = 0; i < numLayers_; i++) {
    layers_[i].bloom.~BloomLayer();
  }
  if (layers_) RMFree(layers_);
}

ScalingBloomFilter::ScalingBloomFilter(ScalingBloomFilter&& other) noexcept
    : layers_(other.layers_),
      totalItems_(other.totalItems_),
      numLayers_(other.numLayers_),
      layerCapacity_(other.layerCapacity_),
      flags_(other.flags_),
      expansionFactor_(other.expansionFactor_) {
  other.layers_ = nullptr;
  other.numLayers_ = 0;
  other.layerCapacity_ = 0;
}

ScalingBloomFilter& ScalingBloomFilter::operator=(ScalingBloomFilter&& other) noexcept {
  if (this != &other) {
    this->~ScalingBloomFilter();
    layers_ = other.layers_;
    totalItems_ = other.totalItems_;
    numLayers_ = other.numLayers_;
    layerCapacity_ = other.layerCapacity_;
    flags_ = other.flags_;
    expansionFactor_ = other.expansionFactor_;
    other.layers_ = nullptr;
    other.numLayers_ = 0;
    other.layerCapacity_ = 0;
  }
  return *this;
}

bool ScalingBloomFilter::AppendLayer(uint64_t cap, double rate) {
  if (numLayers_ >= layerCapacity_) {
    size_t newCap = std::max(layerCapacity_ * 2, size_t{4});
    auto* expanded = static_cast<FilterLayer*>(RMRealloc(layers_, newCap * sizeof(FilterLayer)));
    if (!expanded) return false;
    layers_ = expanded;
    layerCapacity_ = newCap;
  }

  auto maybeLayer = BloomLayer::Create(cap, rate, flags_);
  if (!maybeLayer) return false;

  auto* slot = &layers_[numLayers_];
  new (slot) FilterLayer{std::move(*maybeLayer), 0};
  numLayers_++;
  return true;
}

HashPair ScalingBloomFilter::ComputeHash(std::span<const std::byte> data) const {
  return HasFlag(flags_, BloomFlags::Use64Bit)
    ? Hash64Policy::Compute(data)
    : Hash32Policy::Compute(data);
}

std::optional<bool> ScalingBloomFilter::Put(std::span<const std::byte> data) {
  auto hp = ComputeHash(data);

  // Check all existing layers (newest first) for duplicates
  auto layerSpan = Layers();
  for (auto& layer : std::views::reverse(layerSpan)) {
    if (layer.bloom.Test(hp)) return false;
  }

  auto& top = layers_[numLayers_ - 1];
  if (top.itemCount >= top.bloom.GetCapacity()) {
    if (HasFlag(flags_, BloomFlags::FixedSize)) return std::nullopt;

    uint64_t nextCap = top.bloom.GetCapacity() * expansionFactor_;
    double nextRate = top.bloom.GetFpRate() * kTighteningRatio;
    if (nextRate <= 0.0) return std::nullopt;
    if (!AppendLayer(nextCap, nextRate)) return std::nullopt;
  }

  auto& current = layers_[numLayers_ - 1];
  current.bloom.Insert(hp);
  current.itemCount++;
  totalItems_++;
  return true;
}

bool ScalingBloomFilter::Contains(std::span<const std::byte> data) const {
  auto hp = ComputeHash(data);
  auto layerSpan = Layers();
  return std::ranges::any_of(layerSpan, [&](const FilterLayer& layer) {
    return layer.bloom.Test(hp);
  });
}

uint64_t ScalingBloomFilter::TotalCapacity() const {
  auto layerSpan = Layers();
  return std::transform_reduce(layerSpan.begin(), layerSpan.end(),
    uint64_t{0}, std::plus<>{},
    [](const FilterLayer& l) { return l.bloom.GetCapacity(); });
}

size_t ScalingBloomFilter::BytesUsed() const {
  size_t base = sizeof(ScalingBloomFilter) + numLayers_ * sizeof(FilterLayer);
  auto layerSpan = Layers();
  return std::transform_reduce(layerSpan.begin(), layerSpan.end(),
    base, std::plus<>{},
    [](const FilterLayer& l) { return static_cast<size_t>(l.bloom.GetDataSize()); });
}

// --- RDB deserialization helpers ---

ScalingBloomFilter* ScalingBloomFilter::FromRdbShell(RdbShell shell) {
  auto* filter = static_cast<ScalingBloomFilter*>(RMAlloc(sizeof(ScalingBloomFilter)));
  if (!filter) return nullptr;

  new (filter) ScalingBloomFilter(0, 0.01, BloomFlags::None, 2);
  // Override fields from shell
  if (filter->layers_) {
    filter->layers_[0].bloom.~BloomLayer();
    RMFree(filter->layers_);
  }
  filter->layers_ = static_cast<FilterLayer*>(RMCalloc(shell.numLayers, sizeof(FilterLayer)));
  if (!filter->layers_) {
    filter->~ScalingBloomFilter();
    RMFree(filter);
    return nullptr;
  }
  filter->totalItems_ = shell.totalItems;
  filter->numLayers_ = shell.numLayers;
  filter->layerCapacity_ = shell.numLayers;
  filter->flags_ = shell.flags;
  filter->expansionFactor_ = shell.expansionFactor;
  return filter;
}

void ScalingBloomFilter::SetLayer(size_t index, FilterLayer&& layer) {
  if (index < numLayers_) {
    layers_[index] = {std::move(layer.bloom), layer.itemCount};
  }
}

// --- Wire format serialization ---

size_t ComputeHeaderSize(const ScalingBloomFilter& filter) {
  return sizeof(WireFilterHeader) + filter.NumLayers() * sizeof(WireLayerMeta);
}

size_t SerializeHeader(const ScalingBloomFilter& filter, void* output) {
  auto* hdr = static_cast<WireFilterHeader*>(output);
  hdr->totalItems = filter.TotalItems();
  hdr->numLayers = static_cast<uint32_t>(filter.NumLayers());
  hdr->flags = ToUnderlying(filter.Flags());
  hdr->expansionFactor = filter.ExpansionFactor();

  auto* meta = reinterpret_cast<WireLayerMeta*>(
    static_cast<char*>(output) + sizeof(WireFilterHeader));

  for (size_t i = 0; i < filter.NumLayers(); i++) {
    const auto& layer = filter.Layers()[i];
    meta[i] = {
      .dataSize = layer.bloom.GetDataSize(),
      .totalBits = layer.bloom.GetTotalBits(),
      .itemCount = layer.itemCount,
      .fpRate = layer.bloom.GetFpRate(),
      .bitsPerEntry = layer.bloom.GetBitsPerEntry(),
      .hashCount = layer.bloom.GetHashCount(),
      .capacity = layer.bloom.GetCapacity(),
      .log2Bits = layer.bloom.GetLog2Bits(),
    };
  }

  return sizeof(WireFilterHeader) + filter.NumLayers() * sizeof(WireLayerMeta);
}

ScalingBloomFilter* DeserializeHeader(const void* data, size_t length) {
  if (length < sizeof(WireFilterHeader)) return nullptr;

  const auto* hdr = static_cast<const WireFilterHeader*>(data);
  size_t required = sizeof(WireFilterHeader) + hdr->numLayers * sizeof(WireLayerMeta);
  if (length < required) return nullptr;

  auto* filter = ScalingBloomFilter::FromRdbShell({
    .totalItems = hdr->totalItems,
    .numLayers = hdr->numLayers,
    .flags = FromUnderlying(hdr->flags),
    .expansionFactor = hdr->expansionFactor,
  });
  if (!filter) return nullptr;

  const auto* meta = reinterpret_cast<const WireLayerMeta*>(
    static_cast<const char*>(data) + sizeof(WireFilterHeader));

  for (size_t i = 0; i < hdr->numLayers; i++) {
    auto* bitArray = static_cast<uint8_t*>(RMCalloc(meta[i].dataSize, 1));
    if (!bitArray) {
      filter->~ScalingBloomFilter();
      RMFree(filter);
      return nullptr;
    }
    auto layer = BloomLayer::FromRdb({
      .hashCount = meta[i].hashCount,
      .log2Bits = meta[i].log2Bits,
      .capacity = meta[i].capacity,
      .fpRate = meta[i].fpRate,
      .bitsPerEntry = meta[i].bitsPerEntry,
      .totalBits = meta[i].totalBits,
      .dataSize = meta[i].dataSize,
      .use64Bit = HasFlag(FromUnderlying(hdr->flags), BloomFlags::Use64Bit),
      .bitArray = bitArray,
      .itemCount = meta[i].itemCount,
    });
    filter->SetLayer(i, {std::move(layer), meta[i].itemCount});
  }

  return filter;
}
