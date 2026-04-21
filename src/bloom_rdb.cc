#include "bloom_rdb.h"
#include "sb_chain.h"
#include "rm_alloc.h"

#include <algorithm>
#include <cstring>

// --- RdbWriter / RdbReader ---

void RdbWriter::PutUint(uint64_t v) { RedisModule_SaveUnsigned(io_, v); }
void RdbWriter::PutFloat(double v) { RedisModule_SaveDouble(io_, v); }
void RdbWriter::PutBlob(const uint8_t* data, uint64_t len) {
  RedisModule_SaveStringBuffer(io_, reinterpret_cast<const char*>(data), len);
}

uint64_t RdbReader::GetUint() { return RedisModule_LoadUnsigned(io_); }
double RdbReader::GetFloat() { return RedisModule_LoadDouble(io_); }
std::pair<char*, size_t> RdbReader::GetBlob() {
  size_t len = 0;
  char* buf = RedisModule_LoadStringBuffer(io_, &len);
  return {buf, len};
}

// --- BloomLayer serialization (lives here to access Redis Module API) ---

void BloomLayer::WriteTo(RdbWriter& w) const {
  w.PutUint(capacity_);
  w.PutFloat(fpRate_);
  w.PutUint(hashCount_);
  w.PutFloat(bitsPerEntry_);
  w.PutUint(totalBits_);
  w.PutUint(log2Bits_);
  w.PutBlob(bitArray_, dataSize_);
}

std::optional<BloomLayer> BloomLayer::ReadFrom(RdbReader& r, BloomFlags filterFlags) {
  BloomLayer layer;
  layer.capacity_ = r.GetUint();
  layer.fpRate_ = r.GetFloat();
  layer.hashCount_ = static_cast<uint32_t>(r.GetUint());
  layer.bitsPerEntry_ = r.GetFloat();
  layer.totalBits_ = r.GetUint();
  layer.log2Bits_ = static_cast<uint8_t>(r.GetUint());
  layer.dataSize_ = (layer.totalBits_ > 0) ? (layer.totalBits_ / 8) : 0;
  layer.use64Bit_ = HasFlag(filterFlags, BloomFlags::Use64Bit);

  auto [buf, bufLen] = r.GetBlob();
  layer.bitArray_ = static_cast<uint8_t*>(RMAlloc(layer.dataSize_));
  if (!layer.bitArray_) {
    if (buf) RedisModule_Free(buf);
    return std::nullopt;
  }
  if (buf && bufLen > 0) {
    std::memcpy(layer.bitArray_, buf, std::min(bufLen, static_cast<size_t>(layer.dataSize_)));
    RedisModule_Free(buf);
  } else {
    std::memset(layer.bitArray_, 0, layer.dataSize_);
    if (buf) RedisModule_Free(buf);
  }
  return layer;
}

WireLayerMeta BloomLayer::ToWireMeta(size_t itemCount) const {
  return {
    .dataSize = dataSize_,
    .totalBits = totalBits_,
    .itemCount = itemCount,
    .fpRate = fpRate_,
    .bitsPerEntry = bitsPerEntry_,
    .hashCount = hashCount_,
    .capacity = capacity_,
    .log2Bits = log2Bits_,
  };
}

BloomLayer BloomLayer::FromWireMeta(const WireLayerMeta& meta, BloomFlags filterFlags) {
  BloomLayer layer;
  layer.hashCount_ = meta.hashCount;
  layer.log2Bits_ = meta.log2Bits;
  layer.use64Bit_ = HasFlag(filterFlags, BloomFlags::Use64Bit);
  layer.capacity_ = meta.capacity;
  layer.fpRate_ = meta.fpRate;
  layer.bitsPerEntry_ = meta.bitsPerEntry;
  layer.totalBits_ = meta.totalBits;
  layer.dataSize_ = meta.dataSize;
  layer.bitArray_ = static_cast<uint8_t*>(RMCalloc(layer.dataSize_, 1));
  return layer;
}

// --- ScalingBloomFilter RDB serialization ---

void ScalingBloomFilter::WriteTo(RdbWriter& w) const {
  w.PutUint(totalItems_);
  w.PutUint(numLayers_);
  w.PutUint(ToUnderlying(flags_));
  w.PutUint(expansionFactor_);

  for (const auto& layer : Layers()) {
    layer.bloom.WriteTo(w);
    w.PutUint(layer.itemCount);
  }
}

ScalingBloomFilter* ScalingBloomFilter::ReadFrom(RdbReader& r, int encver) {
  RdbShell shell;
  shell.totalItems = r.GetUint();
  shell.numLayers = static_cast<size_t>(r.GetUint());

  shell.flags = (encver >= kEncVerWithFlags)
    ? FromUnderlying(static_cast<unsigned>(r.GetUint()))
    : (BloomFlags::Use64Bit | BloomFlags::NoRound);

  shell.expansionFactor = (encver >= kEncVerWithExpansion)
    ? static_cast<unsigned>(r.GetUint())
    : 2;

  if (shell.numLayers == 0) return nullptr;

  auto* filter = FromRdbShell(shell);
  if (!filter) return nullptr;

  for (size_t i = 0; i < shell.numLayers; i++) {
    auto maybeLayer = BloomLayer::ReadFrom(r, shell.flags);
    if (!maybeLayer) {
      filter->~ScalingBloomFilter();
      RMFree(filter);
      return nullptr;
    }
    size_t count = static_cast<size_t>(r.GetUint());
    filter->SetLayer(i, {std::move(*maybeLayer), count});
  }

  return filter;
}

// --- SCANDUMP wire format ---

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
    meta[i] = layer.bloom.ToWireMeta(layer.itemCount);
  }

  return sizeof(WireFilterHeader) + filter.NumLayers() * sizeof(WireLayerMeta);
}

ScalingBloomFilter* DeserializeHeader(const void* data, size_t length) {
  if (length < sizeof(WireFilterHeader)) return nullptr;

  const auto* hdr = static_cast<const WireFilterHeader*>(data);
  size_t required = sizeof(WireFilterHeader) + hdr->numLayers * sizeof(WireLayerMeta);
  if (length < required) return nullptr;

  auto filterFlags = FromUnderlying(hdr->flags);
  auto* filter = ScalingBloomFilter::FromRdbShell({
    .totalItems = hdr->totalItems,
    .numLayers = hdr->numLayers,
    .flags = filterFlags,
    .expansionFactor = hdr->expansionFactor,
  });
  if (!filter) return nullptr;

  const auto* meta = reinterpret_cast<const WireLayerMeta*>(
    static_cast<const char*>(data) + sizeof(WireFilterHeader));

  for (size_t i = 0; i < hdr->numLayers; i++) {
    auto layer = BloomLayer::FromWireMeta(meta[i], filterFlags);
    if (!layer.GetBitArray()) {
      filter->~ScalingBloomFilter();
      RMFree(filter);
      return nullptr;
    }
    filter->SetLayer(i, {std::move(layer), meta[i].itemCount});
  }

  return filter;
}

// --- Module type callbacks ---

RedisModuleType* BloomType = nullptr;

// The filter object serializes itself via WriteTo/ReadFrom.
// These callbacks are thin adapters between the Redis Module C API
// and our C++ serialization methods.

void* RdbLoadBloom(RedisModuleIO* rdb, int encver) {
  if (encver > kCurrentEncVer) return nullptr;
  RdbReader reader(rdb);
  return ScalingBloomFilter::ReadFrom(reader, encver);
}

void RdbSaveBloom(RedisModuleIO* rdb, void* value) {
  RdbWriter writer(rdb);
  static_cast<ScalingBloomFilter*>(value)->WriteTo(writer);
}

void AofRewriteBloom(RedisModuleIO* aof, RedisModuleString* key, void* value) {
  auto* filter = static_cast<ScalingBloomFilter*>(value);

  size_t hdrBytes = ComputeHeaderSize(*filter);
  auto* hdrBuf = static_cast<char*>(RMAlloc(hdrBytes));
  SerializeHeader(*filter, hdrBuf);

  RedisModule_EmitAOF(aof, "BF.LOADCHUNK", "slb", key, (long long)1, hdrBuf, hdrBytes);
  RMFree(hdrBuf);

  long long idx = 2;
  for (const auto& layer : filter->Layers()) {
    RedisModule_EmitAOF(aof, "BF.LOADCHUNK", "slb", key, idx++,
      reinterpret_cast<const char*>(layer.bloom.GetBitArray()),
      layer.bloom.GetDataSize());
  }
}

void FreeBloom(void* value) {
  if (auto* filter = static_cast<ScalingBloomFilter*>(value)) {
    filter->~ScalingBloomFilter();
    RMFree(filter);
  }
}

size_t BloomMemUsage(const void* value) {
  if (!value) return 0;
  return static_cast<const ScalingBloomFilter*>(value)->BytesUsed();
}
