#include "bloom_rdb.h"
#include "sb_chain.h"
#include "rm_alloc.h"

#include <cstring>

RedisModuleType* BloomType = nullptr;

void* RdbLoadBloom(RedisModuleIO* rdb, int encver) {
  if (encver > kCurrentEncVer) return nullptr;

  ScalingBloomFilter::RdbShell shell;
  shell.totalItems = RedisModule_LoadUnsigned(rdb);
  shell.numLayers = static_cast<size_t>(RedisModule_LoadUnsigned(rdb));

  shell.flags = (encver >= kEncVerWithFlags)
    ? FromUnderlying(static_cast<unsigned>(RedisModule_LoadUnsigned(rdb)))
    : (BloomFlags::Use64Bit | BloomFlags::NoRound);

  shell.expansionFactor = (encver >= kEncVerWithExpansion)
    ? static_cast<unsigned>(RedisModule_LoadUnsigned(rdb))
    : 2;

  if (shell.numLayers == 0) return nullptr;

  auto* filter = ScalingBloomFilter::FromRdbShell(shell);
  if (!filter) return nullptr;

  for (size_t i = 0; i < shell.numLayers; i++) {
    BloomLayer::RdbParams params;
    params.capacity = RedisModule_LoadUnsigned(rdb);
    params.fpRate = RedisModule_LoadDouble(rdb);
    params.hashCount = static_cast<uint32_t>(RedisModule_LoadUnsigned(rdb));
    params.bitsPerEntry = RedisModule_LoadDouble(rdb);
    params.totalBits = RedisModule_LoadUnsigned(rdb);
    params.log2Bits = static_cast<uint8_t>(RedisModule_LoadUnsigned(rdb));
    params.dataSize = (params.totalBits > 0) ? (params.totalBits / 8) : 0;
    params.use64Bit = HasFlag(shell.flags, BloomFlags::Use64Bit);

    size_t bufLen = 0;
    char* buf = RedisModule_LoadStringBuffer(rdb, &bufLen);
    params.bitArray = static_cast<uint8_t*>(RMAlloc(params.dataSize));
    if (!params.bitArray) {
      if (buf) RedisModule_Free(buf);
      filter->~ScalingBloomFilter();
      RMFree(filter);
      return nullptr;
    }
    if (buf && bufLen > 0) {
      std::memcpy(params.bitArray, buf,
                   std::min(bufLen, static_cast<size_t>(params.dataSize)));
      RedisModule_Free(buf);
    } else {
      std::memset(params.bitArray, 0, params.dataSize);
      if (buf) RedisModule_Free(buf);
    }

    params.itemCount = static_cast<size_t>(RedisModule_LoadUnsigned(rdb));

    auto layer = BloomLayer::FromRdb(params);
    filter->SetLayer(i, {std::move(layer), params.itemCount});
  }

  return filter;
}

void RdbSaveBloom(RedisModuleIO* rdb, void* value) {
  auto* filter = static_cast<ScalingBloomFilter*>(value);

  RedisModule_SaveUnsigned(rdb, filter->TotalItems());
  RedisModule_SaveUnsigned(rdb, static_cast<uint64_t>(filter->NumLayers()));
  RedisModule_SaveUnsigned(rdb, ToUnderlying(filter->Flags()));
  RedisModule_SaveUnsigned(rdb, filter->ExpansionFactor());

  for (const auto& layer : filter->Layers()) {
    RedisModule_SaveUnsigned(rdb, layer.bloom.GetCapacity());
    RedisModule_SaveDouble(rdb, layer.bloom.GetFpRate());
    RedisModule_SaveUnsigned(rdb, layer.bloom.GetHashCount());
    RedisModule_SaveDouble(rdb, layer.bloom.GetBitsPerEntry());
    RedisModule_SaveUnsigned(rdb, layer.bloom.GetTotalBits());
    RedisModule_SaveUnsigned(rdb, layer.bloom.GetLog2Bits());
    RedisModule_SaveStringBuffer(rdb,
      reinterpret_cast<const char*>(layer.bloom.GetBitArray()),
      layer.bloom.GetDataSize());
    RedisModule_SaveUnsigned(rdb, layer.itemCount);
  }
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
