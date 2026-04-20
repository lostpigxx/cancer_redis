#include "bloom_rdb.h"
#include "sb_chain.h"
#include "rm_alloc.h"

#include <cstring>

RedisModuleType* BloomType = nullptr;

void* RdbLoadBloom(RedisModuleIO* rdb, int encver) {
  if (encver > kCurrentEncVer) return nullptr;

  auto* filter = static_cast<ScalingBloomFilter*>(RMCalloc(1, sizeof(ScalingBloomFilter)));
  if (!filter) return nullptr;

  filter->totalItems = RedisModule_LoadUnsigned(rdb);
  filter->numLayers = static_cast<size_t>(RedisModule_LoadUnsigned(rdb));

  filter->flags = (encver >= kEncVerWithFlags)
    ? static_cast<unsigned>(RedisModule_LoadUnsigned(rdb))
    : (kUse64Bit | kNoRound);

  filter->expansionFactor = (encver >= kEncVerWithExpansion)
    ? static_cast<unsigned>(RedisModule_LoadUnsigned(rdb))
    : 2;

  if (filter->numLayers == 0) {
    RMFree(filter);
    return nullptr;
  }

  filter->layers = static_cast<FilterLayer*>(RMCalloc(filter->numLayers, sizeof(FilterLayer)));
  if (!filter->layers) {
    RMFree(filter);
    return nullptr;
  }

  for (size_t i = 0; i < filter->numLayers; i++) {
    FilterLayer& layer = filter->layers[i];
    std::memset(&layer, 0, sizeof(FilterLayer));

    layer.bloom.capacity = RedisModule_LoadUnsigned(rdb);
    layer.bloom.fpRate = RedisModule_LoadDouble(rdb);
    layer.bloom.hashCount = static_cast<uint32_t>(RedisModule_LoadUnsigned(rdb));
    layer.bloom.bitsPerEntry = RedisModule_LoadDouble(rdb);
    layer.bloom.totalBits = RedisModule_LoadUnsigned(rdb);
    layer.bloom.log2Bits = static_cast<uint8_t>(RedisModule_LoadUnsigned(rdb));
    layer.bloom.dataSize = (layer.bloom.totalBits > 0) ? (layer.bloom.totalBits / 8) : 0;
    layer.bloom.prefer64 = (filter->flags & kUse64Bit) ? 1 : 0;

    size_t bufLen = 0;
    char* buf = RedisModule_LoadStringBuffer(rdb, &bufLen);
    if (buf && bufLen > 0) {
      layer.bloom.bitArray = static_cast<uint8_t*>(RMAlloc(layer.bloom.dataSize));
      if (!layer.bloom.bitArray) {
        RedisModule_Free(buf);
        goto cleanup;
      }
      std::memcpy(layer.bloom.bitArray, buf,
                   bufLen < layer.bloom.dataSize ? bufLen : layer.bloom.dataSize);
      RedisModule_Free(buf);
    } else {
      if (buf) RedisModule_Free(buf);
      layer.bloom.bitArray = static_cast<uint8_t*>(RMCalloc(layer.bloom.dataSize, 1));
      if (!layer.bloom.bitArray) goto cleanup;
    }

    layer.itemCount = static_cast<size_t>(RedisModule_LoadUnsigned(rdb));
  }

  return filter;

cleanup:
  for (size_t j = 0; j < filter->numLayers; j++) {
    if (filter->layers[j].bloom.bitArray) {
      RMFree(filter->layers[j].bloom.bitArray);
    }
  }
  RMFree(filter->layers);
  RMFree(filter);
  return nullptr;
}

void RdbSaveBloom(RedisModuleIO* rdb, void* value) {
  auto* filter = static_cast<ScalingBloomFilter*>(value);

  RedisModule_SaveUnsigned(rdb, filter->totalItems);
  RedisModule_SaveUnsigned(rdb, static_cast<uint64_t>(filter->numLayers));
  RedisModule_SaveUnsigned(rdb, filter->flags);
  RedisModule_SaveUnsigned(rdb, filter->expansionFactor);

  for (size_t i = 0; i < filter->numLayers; i++) {
    const FilterLayer& layer = filter->layers[i];
    RedisModule_SaveUnsigned(rdb, layer.bloom.capacity);
    RedisModule_SaveDouble(rdb, layer.bloom.fpRate);
    RedisModule_SaveUnsigned(rdb, layer.bloom.hashCount);
    RedisModule_SaveDouble(rdb, layer.bloom.bitsPerEntry);
    RedisModule_SaveUnsigned(rdb, layer.bloom.totalBits);
    RedisModule_SaveUnsigned(rdb, layer.bloom.log2Bits);
    RedisModule_SaveStringBuffer(rdb, reinterpret_cast<const char*>(layer.bloom.bitArray),
                                  layer.bloom.dataSize);
    RedisModule_SaveUnsigned(rdb, layer.itemCount);
  }
}

void AofRewriteBloom(RedisModuleIO* aof, RedisModuleString* key, void* value) {
  auto* filter = static_cast<ScalingBloomFilter*>(value);

  size_t hdrBytes = ComputeHeaderSize(filter);
  auto* hdrBuf = static_cast<char*>(RMAlloc(hdrBytes));
  SerializeHeader(filter, hdrBuf);

  RedisModule_EmitAOF(aof, "BF.LOADCHUNK", "slb", key, (long long)1, hdrBuf, hdrBytes);
  RMFree(hdrBuf);

  for (size_t i = 0; i < filter->numLayers; i++) {
    const FilterLayer& layer = filter->layers[i];
    RedisModule_EmitAOF(aof, "BF.LOADCHUNK", "slb", key,
                         static_cast<long long>(i + 2),
                         reinterpret_cast<const char*>(layer.bloom.bitArray),
                         layer.bloom.dataSize);
  }
}

void FreeBloom(void* value) {
  if (value) {
    static_cast<ScalingBloomFilter*>(value)->Free();
  }
}

size_t BloomMemUsage(const void* value) {
  if (!value) return 0;
  return static_cast<const ScalingBloomFilter*>(value)->BytesUsed();
}
