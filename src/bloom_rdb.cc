#include "bloom_rdb.h"
#include "sb_chain.h"
#include "rm_alloc.h"

#include <cstring>

RedisModuleType* BFType = nullptr;

void* BFRdbLoad(RedisModuleIO* rdb, int encver) {
  if (encver > kBFCurrentEncver) {
    return nullptr;
  }

  auto* chain = static_cast<SBChain*>(RMCalloc(1, sizeof(SBChain)));
  if (!chain) return nullptr;

  chain->size = RedisModule_LoadUnsigned(rdb);
  chain->nfilters = static_cast<size_t>(RedisModule_LoadUnsigned(rdb));

  if (encver >= kBFMinOptionsEnc) {
    chain->options = static_cast<unsigned>(RedisModule_LoadUnsigned(rdb));
  } else {
    chain->options = kBloomOptForce64 | kBloomOptNoRound;
  }

  if (encver >= kBFMinGrowthEnc) {
    chain->growth = static_cast<unsigned>(RedisModule_LoadUnsigned(rdb));
  } else {
    chain->growth = 2;
  }

  if (chain->nfilters == 0) {
    RMFree(chain);
    return nullptr;
  }

  chain->filters = static_cast<SBLink*>(RMCalloc(chain->nfilters, sizeof(SBLink)));
  if (!chain->filters) {
    RMFree(chain);
    return nullptr;
  }

  for (size_t i = 0; i < chain->nfilters; i++) {
    SBLink& link = chain->filters[i];
    std::memset(&link, 0, sizeof(SBLink));

    link.inner.entries = RedisModule_LoadUnsigned(rdb);
    link.inner.error = RedisModule_LoadDouble(rdb);
    link.inner.hashes = static_cast<uint32_t>(RedisModule_LoadUnsigned(rdb));
    link.inner.bpe = RedisModule_LoadDouble(rdb);
    link.inner.bits = RedisModule_LoadUnsigned(rdb);
    link.inner.n2 = static_cast<uint8_t>(RedisModule_LoadUnsigned(rdb));

    if (link.inner.bits > 0) {
      link.inner.bytes = link.inner.bits / 8;
    } else {
      link.inner.bytes = 0;
    }

    link.inner.force64 = (chain->options & kBloomOptForce64) ? 1 : 0;

    size_t bufLen = 0;
    char* buf = RedisModule_LoadStringBuffer(rdb, &bufLen);
    if (buf && bufLen > 0) {
      link.inner.bf = static_cast<uint8_t*>(RMAlloc(link.inner.bytes));
      if (!link.inner.bf) {
        RedisModule_Free(buf);
        goto error;
      }
      size_t copyLen = bufLen < link.inner.bytes ? bufLen : link.inner.bytes;
      std::memcpy(link.inner.bf, buf, copyLen);
      RedisModule_Free(buf);
    } else {
      if (buf) RedisModule_Free(buf);
      link.inner.bf = static_cast<uint8_t*>(RMCalloc(link.inner.bytes, 1));
      if (!link.inner.bf) goto error;
    }

    link.size = static_cast<size_t>(RedisModule_LoadUnsigned(rdb));
  }

  return chain;

error:
  for (size_t j = 0; j < chain->nfilters; j++) {
    if (chain->filters[j].inner.bf) {
      RMFree(chain->filters[j].inner.bf);
    }
  }
  RMFree(chain->filters);
  RMFree(chain);
  return nullptr;
}

void BFRdbSave(RedisModuleIO* rdb, void* value) {
  auto* chain = static_cast<SBChain*>(value);

  RedisModule_SaveUnsigned(rdb, chain->size);
  RedisModule_SaveUnsigned(rdb, static_cast<uint64_t>(chain->nfilters));
  RedisModule_SaveUnsigned(rdb, chain->options);
  RedisModule_SaveUnsigned(rdb, chain->growth);

  for (size_t i = 0; i < chain->nfilters; i++) {
    const SBLink& link = chain->filters[i];
    RedisModule_SaveUnsigned(rdb, link.inner.entries);
    RedisModule_SaveDouble(rdb, link.inner.error);
    RedisModule_SaveUnsigned(rdb, link.inner.hashes);
    RedisModule_SaveDouble(rdb, link.inner.bpe);
    RedisModule_SaveUnsigned(rdb, link.inner.bits);
    RedisModule_SaveUnsigned(rdb, link.inner.n2);
    RedisModule_SaveStringBuffer(rdb, reinterpret_cast<const char*>(link.inner.bf),
                                  link.inner.bytes);
    RedisModule_SaveUnsigned(rdb, link.size);
  }
}

void BFAofRewrite(RedisModuleIO* aof, RedisModuleString* key, void* value) {
  auto* chain = static_cast<SBChain*>(value);

  size_t hdrSize = SBChainDumpHeaderSize(chain);
  auto* hdrBuf = static_cast<char*>(RMAlloc(hdrSize));
  SBChainDumpHeader(chain, hdrBuf);

  RedisModule_EmitAOF(aof, "BF.LOADCHUNK", "slb",
                       key, (long long)1, hdrBuf, hdrSize);
  RMFree(hdrBuf);

  long long iter = 2;
  for (size_t i = 0; i < chain->nfilters; i++) {
    const SBLink& link = chain->filters[i];
    RedisModule_EmitAOF(aof, "BF.LOADCHUNK", "slb",
                         key, iter,
                         reinterpret_cast<const char*>(link.inner.bf),
                         link.inner.bytes);
    iter++;
  }
}

void BFFree(void* value) {
  if (value) {
    auto* chain = static_cast<SBChain*>(value);
    chain->Destroy();
  }
}

size_t BFMemUsage(const void* value) {
  if (!value) return 0;
  return static_cast<const SBChain*>(value)->MemUsage();
}
