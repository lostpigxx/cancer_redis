#include "bloom_commands.h"
#include "bloom_rdb.h"
#include "bloom_config.h"
#include "sb_chain.h"
#include "rm_alloc.h"

#include <cstring>

// Helper: open key and get SBChain, or return null
static SBChain* GetBloomChain(RedisModuleKey* key) {
  if (RedisModule_ModuleTypeGetType(key) != BFType) {
    return nullptr;
  }
  return static_cast<SBChain*>(RedisModule_ModuleTypeGetValue(key));
}

static SBChain* CreateDefaultChain() {
  unsigned opts = kBloomOptForce64 | kBloomOptNoRound;
  return SBChain::Create(g_bloomConfig.defaultCapacity,
                          g_bloomConfig.defaultErrorRate,
                          opts, g_bloomConfig.defaultExpansion);
}

static SBChain* CreateChain(uint64_t capacity, double errorRate,
                             unsigned expansion, bool nonScaling) {
  unsigned opts = kBloomOptForce64 | kBloomOptNoRound;
  if (nonScaling) opts |= kBloomOptNoScaling;
  if (expansion == 0) opts |= kBloomOptNoScaling;
  return SBChain::Create(capacity, errorRate, opts,
                          expansion > 0 ? expansion : 2);
}

// --- BF.RESERVE ---
int BFReserveCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 4) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx);

  double errorRate;
  if (RedisModule_StringToDouble(argv[2], &errorRate) != REDISMODULE_OK ||
      errorRate <= 0.0 || errorRate >= 1.0) {
    return RedisModule_ReplyWithError(ctx, "ERR (0 < error rate range < 1)");
  }

  long long capacity;
  if (RedisModule_StringToLongLong(argv[3], &capacity) != REDISMODULE_OK ||
      capacity <= 0) {
    return RedisModule_ReplyWithError(ctx, "ERR (capacity must be > 0)");
  }

  unsigned expansion = g_bloomConfig.defaultExpansion;
  bool nonScaling = false;

  for (int i = 4; i < argc; i++) {
    size_t len;
    const char* arg = RedisModule_StringPtrLen(argv[i], &len);
    if (strncasecmp(arg, "EXPANSION", len) == 0) {
      if (i + 1 >= argc) {
        return RedisModule_ReplyWithError(ctx, "ERR EXPANSION requires a value");
      }
      long long val;
      if (RedisModule_StringToLongLong(argv[++i], &val) != REDISMODULE_OK || val < 0) {
        return RedisModule_ReplyWithError(ctx, "ERR bad expansion");
      }
      expansion = static_cast<unsigned>(val);
    } else if (strncasecmp(arg, "NONSCALING", len) == 0) {
      nonScaling = true;
    } else {
      return RedisModule_ReplyWithError(ctx, "ERR unknown argument");
    }
  }

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));

  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "ERR item exists");
  }

  auto* chain = CreateChain(static_cast<uint64_t>(capacity), errorRate,
                             expansion, nonScaling);
  if (!chain) {
    return RedisModule_ReplyWithError(ctx, "ERR could not create filter");
  }

  RedisModule_ModuleTypeSetValue(key, BFType, chain);
  RedisModule_ReplicateVerbatim(ctx);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// --- BF.ADD ---
int BFAddCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));

  SBChain* chain = nullptr;
  int keyType = RedisModule_KeyType(key);

  if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
    chain = CreateDefaultChain();
    if (!chain) {
      return RedisModule_ReplyWithError(ctx, "ERR could not create filter");
    }
    RedisModule_ModuleTypeSetValue(key, BFType, chain);
  } else if (keyType == REDISMODULE_KEYTYPE_MODULE) {
    chain = GetBloomChain(key);
    if (!chain) {
      return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
  } else {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  size_t len;
  const char* buf = RedisModule_StringPtrLen(argv[2], &len);
  int rv = chain->Add(buf, len);

  if (rv == -1) {
    return RedisModule_ReplyWithError(ctx, "ERR non-scaling filter is full");
  }

  if (rv == 1) {
    RedisModule_ReplicateVerbatim(ctx);
  }
  return RedisModule_ReplyWithLongLong(ctx, rv);
}

// --- BF.MADD ---
int BFMaddCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));

  SBChain* chain = nullptr;
  int keyType = RedisModule_KeyType(key);

  if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
    chain = CreateDefaultChain();
    if (!chain) {
      return RedisModule_ReplyWithError(ctx, "ERR could not create filter");
    }
    RedisModule_ModuleTypeSetValue(key, BFType, chain);
  } else if (keyType == REDISMODULE_KEYTYPE_MODULE) {
    chain = GetBloomChain(key);
    if (!chain) {
      return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
  } else {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  int nitems = argc - 2;
  RedisModule_ReplyWithArray(ctx, nitems);

  bool modified = false;
  for (int i = 0; i < nitems; i++) {
    size_t len;
    const char* buf = RedisModule_StringPtrLen(argv[i + 2], &len);
    int rv = chain->Add(buf, len);
    if (rv == -1) {
      RedisModule_ReplyWithError(ctx, "ERR non-scaling filter is full");
    } else {
      RedisModule_ReplyWithLongLong(ctx, rv);
      if (rv == 1) modified = true;
    }
  }

  if (modified) {
    RedisModule_ReplicateVerbatim(ctx);
  }
  return REDISMODULE_OK;
}

// --- BF.INSERT ---
int BFInsertCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 4) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  double errorRate = g_bloomConfig.defaultErrorRate;
  long long capacity = static_cast<long long>(g_bloomConfig.defaultCapacity);
  unsigned expansion = g_bloomConfig.defaultExpansion;
  bool noCreate = false;
  bool nonScaling = false;
  int itemsIdx = -1;

  for (int i = 2; i < argc; i++) {
    size_t len;
    const char* arg = RedisModule_StringPtrLen(argv[i], &len);

    if (strncasecmp(arg, "ERROR", len) == 0) {
      if (i + 1 >= argc) return RedisModule_WrongArity(ctx);
      if (RedisModule_StringToDouble(argv[++i], &errorRate) != REDISMODULE_OK ||
          errorRate <= 0.0 || errorRate >= 1.0) {
        return RedisModule_ReplyWithError(ctx, "ERR (0 < error rate range < 1)");
      }
    } else if (strncasecmp(arg, "CAPACITY", len) == 0) {
      if (i + 1 >= argc) return RedisModule_WrongArity(ctx);
      if (RedisModule_StringToLongLong(argv[++i], &capacity) != REDISMODULE_OK ||
          capacity <= 0) {
        return RedisModule_ReplyWithError(ctx, "ERR (capacity must be > 0)");
      }
    } else if (strncasecmp(arg, "EXPANSION", len) == 0) {
      if (i + 1 >= argc) return RedisModule_WrongArity(ctx);
      long long val;
      if (RedisModule_StringToLongLong(argv[++i], &val) != REDISMODULE_OK || val < 0) {
        return RedisModule_ReplyWithError(ctx, "ERR bad expansion");
      }
      expansion = static_cast<unsigned>(val);
    } else if (strncasecmp(arg, "NOCREATE", len) == 0) {
      noCreate = true;
    } else if (strncasecmp(arg, "NONSCALING", len) == 0) {
      nonScaling = true;
    } else if (strncasecmp(arg, "ITEMS", len) == 0) {
      itemsIdx = i + 1;
      break;
    } else {
      return RedisModule_ReplyWithError(ctx, "ERR unknown argument");
    }
  }

  if (itemsIdx < 0 || itemsIdx >= argc) {
    return RedisModule_ReplyWithError(ctx, "ERR ITEMS not found");
  }

  int nitems = argc - itemsIdx;

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));

  SBChain* chain = nullptr;
  int keyType = RedisModule_KeyType(key);

  if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
    if (noCreate) {
      return RedisModule_ReplyWithError(ctx, "ERR not found");
    }
    chain = CreateChain(static_cast<uint64_t>(capacity), errorRate,
                         expansion, nonScaling);
    if (!chain) {
      return RedisModule_ReplyWithError(ctx, "ERR could not create filter");
    }
    RedisModule_ModuleTypeSetValue(key, BFType, chain);
  } else if (keyType == REDISMODULE_KEYTYPE_MODULE) {
    chain = GetBloomChain(key);
    if (!chain) {
      return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
  } else {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  RedisModule_ReplyWithArray(ctx, nitems);

  bool modified = false;
  for (int i = 0; i < nitems; i++) {
    size_t len;
    const char* buf = RedisModule_StringPtrLen(argv[itemsIdx + i], &len);
    int rv = chain->Add(buf, len);
    if (rv == -1) {
      RedisModule_ReplyWithError(ctx, "ERR non-scaling filter is full");
    } else {
      RedisModule_ReplyWithLongLong(ctx, rv);
      if (rv == 1) modified = true;
    }
  }

  if (modified) {
    RedisModule_ReplicateVerbatim(ctx);
  }
  return REDISMODULE_OK;
}

// --- BF.EXISTS ---
int BFExistsCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }

  auto* chain = GetBloomChain(key);
  if (!chain) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  size_t len;
  const char* buf = RedisModule_StringPtrLen(argv[2], &len);
  int rv = chain->Check(buf, len);
  return RedisModule_ReplyWithLongLong(ctx, rv);
}

// --- BF.MEXISTS ---
int BFMexistsCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  int nitems = argc - 2;
  RedisModule_ReplyWithArray(ctx, nitems);

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    for (int i = 0; i < nitems; i++) {
      RedisModule_ReplyWithLongLong(ctx, 0);
    }
    return REDISMODULE_OK;
  }

  auto* chain = GetBloomChain(key);
  if (!chain) {
    for (int i = 0; i < nitems; i++) {
      RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    return REDISMODULE_OK;
  }

  for (int i = 0; i < nitems; i++) {
    size_t len;
    const char* buf = RedisModule_StringPtrLen(argv[i + 2], &len);
    int rv = chain->Check(buf, len);
    RedisModule_ReplyWithLongLong(ctx, rv);
  }
  return REDISMODULE_OK;
}

// --- BF.INFO ---
int BFInfoCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 2 || argc > 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "ERR not found");
  }

  auto* chain = GetBloomChain(key);
  if (!chain) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  if (argc == 3) {
    size_t len;
    const char* sub = RedisModule_StringPtrLen(argv[2], &len);
    if (strncasecmp(sub, "Capacity", len) == 0) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(chain->Capacity()));
    } else if (strncasecmp(sub, "Size", len) == 0) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(chain->MemUsage()));
    } else if (strncasecmp(sub, "Filters", len) == 0) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(chain->nfilters));
    } else if (strncasecmp(sub, "Items", len) == 0) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(chain->size));
    } else if (strncasecmp(sub, "Expansion", len) == 0) {
      if (chain->options & kBloomOptNoScaling) {
        return RedisModule_ReplyWithNull(ctx);
      }
      return RedisModule_ReplyWithLongLong(ctx, chain->growth);
    } else {
      return RedisModule_ReplyWithError(ctx, "ERR unknown subcommand");
    }
  }

  // Full info: 10 elements (5 key-value pairs)
  RedisModule_ReplyWithArray(ctx, 10);

  RedisModule_ReplyWithSimpleString(ctx, "Capacity");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(chain->Capacity()));

  RedisModule_ReplyWithSimpleString(ctx, "Size");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(chain->MemUsage()));

  RedisModule_ReplyWithSimpleString(ctx, "Number of filters");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(chain->nfilters));

  RedisModule_ReplyWithSimpleString(ctx, "Number of items inserted");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(chain->size));

  RedisModule_ReplyWithSimpleString(ctx, "Expansion rate");
  if (chain->options & kBloomOptNoScaling) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    RedisModule_ReplyWithLongLong(ctx, chain->growth);
  }

  return REDISMODULE_OK;
}

// --- BF.CARD ---
int BFCardCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }

  auto* chain = GetBloomChain(key);
  if (!chain) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(chain->size));
}

// --- BF.SCANDUMP ---
int BFScandumpCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "ERR not found");
  }

  auto* chain = GetBloomChain(key);
  if (!chain) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  long long iter;
  if (RedisModule_StringToLongLong(argv[2], &iter) != REDISMODULE_OK || iter < 0) {
    return RedisModule_ReplyWithError(ctx, "ERR invalid iterator");
  }

  RedisModule_ReplyWithArray(ctx, 2);

  if (iter == 0) {
    // Return header
    size_t hdrSize = SBChainDumpHeaderSize(chain);
    auto* hdrBuf = static_cast<char*>(RMAlloc(hdrSize));
    SBChainDumpHeader(chain, hdrBuf);
    RedisModule_ReplyWithLongLong(ctx, 1);
    RedisModule_ReplyWithStringBuffer(ctx, hdrBuf, hdrSize);
    RMFree(hdrBuf);
  } else if (iter >= 1 && static_cast<size_t>(iter - 1) < chain->nfilters) {
    // Return bit array for filter (iter-1)
    size_t idx = static_cast<size_t>(iter - 1);
    const SBLink& link = chain->filters[idx];
    long long nextIter = (idx + 1 < chain->nfilters) ? iter + 1 : 0;
    RedisModule_ReplyWithLongLong(ctx, nextIter);
    RedisModule_ReplyWithStringBuffer(ctx,
                                       reinterpret_cast<const char*>(link.inner.bf),
                                       link.inner.bytes);
  } else {
    // Done
    RedisModule_ReplyWithLongLong(ctx, 0);
    RedisModule_ReplyWithStringBuffer(ctx, "", 0);
  }

  return REDISMODULE_OK;
}

// --- BF.LOADCHUNK ---
int BFLoadchunkCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  long long iter;
  if (RedisModule_StringToLongLong(argv[2], &iter) != REDISMODULE_OK || iter < 1) {
    return RedisModule_ReplyWithError(ctx, "ERR invalid iterator");
  }

  size_t dataLen;
  const char* data = RedisModule_StringPtrLen(argv[3], &dataLen);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));

  if (iter == 1) {
    // Header chunk — create the filter
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
      // Delete existing key first
      RedisModule_DeleteKey(key);
      key = static_cast<RedisModuleKey*>(
        RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));
    }

    SBChain* chain = SBChainLoadHeader(data, dataLen);
    if (!chain) {
      return RedisModule_ReplyWithError(ctx, "ERR invalid header");
    }
    RedisModule_ModuleTypeSetValue(key, BFType, chain);
  } else {
    // Data chunk — load bit array
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
      return RedisModule_ReplyWithError(ctx, "ERR not found");
    }

    auto* chain = GetBloomChain(key);
    if (!chain) {
      return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    size_t idx = static_cast<size_t>(iter - 2);
    if (idx >= chain->nfilters) {
      return RedisModule_ReplyWithError(ctx, "ERR invalid iterator");
    }

    SBLink& link = chain->filters[idx];
    size_t copyLen = dataLen < link.inner.bytes ? dataLen : link.inner.bytes;
    std::memcpy(link.inner.bf, data, copyLen);
  }

  RedisModule_ReplicateVerbatim(ctx);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// --- Command Registration ---
int RegisterBloomCommands(RedisModuleCtx* ctx) {
#define REG(name, func, flags) \
  if (RedisModule_CreateCommand(ctx, name, func, flags, 1, 1, 1) == REDISMODULE_ERR) \
    return REDISMODULE_ERR;

  REG("BF.RESERVE", BFReserveCommand, "write deny-oom");
  REG("BF.ADD", BFAddCommand, "write deny-oom");
  REG("BF.MADD", BFMaddCommand, "write deny-oom");
  REG("BF.INSERT", BFInsertCommand, "write deny-oom");
  REG("BF.EXISTS", BFExistsCommand, "readonly");
  REG("BF.MEXISTS", BFMexistsCommand, "readonly");
  REG("BF.INFO", BFInfoCommand, "readonly");
  REG("BF.CARD", BFCardCommand, "readonly");
  REG("BF.SCANDUMP", BFScandumpCommand, "readonly");
  REG("BF.LOADCHUNK", BFLoadchunkCommand, "write deny-oom");

#undef REG
  return REDISMODULE_OK;
}
