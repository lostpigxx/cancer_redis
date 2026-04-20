#include "bloom_commands.h"
#include "bloom_rdb.h"
#include "bloom_config.h"
#include "sb_chain.h"
#include "rm_alloc.h"

#include <cstring>

static ScalingBloomFilter* GetFilter(RedisModuleKey* key) {
  if (RedisModule_ModuleTypeGetType(key) != BloomType) return nullptr;
  return static_cast<ScalingBloomFilter*>(RedisModule_ModuleTypeGetValue(key));
}

static ScalingBloomFilter* MakeFilter(uint64_t cap, double rate,
                                       unsigned expansion, bool fixed) {
  unsigned flg = kUse64Bit | kNoRound;
  if (fixed || expansion == 0) flg |= kFixedSize;
  return ScalingBloomFilter::New(cap, rate, flg, expansion > 0 ? expansion : 2);
}

static ScalingBloomFilter* MakeDefaultFilter() {
  return MakeFilter(g_bloomConfig.defaultCapacity, g_bloomConfig.defaultErrorRate,
                     g_bloomConfig.defaultExpansion, false);
}

// Opens key for read+write, returns existing filter or creates a default one.
// Sets *created=true if a new filter was created.
// Returns nullptr and sends error reply on type mismatch.
static ScalingBloomFilter* OpenOrCreate(RedisModuleCtx* ctx, RedisModuleString* keyName,
                                         RedisModuleKey** outKey, bool* created) {
  *created = false;
  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE));
  *outKey = key;

  int type = RedisModule_KeyType(key);
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    auto* filter = MakeDefaultFilter();
    if (!filter) {
      RedisModule_ReplyWithError(ctx, "ERR failed to allocate bloom filter");
      return nullptr;
    }
    RedisModule_ModuleTypeSetValue(key, BloomType, filter);
    *created = true;
    return filter;
  }

  if (type != REDISMODULE_KEYTYPE_MODULE) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return nullptr;
  }

  auto* filter = GetFilter(key);
  if (!filter) {
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return nullptr;
  }
  return filter;
}

// --- BF.RESERVE ---
static int CmdReserve(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 4) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  double rate;
  if (RedisModule_StringToDouble(argv[2], &rate) != REDISMODULE_OK ||
      rate <= 0.0 || rate >= 1.0) {
    return RedisModule_ReplyWithError(ctx, "ERR error rate must be between 0 and 1 exclusive");
  }

  long long cap;
  if (RedisModule_StringToLongLong(argv[3], &cap) != REDISMODULE_OK || cap <= 0) {
    return RedisModule_ReplyWithError(ctx, "ERR capacity must be a positive integer");
  }

  unsigned expansion = g_bloomConfig.defaultExpansion;
  bool fixed = false;

  for (int i = 4; i < argc; i++) {
    size_t len;
    const char* arg = RedisModule_StringPtrLen(argv[i], &len);
    if (strncasecmp(arg, "EXPANSION", len) == 0) {
      if (++i >= argc) return RedisModule_ReplyWithError(ctx, "ERR EXPANSION requires a value");
      long long val;
      if (RedisModule_StringToLongLong(argv[i], &val) != REDISMODULE_OK || val < 0) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid expansion value");
      }
      expansion = static_cast<unsigned>(val);
    } else if (strncasecmp(arg, "NONSCALING", len) == 0) {
      fixed = true;
    } else {
      return RedisModule_ReplyWithError(ctx, "ERR unrecognized option");
    }
  }

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "ERR key already exists");
  }

  auto* filter = MakeFilter(static_cast<uint64_t>(cap), rate, expansion, fixed);
  if (!filter) {
    return RedisModule_ReplyWithError(ctx, "ERR failed to allocate bloom filter");
  }

  RedisModule_ModuleTypeSetValue(key, BloomType, filter);
  RedisModule_ReplicateVerbatim(ctx);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// --- BF.ADD ---
static int CmdAdd(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  RedisModuleKey* key;
  bool created;
  auto* filter = OpenOrCreate(ctx, argv[1], &key, &created);
  if (!filter) return REDISMODULE_OK;

  size_t len;
  const char* item = RedisModule_StringPtrLen(argv[2], &len);
  int rv = filter->Put(item, len);

  if (rv < 0) {
    return RedisModule_ReplyWithError(ctx, "ERR filter is full and non-scaling");
  }

  if (rv == 1 || created) RedisModule_ReplicateVerbatim(ctx);
  return RedisModule_ReplyWithLongLong(ctx, rv);
}

// --- BF.MADD ---
static int CmdMadd(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  RedisModuleKey* key;
  bool created;
  auto* filter = OpenOrCreate(ctx, argv[1], &key, &created);
  if (!filter) return REDISMODULE_OK;

  int count = argc - 2;
  RedisModule_ReplyWithArray(ctx, count);

  bool changed = created;
  for (int i = 0; i < count; i++) {
    size_t len;
    const char* item = RedisModule_StringPtrLen(argv[i + 2], &len);
    int rv = filter->Put(item, len);
    if (rv < 0) {
      RedisModule_ReplyWithError(ctx, "ERR filter is full and non-scaling");
    } else {
      RedisModule_ReplyWithLongLong(ctx, rv);
      if (rv == 1) changed = true;
    }
  }

  if (changed) RedisModule_ReplicateVerbatim(ctx);
  return REDISMODULE_OK;
}

// --- BF.INSERT ---
static int CmdInsert(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 4) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  double rate = g_bloomConfig.defaultErrorRate;
  long long cap = static_cast<long long>(g_bloomConfig.defaultCapacity);
  unsigned expansion = g_bloomConfig.defaultExpansion;
  bool noCreate = false;
  bool fixed = false;
  int itemsStart = -1;

  for (int i = 2; i < argc; i++) {
    size_t len;
    const char* arg = RedisModule_StringPtrLen(argv[i], &len);

    if (strncasecmp(arg, "ERROR", len) == 0) {
      if (++i >= argc) return RedisModule_WrongArity(ctx);
      if (RedisModule_StringToDouble(argv[i], &rate) != REDISMODULE_OK ||
          rate <= 0.0 || rate >= 1.0) {
        return RedisModule_ReplyWithError(ctx, "ERR error rate must be between 0 and 1 exclusive");
      }
    } else if (strncasecmp(arg, "CAPACITY", len) == 0) {
      if (++i >= argc) return RedisModule_WrongArity(ctx);
      if (RedisModule_StringToLongLong(argv[i], &cap) != REDISMODULE_OK || cap <= 0) {
        return RedisModule_ReplyWithError(ctx, "ERR capacity must be a positive integer");
      }
    } else if (strncasecmp(arg, "EXPANSION", len) == 0) {
      if (++i >= argc) return RedisModule_WrongArity(ctx);
      long long val;
      if (RedisModule_StringToLongLong(argv[i], &val) != REDISMODULE_OK || val < 0) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid expansion value");
      }
      expansion = static_cast<unsigned>(val);
    } else if (strncasecmp(arg, "NOCREATE", len) == 0) {
      noCreate = true;
    } else if (strncasecmp(arg, "NONSCALING", len) == 0) {
      fixed = true;
    } else if (strncasecmp(arg, "ITEMS", len) == 0) {
      itemsStart = i + 1;
      break;
    } else {
      return RedisModule_ReplyWithError(ctx, "ERR unrecognized option");
    }
  }

  if (itemsStart < 0 || itemsStart >= argc) {
    return RedisModule_ReplyWithError(ctx, "ERR missing ITEMS keyword");
  }

  int count = argc - itemsStart;

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));

  ScalingBloomFilter* filter = nullptr;
  int keyType = RedisModule_KeyType(key);

  if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
    if (noCreate) {
      return RedisModule_ReplyWithError(ctx, "ERR key does not exist");
    }
    filter = MakeFilter(static_cast<uint64_t>(cap), rate, expansion, fixed);
    if (!filter) {
      return RedisModule_ReplyWithError(ctx, "ERR failed to allocate bloom filter");
    }
    RedisModule_ModuleTypeSetValue(key, BloomType, filter);
  } else if (keyType == REDISMODULE_KEYTYPE_MODULE) {
    filter = GetFilter(key);
    if (!filter) {
      return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
  } else {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  RedisModule_ReplyWithArray(ctx, count);

  bool changed = false;
  for (int i = 0; i < count; i++) {
    size_t len;
    const char* item = RedisModule_StringPtrLen(argv[itemsStart + i], &len);
    int rv = filter->Put(item, len);
    if (rv < 0) {
      RedisModule_ReplyWithError(ctx, "ERR filter is full and non-scaling");
    } else {
      RedisModule_ReplyWithLongLong(ctx, rv);
      if (rv == 1) changed = true;
    }
  }

  if (changed) RedisModule_ReplicateVerbatim(ctx);
  return REDISMODULE_OK;
}

// --- BF.EXISTS ---
static int CmdExists(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }

  auto* filter = GetFilter(key);
  if (!filter) return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

  size_t len;
  const char* item = RedisModule_StringPtrLen(argv[2], &len);
  return RedisModule_ReplyWithLongLong(ctx, filter->Contains(item, len));
}

// --- BF.MEXISTS ---
static int CmdMexists(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  int count = argc - 2;
  RedisModule_ReplyWithArray(ctx, count);

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    for (int i = 0; i < count; i++) RedisModule_ReplyWithLongLong(ctx, 0);
    return REDISMODULE_OK;
  }

  auto* filter = GetFilter(key);
  if (!filter) {
    for (int i = 0; i < count; i++) {
      RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    return REDISMODULE_OK;
  }

  for (int i = 0; i < count; i++) {
    size_t len;
    const char* item = RedisModule_StringPtrLen(argv[i + 2], &len);
    RedisModule_ReplyWithLongLong(ctx, filter->Contains(item, len));
  }
  return REDISMODULE_OK;
}

// --- BF.INFO ---
// Response field names match the Redis protocol specification for client compatibility.
static int CmdInfo(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc < 2 || argc > 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "ERR key does not exist");
  }

  auto* filter = GetFilter(key);
  if (!filter) return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

  if (argc == 3) {
    size_t len;
    const char* field = RedisModule_StringPtrLen(argv[2], &len);
    if (strncasecmp(field, "Capacity", len) == 0) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->TotalCapacity()));
    } else if (strncasecmp(field, "Size", len) == 0) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->BytesUsed()));
    } else if (strncasecmp(field, "Filters", len) == 0) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->numLayers));
    } else if (strncasecmp(field, "Items", len) == 0) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->totalItems));
    } else if (strncasecmp(field, "Expansion", len) == 0) {
      if (filter->flags & kFixedSize) return RedisModule_ReplyWithNull(ctx);
      return RedisModule_ReplyWithLongLong(ctx, filter->expansionFactor);
    }
    return RedisModule_ReplyWithError(ctx, "ERR unrecognized info field");
  }

  // Full info response. Field labels are part of the BF.INFO protocol spec.
  RedisModule_ReplyWithArray(ctx, 10);
  RedisModule_ReplyWithSimpleString(ctx, "Capacity");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->TotalCapacity()));
  RedisModule_ReplyWithSimpleString(ctx, "Size");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->BytesUsed()));
  RedisModule_ReplyWithSimpleString(ctx, "Number of filters");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->numLayers));
  RedisModule_ReplyWithSimpleString(ctx, "Number of items inserted");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->totalItems));
  RedisModule_ReplyWithSimpleString(ctx, "Expansion rate");
  if (filter->flags & kFixedSize) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    RedisModule_ReplyWithLongLong(ctx, filter->expansionFactor);
  }
  return REDISMODULE_OK;
}

// --- BF.CARD ---
static int CmdCard(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }

  auto* filter = GetFilter(key);
  if (!filter) return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

  return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->totalItems));
}

// --- BF.SCANDUMP ---
static int CmdScandump(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ));

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, "ERR key does not exist");
  }

  auto* filter = GetFilter(key);
  if (!filter) return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

  long long cursor;
  if (RedisModule_StringToLongLong(argv[2], &cursor) != REDISMODULE_OK || cursor < 0) {
    return RedisModule_ReplyWithError(ctx, "ERR invalid cursor value");
  }

  RedisModule_ReplyWithArray(ctx, 2);

  if (cursor == 0) {
    size_t hdrBytes = ComputeHeaderSize(filter);
    auto* hdrBuf = static_cast<char*>(RMAlloc(hdrBytes));
    SerializeHeader(filter, hdrBuf);
    RedisModule_ReplyWithLongLong(ctx, 1);
    RedisModule_ReplyWithStringBuffer(ctx, hdrBuf, hdrBytes);
    RMFree(hdrBuf);
  } else if (cursor >= 1 && static_cast<size_t>(cursor - 1) < filter->numLayers) {
    size_t idx = static_cast<size_t>(cursor - 1);
    const FilterLayer& layer = filter->layers[idx];
    long long nextCursor = (idx + 1 < filter->numLayers) ? cursor + 1 : 0;
    RedisModule_ReplyWithLongLong(ctx, nextCursor);
    RedisModule_ReplyWithStringBuffer(ctx,
                                       reinterpret_cast<const char*>(layer.bloom.bitArray),
                                       layer.bloom.dataSize);
  } else {
    RedisModule_ReplyWithLongLong(ctx, 0);
    RedisModule_ReplyWithStringBuffer(ctx, "", 0);
  }
  return REDISMODULE_OK;
}

// --- BF.LOADCHUNK ---
static int CmdLoadchunk(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);
  RedisModule_AutoMemory(ctx);

  long long cursor;
  if (RedisModule_StringToLongLong(argv[2], &cursor) != REDISMODULE_OK || cursor < 1) {
    return RedisModule_ReplyWithError(ctx, "ERR invalid cursor value");
  }

  size_t dataLen;
  const char* data = RedisModule_StringPtrLen(argv[3], &dataLen);

  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));

  if (cursor == 1) {
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
      RedisModule_DeleteKey(key);
      key = static_cast<RedisModuleKey*>(
        RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE));
    }
    auto* filter = DeserializeHeader(data, dataLen);
    if (!filter) {
      return RedisModule_ReplyWithError(ctx, "ERR malformed header data");
    }
    RedisModule_ModuleTypeSetValue(key, BloomType, filter);
  } else {
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
      return RedisModule_ReplyWithError(ctx, "ERR key does not exist");
    }
    auto* filter = GetFilter(key);
    if (!filter) return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

    size_t idx = static_cast<size_t>(cursor - 2);
    if (idx >= filter->numLayers) {
      return RedisModule_ReplyWithError(ctx, "ERR cursor out of range");
    }
    FilterLayer& layer = filter->layers[idx];
    size_t copyLen = dataLen < layer.bloom.dataSize ? dataLen : layer.bloom.dataSize;
    std::memcpy(layer.bloom.bitArray, data, copyLen);
  }

  RedisModule_ReplicateVerbatim(ctx);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// --- Command Registration ---
int RegisterBloomCommands(RedisModuleCtx* ctx) {
  struct CmdEntry {
    const char* name;
    RedisModuleCmdFunc handler;
    const char* flags;
  };

  CmdEntry commands[] = {
    {"BF.RESERVE",   CmdReserve,   "write deny-oom"},
    {"BF.ADD",       CmdAdd,       "write deny-oom"},
    {"BF.MADD",      CmdMadd,      "write deny-oom"},
    {"BF.INSERT",    CmdInsert,    "write deny-oom"},
    {"BF.EXISTS",    CmdExists,    "readonly"},
    {"BF.MEXISTS",   CmdMexists,   "readonly"},
    {"BF.INFO",      CmdInfo,      "readonly"},
    {"BF.CARD",      CmdCard,      "readonly"},
    {"BF.SCANDUMP",  CmdScandump,  "readonly"},
    {"BF.LOADCHUNK", CmdLoadchunk, "write deny-oom"},
  };

  for (auto& cmd : commands) {
    if (RedisModule_CreateCommand(ctx, cmd.name, cmd.handler, cmd.flags, 1, 1, 1) == REDISMODULE_ERR) {
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}
