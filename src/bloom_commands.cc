#include "bloom_commands.h"
#include "bloom_rdb.h"
#include "bloom_config.h"
#include "sb_chain.h"
#include "rm_alloc.h"

#include <cstring>
#include <string_view>

static ScalingBloomFilter* GetFilter(RedisModuleKey* key) {
  if (RedisModule_ModuleTypeGetType(key) != BloomType) return nullptr;
  return static_cast<ScalingBloomFilter*>(RedisModule_ModuleTypeGetValue(key));
}

static ScalingBloomFilter* AllocFilter(uint64_t cap, double rate,
                                        unsigned expansion, bool fixed) {
  auto flg = BloomFlags::Use64Bit | BloomFlags::NoRound;
  if (fixed || expansion == 0) flg = flg | BloomFlags::FixedSize;
  auto* mem = static_cast<ScalingBloomFilter*>(RMAlloc(sizeof(ScalingBloomFilter)));
  if (!mem) return nullptr;
  new (mem) ScalingBloomFilter(cap, rate, flg, expansion > 0 ? expansion : 2);
  if (!mem->IsValid()) {
    mem->~ScalingBloomFilter();
    RMFree(mem);
    return nullptr;
  }
  return mem;
}

static ScalingBloomFilter* AllocDefaultFilter() {
  return AllocFilter(g_bloomConfig.defaultCapacity, g_bloomConfig.defaultErrorRate,
                      g_bloomConfig.defaultExpansion, false);
}

static ScalingBloomFilter* OpenOrCreate(RedisModuleCtx* ctx, RedisModuleString* keyName,
                                         RedisModuleKey** outKey, bool* created) {
  *created = false;
  auto* key = static_cast<RedisModuleKey*>(
    RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE));
  *outKey = key;

  int type = RedisModule_KeyType(key);
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    auto* filter = AllocDefaultFilter();
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

static bool MatchArg(std::string_view arg, std::string_view target) {
  if (arg.size() != target.size()) return false;
  return strncasecmp(arg.data(), target.data(), arg.size()) == 0;
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
    auto sv = std::string_view{arg, len};

    if (MatchArg(sv, "EXPANSION")) {
      if (++i >= argc) return RedisModule_ReplyWithError(ctx, "ERR EXPANSION requires a value");
      long long val;
      if (RedisModule_StringToLongLong(argv[i], &val) != REDISMODULE_OK || val < 0) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid expansion value");
      }
      expansion = static_cast<unsigned>(val);
    } else if (MatchArg(sv, "NONSCALING")) {
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

  auto* filter = AllocFilter(static_cast<uint64_t>(cap), rate, expansion, fixed);
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
  auto result = filter->Put(AsBytes(item, len));

  if (!result.has_value()) {
    return RedisModule_ReplyWithError(ctx, "ERR filter is full and non-scaling");
  }

  if (*result || created) RedisModule_ReplicateVerbatim(ctx);
  return RedisModule_ReplyWithLongLong(ctx, *result ? 1 : 0);
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
    auto result = filter->Put(AsBytes(item, len));
    if (!result.has_value()) {
      RedisModule_ReplyWithError(ctx, "ERR filter is full and non-scaling");
    } else {
      RedisModule_ReplyWithLongLong(ctx, *result ? 1 : 0);
      if (*result) changed = true;
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
    auto sv = std::string_view{arg, len};

    if (MatchArg(sv, "ERROR")) {
      if (++i >= argc) return RedisModule_WrongArity(ctx);
      if (RedisModule_StringToDouble(argv[i], &rate) != REDISMODULE_OK ||
          rate <= 0.0 || rate >= 1.0) {
        return RedisModule_ReplyWithError(ctx, "ERR error rate must be between 0 and 1 exclusive");
      }
    } else if (MatchArg(sv, "CAPACITY")) {
      if (++i >= argc) return RedisModule_WrongArity(ctx);
      if (RedisModule_StringToLongLong(argv[i], &cap) != REDISMODULE_OK || cap <= 0) {
        return RedisModule_ReplyWithError(ctx, "ERR capacity must be a positive integer");
      }
    } else if (MatchArg(sv, "EXPANSION")) {
      if (++i >= argc) return RedisModule_WrongArity(ctx);
      long long val;
      if (RedisModule_StringToLongLong(argv[i], &val) != REDISMODULE_OK || val < 0) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid expansion value");
      }
      expansion = static_cast<unsigned>(val);
    } else if (MatchArg(sv, "NOCREATE")) {
      noCreate = true;
    } else if (MatchArg(sv, "NONSCALING")) {
      fixed = true;
    } else if (MatchArg(sv, "ITEMS")) {
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
    filter = AllocFilter(static_cast<uint64_t>(cap), rate, expansion, fixed);
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
    auto result = filter->Put(AsBytes(item, len));
    if (!result.has_value()) {
      RedisModule_ReplyWithError(ctx, "ERR filter is full and non-scaling");
    } else {
      RedisModule_ReplyWithLongLong(ctx, *result ? 1 : 0);
      if (*result) changed = true;
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
  return RedisModule_ReplyWithLongLong(ctx, filter->Contains(AsBytes(item, len)) ? 1 : 0);
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
    RedisModule_ReplyWithLongLong(ctx, filter->Contains(AsBytes(item, len)) ? 1 : 0);
  }
  return REDISMODULE_OK;
}

// --- BF.INFO ---
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
    auto sv = std::string_view{field, len};

    if (MatchArg(sv, "Capacity")) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->TotalCapacity()));
    } else if (MatchArg(sv, "Size")) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->BytesUsed()));
    } else if (MatchArg(sv, "Filters")) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->NumLayers()));
    } else if (MatchArg(sv, "Items")) {
      return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->TotalItems()));
    } else if (MatchArg(sv, "Expansion")) {
      if (HasFlag(filter->Flags(), BloomFlags::FixedSize)) return RedisModule_ReplyWithNull(ctx);
      return RedisModule_ReplyWithLongLong(ctx, filter->ExpansionFactor());
    }
    return RedisModule_ReplyWithError(ctx, "ERR unrecognized info field");
  }

  // Full info response
  RedisModule_ReplyWithArray(ctx, 10);
  RedisModule_ReplyWithSimpleString(ctx, "Capacity");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->TotalCapacity()));
  RedisModule_ReplyWithSimpleString(ctx, "Size");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->BytesUsed()));
  RedisModule_ReplyWithSimpleString(ctx, "Number of filters");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->NumLayers()));
  RedisModule_ReplyWithSimpleString(ctx, "Number of items inserted");
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->TotalItems()));
  RedisModule_ReplyWithSimpleString(ctx, "Expansion rate");
  if (HasFlag(filter->Flags(), BloomFlags::FixedSize)) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    RedisModule_ReplyWithLongLong(ctx, filter->ExpansionFactor());
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

  return RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(filter->TotalItems()));
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
    size_t hdrBytes = ComputeHeaderSize(*filter);
    auto* hdrBuf = static_cast<char*>(RMAlloc(hdrBytes));
    SerializeHeader(*filter, hdrBuf);
    RedisModule_ReplyWithLongLong(ctx, 1);
    RedisModule_ReplyWithStringBuffer(ctx, hdrBuf, hdrBytes);
    RMFree(hdrBuf);
  } else if (cursor >= 1 && static_cast<size_t>(cursor - 1) < filter->NumLayers()) {
    size_t idx = static_cast<size_t>(cursor - 1);
    const auto& layer = filter->Layers()[idx];
    long long nextCursor = (idx + 1 < filter->NumLayers()) ? cursor + 1 : 0;
    RedisModule_ReplyWithLongLong(ctx, nextCursor);
    RedisModule_ReplyWithStringBuffer(ctx,
      reinterpret_cast<const char*>(layer.bloom.GetBitArray()),
      layer.bloom.GetDataSize());
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
    if (idx >= filter->NumLayers()) {
      return RedisModule_ReplyWithError(ctx, "ERR cursor out of range");
    }
    auto& layer = filter->Layers()[idx];
    size_t copyLen = std::min(dataLen, static_cast<size_t>(layer.bloom.GetDataSize()));
    std::memcpy(layer.bloom.GetBitArray(), data, copyLen);
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
