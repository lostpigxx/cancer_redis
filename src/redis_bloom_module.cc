// This file defines the RedisModule_* function pointer variables.
// It must be the ONLY file that includes redismodule.h without REDISMODULE_API=extern.
extern "C" {
#include "redismodule.h"
}

#include "bloom_commands.h"
#include "bloom_rdb.h"
#include "bloom_config.h"

extern "C" int RedisModule_OnLoad(RedisModuleCtx* ctx,
                                   RedisModuleString** argv, int argc) {
  if (RedisModule_Init(ctx, "bf", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (BloomConfigLoad(ctx, argv, argc) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  RedisModuleTypeMethods tm = {};
  tm.version = REDISMODULE_TYPE_METHOD_VERSION;
  tm.rdb_load = BFRdbLoad;
  tm.rdb_save = BFRdbSave;
  tm.aof_rewrite = BFAofRewrite;
  tm.free = BFFree;
  tm.mem_usage = BFMemUsage;

  BFType = RedisModule_CreateDataType(ctx, "MBbloom--", kBFCurrentEncver, &tm);
  if (!BFType) {
    RedisModule_Log(ctx, "warning", "Failed to create bloom filter data type");
    return REDISMODULE_ERR;
  }

  if (RegisterBloomCommands(ctx) != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning", "Failed to register bloom filter commands");
    return REDISMODULE_ERR;
  }

  RedisModule_Log(ctx, "notice", "Bloom filter module loaded (C++ rewrite)");
  return REDISMODULE_OK;
}
