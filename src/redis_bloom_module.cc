// Module entry point for the bloom filter Redis module.
// Registers data type and BF.* commands.
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

  // Type name "MBbloom--" and encoding version 4 are required for
  // interoperability with existing Redis RDB files that contain
  // bloom filter data. This is a wire-format compatibility requirement,
  // not derived from the RedisBloom source code.
  RedisModuleTypeMethods tm = {};
  tm.version = REDISMODULE_TYPE_METHOD_VERSION;
  tm.rdb_load = RdbLoadBloom;
  tm.rdb_save = RdbSaveBloom;
  tm.aof_rewrite = AofRewriteBloom;
  tm.free = FreeBloom;
  tm.mem_usage = BloomMemUsage;

  BloomType = RedisModule_CreateDataType(ctx, "MBbloom--", kCurrentEncVer, &tm);
  if (!BloomType) {
    RedisModule_Log(ctx, "warning", "Failed to register bloom filter data type");
    return REDISMODULE_ERR;
  }

  if (RegisterBloomCommands(ctx) != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning", "Failed to register bloom filter commands");
    return REDISMODULE_ERR;
  }

  RedisModule_Log(ctx, "notice", "Bloom filter module loaded");
  return REDISMODULE_OK;
}
