#include "bloom_config.h"

#include <cstring>

BloomConfig g_bloomConfig;

int BloomConfigLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  for (int i = 0; i < argc; i++) {
    size_t len;
    const char* arg = RedisModule_StringPtrLen(argv[i], &len);

    if (i + 1 < argc) {
      if (strncasecmp(arg, "ERROR_RATE", len) == 0) {
        double val;
        if (RedisModule_StringToDouble(argv[i + 1], &val) != REDISMODULE_OK ||
            val <= 0.0 || val >= 1.0) {
          RedisModule_Log(ctx, "warning", "Invalid ERROR_RATE");
          return REDISMODULE_ERR;
        }
        g_bloomConfig.defaultErrorRate = val;
        i++;
      } else if (strncasecmp(arg, "INITIAL_SIZE", len) == 0) {
        long long val;
        if (RedisModule_StringToLongLong(argv[i + 1], &val) != REDISMODULE_OK ||
            val <= 0) {
          RedisModule_Log(ctx, "warning", "Invalid INITIAL_SIZE");
          return REDISMODULE_ERR;
        }
        g_bloomConfig.defaultCapacity = static_cast<uint64_t>(val);
        i++;
      } else if (strncasecmp(arg, "EXPANSION", len) == 0) {
        long long val;
        if (RedisModule_StringToLongLong(argv[i + 1], &val) != REDISMODULE_OK ||
            val < 0) {
          RedisModule_Log(ctx, "warning", "Invalid EXPANSION");
          return REDISMODULE_ERR;
        }
        g_bloomConfig.defaultExpansion = static_cast<unsigned>(val);
        i++;
      }
    }
  }
  return REDISMODULE_OK;
}
