#pragma once

#include <cstdint>

#ifndef REDISMODULE_API
#define REDISMODULE_API extern
#define _BF_CFG_API_DEFINED
#endif

extern "C" {
#include "redismodule.h"
}

#ifdef _BF_CFG_API_DEFINED
#undef REDISMODULE_API
#undef _BF_CFG_API_DEFINED
#endif

struct BloomConfig {
  double defaultErrorRate = 0.01;
  uint64_t defaultCapacity = 100;
  unsigned defaultExpansion = 2;
};

extern BloomConfig g_bloomConfig;

int BloomConfigLoad(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
