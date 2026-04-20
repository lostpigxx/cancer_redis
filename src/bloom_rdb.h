#pragma once

#ifndef REDISMODULE_API
#define REDISMODULE_API extern
#define _BF_RDB_API_DEFINED
#endif

extern "C" {
#include "redismodule.h"
}

#ifdef _BF_RDB_API_DEFINED
#undef REDISMODULE_API
#undef _BF_RDB_API_DEFINED
#endif

extern RedisModuleType* BFType;

constexpr int kBFMinOptionsEnc = 2;
constexpr int kBFMinGrowthEnc = 4;
constexpr int kBFCurrentEncver = 4;

void* BFRdbLoad(RedisModuleIO* rdb, int encver);
void BFRdbSave(RedisModuleIO* rdb, void* value);
void BFAofRewrite(RedisModuleIO* aof, RedisModuleString* key, void* value);
void BFFree(void* value);
size_t BFMemUsage(const void* value);
