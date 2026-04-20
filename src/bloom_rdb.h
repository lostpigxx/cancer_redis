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

extern RedisModuleType* BloomType;

// RDB encoding versions for backward compatibility.
// The wire format is dictated by interoperability with existing Redis data.
constexpr int kEncVerWithFlags = 2;
constexpr int kEncVerWithExpansion = 4;
constexpr int kCurrentEncVer = 4;

void* RdbLoadBloom(RedisModuleIO* rdb, int encver);
void RdbSaveBloom(RedisModuleIO* rdb, void* value);
void AofRewriteBloom(RedisModuleIO* aof, RedisModuleString* key, void* value);
void FreeBloom(void* value);
size_t BloomMemUsage(const void* value);
