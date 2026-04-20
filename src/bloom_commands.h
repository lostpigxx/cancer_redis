#pragma once

#ifndef REDISMODULE_API
#define REDISMODULE_API extern
#define _BF_CMD_API_DEFINED
#endif

extern "C" {
#include "redismodule.h"
}

#ifdef _BF_CMD_API_DEFINED
#undef REDISMODULE_API
#undef _BF_CMD_API_DEFINED
#endif

int RegisterBloomCommands(RedisModuleCtx* ctx);

int BFReserveCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
int BFAddCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
int BFMaddCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
int BFInsertCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
int BFExistsCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
int BFMexistsCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
int BFInfoCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
int BFCardCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
int BFScandumpCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
int BFLoadchunkCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
