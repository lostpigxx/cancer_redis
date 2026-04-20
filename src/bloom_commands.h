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
