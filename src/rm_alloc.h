#pragma once

#include <cstdlib>

#ifdef REDIS_BLOOM_TESTING

inline void* RMAlloc(size_t n) { return malloc(n); }
inline void* RMCalloc(size_t count, size_t size) { return calloc(count, size); }
inline void* RMRealloc(void* p, size_t n) { return realloc(p, n); }
inline void RMFree(void* p) { free(p); }

#else

#ifndef REDISMODULE_API
#define REDISMODULE_API extern
#define _RM_ALLOC_API_DEFINED
#endif

extern "C" {
#include "redismodule.h"
}

#ifdef _RM_ALLOC_API_DEFINED
#undef REDISMODULE_API
#undef _RM_ALLOC_API_DEFINED
#endif

inline void* RMAlloc(size_t n) { return RedisModule_Alloc(n); }
inline void* RMCalloc(size_t count, size_t size) { return RedisModule_Calloc(count, size); }
inline void* RMRealloc(void* p, size_t n) { return RedisModule_Realloc(p, n); }
inline void RMFree(void* p) { RedisModule_Free(p); }

#endif
