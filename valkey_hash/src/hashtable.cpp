// hashtable.cpp -- Global state and non-template utilities for the HashTable.
//
// The global state (hash seed, resize policy, abort-shrink flag) is shared
// across all HashTable template instantiations, matching Valkey's design
// where these are process-wide settings.
//
// Since all globals are declared as `inline` in the header (C++17), this file
// exists primarily for explicit instantiations and any future non-template code.

#include "hashtable.h"

namespace cancer_redis {

// The inline globals in detail:: are defined in the header.
// No additional definitions needed here for C++17.

} // namespace cancer_redis
