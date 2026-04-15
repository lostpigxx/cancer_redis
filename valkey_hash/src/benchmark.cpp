// benchmark.cpp -- Performance benchmarks for the cancer_redis HashTable
// under different data distributions.
//
// Usage: ./benchmark [-n count]
//   -n count   Number of entries per benchmark (default: 100000)

#include "hashtable.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

using namespace cancer_redis;

// ============================================================================
// Entry and Traits
// ============================================================================

struct BenchEntry {
  std::string key;
  uint64_t value;
};

struct BenchTraits : DefaultHashTableTraits<BenchEntry, std::string> {
  static const std::string &EntryGetKey(const BenchEntry *e) {
    return e->key;
  }

  static uint64_t Hash(const std::string &key) {
    return SipHash(reinterpret_cast<const uint8_t *>(key.data()),
                   key.size(), detail::g_hash_seed);
  }

  static bool KeyEqual(const std::string &a, const std::string &b) {
    return a == b;
  }

  static constexpr bool kHasEntryDestructor = true;
  static void EntryDestructor(BenchEntry *e) { delete e; }
};

using BenchTable = HashTable<BenchEntry, std::string, BenchTraits>;

// ============================================================================
// Timer
// ============================================================================

struct Timer {
  using Clock = std::chrono::high_resolution_clock;
  Clock::time_point start;

  Timer() : start(Clock::now()) {}

  double ElapsedMs() const {
    auto now = Clock::now();
    return std::chrono::duration<double, std::milli>(now - start).count();
  }
};

// ============================================================================
// Key generators for different distributions
// ============================================================================

// Pre-generate all keys to avoid measuring string allocation during benchmark.

std::vector<std::string> GenSequentialKeys(size_t n) {
  std::vector<std::string> keys;
  keys.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    keys.push_back("key_" + std::to_string(i));
  }
  return keys;
}

std::vector<std::string> GenRandomUniformKeys(size_t n) {
  std::mt19937_64 rng(42);
  std::uniform_int_distribution<uint64_t> dist;
  std::vector<std::string> keys;
  keys.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    keys.push_back("rnd_" + std::to_string(dist(rng)));
  }
  return keys;
}

std::vector<std::string> GenZipfianKeys(size_t n, double skew = 1.0) {
  // Generate keys following Zipfian distribution.
  // A small number of keys appear very frequently.
  size_t universe = n * 10; // key universe is 10x larger
  std::mt19937_64 rng(42);

  // Build CDF for Zipfian
  std::vector<double> cdf(universe);
  double sum = 0.0;
  for (size_t i = 0; i < universe; ++i) {
    sum += 1.0 / std::pow(static_cast<double>(i + 1), skew);
    cdf[i] = sum;
  }
  for (auto &v : cdf) v /= sum;

  std::uniform_real_distribution<double> udist(0.0, 1.0);
  std::vector<std::string> keys;
  keys.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    double r = udist(rng);
    auto it = std::lower_bound(cdf.begin(), cdf.end(), r);
    size_t idx = static_cast<size_t>(it - cdf.begin());
    keys.push_back("zipf_" + std::to_string(idx));
  }
  return keys;
}

std::vector<std::string> GenClusteredKeys(size_t n) {
  // Keys clustered in 8 narrow ranges, causing hash collisions in nearby buckets.
  std::mt19937_64 rng(42);
  constexpr int kClusters = 8;
  std::uniform_int_distribution<int> cluster_dist(0, kClusters - 1);
  std::uniform_int_distribution<uint32_t> offset_dist(0, 99);
  std::vector<std::string> keys;
  keys.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    int c = cluster_dist(rng);
    uint32_t off = offset_dist(rng);
    keys.push_back("cl_" + std::to_string(c) + "_" +
                    std::to_string(i) + "_" + std::to_string(off));
  }
  return keys;
}

std::vector<std::string> GenLargeKeys(size_t n) {
  // 128-byte keys to stress hash computation.
  std::mt19937_64 rng(42);
  std::vector<std::string> keys;
  keys.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    std::string key(128, 'x');
    // Fill with pseudo-random bytes
    auto val = rng();
    std::snprintf(key.data(), key.size(), "large_%zu_%016llx_", i,
                  (unsigned long long)val);
    // Pad remaining with deterministic content
    for (size_t j = std::strlen(key.c_str()); j < 128; ++j) {
      key[j] = 'a' + (j % 26);
    }
    keys.push_back(std::move(key));
  }
  return keys;
}

// ============================================================================
// Helpers
// ============================================================================

void CompleteRehash(BenchTable &ht) {
  while (ht.IsRehashing()) {
    ht.RehashMicroseconds(100000);
  }
}

void PrintHeader() {
  std::printf("%-14s %-16s %10s %12s %14s\n",
              "Distribution", "Operation", "N", "Time(ms)", "Ops/sec");
  std::printf("%-14s %-16s %10s %12s %14s\n",
              "--------------", "----------------", "----------",
              "------------", "--------------");
}

void PrintResult(const char *dist, const char *op, size_t n, double ms) {
  double ops_sec = (ms > 0.0) ? (static_cast<double>(n) / ms * 1000.0) : 0.0;
  std::printf("%-14s %-16s %10zu %10.2f ms %12.0f\n", dist, op, n, ms, ops_sec);
}

// ============================================================================
// Benchmark routines
// ============================================================================

void BenchInsert(const char *dist_name, const std::vector<std::string> &keys) {
  size_t n = keys.size();
  BenchTable ht;
  ht.Expand(n);
  CompleteRehash(ht);

  Timer t;
  for (size_t i = 0; i < n; ++i) {
    ht.Add(new BenchEntry{keys[i], i});
  }
  CompleteRehash(ht);
  PrintResult(dist_name, "Insert", n, t.ElapsedMs());
}

void BenchFindHit(const char *dist_name, const std::vector<std::string> &keys) {
  size_t n = keys.size();
  BenchTable ht;
  for (size_t i = 0; i < n; ++i) {
    ht.Add(new BenchEntry{keys[i], i});
  }
  CompleteRehash(ht);

  // Shuffle lookup order to avoid sequential access pattern bias
  std::vector<size_t> indices(n);
  std::iota(indices.begin(), indices.end(), 0);
  std::mt19937_64 rng(123);
  std::shuffle(indices.begin(), indices.end(), rng);

  Timer t;
  volatile BenchEntry *sink = nullptr;
  for (size_t i = 0; i < n; ++i) {
    sink = ht.Find(keys[indices[i]]);
  }
  (void)sink;
  PrintResult(dist_name, "Find (hit)", n, t.ElapsedMs());
}

void BenchFindMiss(const char *dist_name, const std::vector<std::string> &keys) {
  size_t n = keys.size();
  BenchTable ht;
  for (size_t i = 0; i < n; ++i) {
    ht.Add(new BenchEntry{keys[i], i});
  }
  CompleteRehash(ht);

  // Generate keys that don't exist in the table
  std::vector<std::string> miss_keys;
  miss_keys.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    miss_keys.push_back("MISS_" + keys[i]);
  }

  Timer t;
  volatile BenchEntry *sink = nullptr;
  for (size_t i = 0; i < n; ++i) {
    sink = ht.Find(miss_keys[i]);
  }
  (void)sink;
  PrintResult(dist_name, "Find (miss)", n, t.ElapsedMs());
}

void BenchDelete(const char *dist_name, const std::vector<std::string> &keys) {
  size_t n = keys.size();
  BenchTable ht;
  for (size_t i = 0; i < n; ++i) {
    ht.Add(new BenchEntry{keys[i], i});
  }
  CompleteRehash(ht);

  // Delete in random order
  std::vector<size_t> indices(n);
  std::iota(indices.begin(), indices.end(), 0);
  std::mt19937_64 rng(456);
  std::shuffle(indices.begin(), indices.end(), rng);

  Timer t;
  for (size_t i = 0; i < n; ++i) {
    ht.Remove(keys[indices[i]]);
  }
  PrintResult(dist_name, "Delete", n, t.ElapsedMs());
}

void BenchMixed(const char *dist_name, const std::vector<std::string> &keys) {
  size_t n = keys.size();
  // Pre-fill table with half the keys
  BenchTable ht;
  size_t half = n / 2;
  for (size_t i = 0; i < half; ++i) {
    ht.Add(new BenchEntry{keys[i], i});
  }
  CompleteRehash(ht);

  // Generate mixed workload: 80% reads, 20% writes (insert/delete)
  std::mt19937_64 rng(789);
  std::uniform_int_distribution<int> op_dist(0, 99);
  std::uniform_int_distribution<size_t> key_dist(0, n - 1);
  size_t ops = n;

  Timer t;
  for (size_t i = 0; i < ops; ++i) {
    int op = op_dist(rng);
    size_t idx = key_dist(rng);
    if (op < 80) {
      // Read
      volatile auto *sink = ht.Find(keys[idx]);
      (void)sink;
    } else if (op < 90) {
      // Insert
      ht.Add(new BenchEntry{keys[idx], idx});
    } else {
      // Delete
      ht.Remove(keys[idx]);
    }
  }
  PrintResult(dist_name, "Mixed 80/20", ops, t.ElapsedMs());
}

void BenchScan(const char *dist_name, const std::vector<std::string> &keys) {
  size_t n = keys.size();
  BenchTable ht;
  for (size_t i = 0; i < n; ++i) {
    ht.Add(new BenchEntry{keys[i], i});
  }
  CompleteRehash(ht);

  volatile size_t count = 0;
  Timer t;
  size_t cursor = 0;
  do {
    cursor = ht.Scan(cursor, [&](void *) {
      count++;
    });
  } while (cursor != 0);
  PrintResult(dist_name, "Scan", count, t.ElapsedMs());
}

// ============================================================================
// Run all benchmarks for one distribution
// ============================================================================

void RunDistribution(const char *name, const std::vector<std::string> &keys) {
  BenchInsert(name, keys);
  BenchFindHit(name, keys);
  BenchFindMiss(name, keys);
  BenchDelete(name, keys);
  BenchMixed(name, keys);
  BenchScan(name, keys);
  std::printf("\n");
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
  size_t n = 100000;

  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
      n = static_cast<size_t>(std::atol(argv[++i]));
    }
  }

  // Set a fixed hash seed for reproducibility
  uint8_t seed[16] = {0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                       0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88};
  BenchTable::SetHashFunctionSeed(seed);

  std::printf("cancer_redis HashTable Benchmark\n");
  std::printf("================================\n");
  std::printf("Entries per bucket: %d | Bucket size: %zu bytes\n",
              kEntriesPerBucket, sizeof(Bucket<void *>));
  std::printf("N = %zu\n\n", n);

  PrintHeader();

  // Generate all key sets
  std::printf("Generating keys...\n\n");

  auto seq_keys = GenSequentialKeys(n);
  RunDistribution("Sequential", seq_keys);

  auto rnd_keys = GenRandomUniformKeys(n);
  RunDistribution("Random", rnd_keys);

  auto zipf_keys = GenZipfianKeys(n, 1.0);
  // Deduplicate for insert/delete benchmarks (Zipfian has repeats)
  std::vector<std::string> zipf_unique;
  {
    std::unordered_set<std::string> seen;
    for (auto &k : zipf_keys) {
      if (seen.insert(k).second) zipf_unique.push_back(k);
    }
  }
  std::printf("  [Zipfian: %zu unique keys from %zu generated]\n\n", zipf_unique.size(), n);
  RunDistribution("Zipfian", zipf_unique);

  auto cl_keys = GenClusteredKeys(n);
  RunDistribution("Clustered", cl_keys);

  auto lg_keys = GenLargeKeys(n);
  RunDistribution("Large-key", lg_keys);

  std::printf("===== Benchmark complete =====\n");
  return 0;
}
