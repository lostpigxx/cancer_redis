// Quick compile-and-run test for the HashTable implementation.
// Tests basic CRUD, rehashing, iteration, scan, incremental find, two-phase ops.

#include "hashtable.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

using namespace cancer_redis;

// ============================================================================
// Test entry: a simple key-value pair
// ============================================================================

struct TestEntry {
  std::string key;
  int value;
};

struct TestTraits : DefaultHashTableTraits<TestEntry, std::string> {
  static const std::string &EntryGetKey(const TestEntry *entry) {
    return entry->key;
  }

  static uint64_t Hash(const std::string &key) {
    return SipHash(reinterpret_cast<const uint8_t *>(key.data()),
                   key.size(), detail::g_hash_seed);
  }

  static bool KeyEqual(const std::string &a, const std::string &b) {
    return a == b;
  }

  static constexpr bool kHasEntryDestructor = true;
  static void EntryDestructor(TestEntry *e) { delete e; }
};

using TestHashTable = HashTable<TestEntry, std::string, TestTraits>;

// ============================================================================
// Test helpers
// ============================================================================

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                                          \
  static void test_##name();                                \
  struct TestReg_##name {                                   \
    TestReg_##name() {                                      \
      std::printf("  %-50s", #name);                        \
      try {                                                 \
        test_##name();                                      \
        std::printf("PASS\n");                              \
        tests_passed++;                                     \
      } catch (...) {                                       \
        std::printf("FAIL\n");                              \
        tests_failed++;                                     \
      }                                                     \
    }                                                       \
  } test_reg_##name;                                        \
  static void test_##name()

#define ASSERT(cond)                                                         \
  do {                                                                       \
    if (!(cond)) {                                                           \
      std::fprintf(stderr, "  ASSERTION FAILED: %s (line %d)\n", #cond,     \
                   __LINE__);                                                \
      throw std::runtime_error("assertion failed");                          \
    }                                                                        \
  } while (0)

// ============================================================================
// Tests
// ============================================================================

TEST(create_empty) {
  TestHashTable ht;
  ASSERT(ht.Size() == 0);
  ASSERT(ht.Empty());
  ASSERT(ht.Find("nonexistent") == nullptr);
}

TEST(add_and_find) {
  TestHashTable ht;
  auto *e1 = new TestEntry{"hello", 1};
  auto *e2 = new TestEntry{"world", 2};

  ASSERT(ht.Add(e1));
  ASSERT(ht.Add(e2));
  ASSERT(ht.Size() == 2);

  auto *found = ht.Find("hello");
  ASSERT(found != nullptr);
  ASSERT(found->value == 1);

  found = ht.Find("world");
  ASSERT(found != nullptr);
  ASSERT(found->value == 2);

  ASSERT(ht.Find("missing") == nullptr);
}

TEST(add_duplicate) {
  TestHashTable ht;
  auto *e1 = new TestEntry{"key", 1};
  auto *e2 = new TestEntry{"key", 2};

  ASSERT(ht.Add(e1));
  ASSERT(!ht.Add(e2));
  ASSERT(ht.Size() == 1);

  delete e2;
}

TEST(add_or_find) {
  TestHashTable ht;
  auto *e1 = new TestEntry{"key", 1};
  auto *e2 = new TestEntry{"key", 2};

  TestEntry *existing = nullptr;
  ASSERT(ht.AddOrFind(e1));
  ASSERT(!ht.AddOrFind(e2, &existing));
  ASSERT(existing == e1);
  ASSERT(existing->value == 1);

  delete e2;
}

TEST(remove) {
  TestHashTable ht;
  auto *e1 = new TestEntry{"a", 1};
  auto *e2 = new TestEntry{"b", 2};

  ht.Add(e1);
  ht.Add(e2);
  ASSERT(ht.Size() == 2);

  ASSERT(ht.Remove("a"));
  ASSERT(ht.Size() == 1);
  ASSERT(ht.Find("a") == nullptr);
  ASSERT(ht.Find("b") != nullptr);

  ASSERT(!ht.Remove("nonexistent"));
}

TEST(pop) {
  TestHashTable ht;
  auto *e1 = new TestEntry{"key", 42};
  ht.Add(e1);

  TestEntry *popped = ht.Pop("key");
  ASSERT(popped != nullptr);
  ASSERT(popped->value == 42);
  ASSERT(ht.Size() == 0);

  delete popped;
}

TEST(many_entries_trigger_rehash) {
  TestHashTable ht;
  const int N = 1000;
  std::vector<TestEntry *> entries;

  for (int i = 0; i < N; ++i) {
    auto *e = new TestEntry{"key_" + std::to_string(i), i};
    entries.push_back(e);
    ASSERT(ht.Add(e));
  }

  ASSERT(ht.Size() == N);

  for (int i = 0; i < N; ++i) {
    auto *found = ht.Find("key_" + std::to_string(i));
    ASSERT(found != nullptr);
    ASSERT(found->value == i);
  }

  for (int i = 0; i < N / 2; ++i) {
    ASSERT(ht.Remove("key_" + std::to_string(i)));
  }
  ASSERT(ht.Size() == N / 2);

  for (int i = N / 2; i < N; ++i) {
    ASSERT(ht.Find("key_" + std::to_string(i)) != nullptr);
  }
}

TEST(iterator_basic) {
  TestHashTable ht;
  const int N = 100;
  std::unordered_set<std::string> keys;

  for (int i = 0; i < N; ++i) {
    auto *e = new TestEntry{"iter_" + std::to_string(i), i};
    ht.Add(e);
    keys.insert(e->key);
  }

  std::unordered_set<std::string> seen;
  auto it = ht.GetIterator();
  while (it) {
    TestEntry *e = *it;
    seen.insert(e->key);
    ++it;
  }

  ASSERT(seen == keys);
}

TEST(safe_iterator_with_delete) {
  TestHashTable ht;
  const int N = 50;

  for (int i = 0; i < N; ++i) {
    auto *e = new TestEntry{"safe_" + std::to_string(i), i};
    ht.Add(e);
  }

  {
    auto it = ht.GetIterator(kIterSafe);
    while (it) {
      TestEntry *e = *it;
      ++it;
      if (e->value % 2 == 0) {
        ht.Remove(e->key);
      }
    }
  }

  ASSERT(ht.Size() == N / 2);
  for (int i = 0; i < N; ++i) {
    auto *found = ht.Find("safe_" + std::to_string(i));
    if (i % 2 == 0)
      ASSERT(found == nullptr);
    else
      ASSERT(found != nullptr);
  }
}

TEST(scan_basic) {
  TestHashTable ht;
  const int N = 200;
  std::unordered_set<std::string> keys;

  for (int i = 0; i < N; ++i) {
    auto *e = new TestEntry{"scan_" + std::to_string(i), i};
    ht.Add(e);
    keys.insert(e->key);
  }

  std::unordered_set<std::string> scanned;
  size_t cursor = 0;
  do {
    cursor = ht.Scan(cursor, [&](void *entry_ptr) {
      auto *e = static_cast<TestEntry *>(entry_ptr);
      scanned.insert(e->key);
    });
  } while (cursor != 0);

  ASSERT(scanned == keys);
}

TEST(two_phase_insert) {
  TestHashTable ht;
  auto *e1 = new TestEntry{"existing", 1};
  ht.Add(e1);

  TestHashTable::Position pos;
  TestEntry *existing = nullptr;
  ASSERT(!ht.FindPositionForInsert("existing", pos, &existing));
  ASSERT(existing == e1);

  ASSERT(ht.FindPositionForInsert("new_key", pos));
  auto *e2 = new TestEntry{"new_key", 2};
  ht.InsertAtPosition(e2, pos);

  ASSERT(ht.Size() == 2);
  ASSERT(ht.Find("new_key")->value == 2);
}

TEST(two_phase_pop) {
  TestHashTable ht;
  auto *e1 = new TestEntry{"popme", 99};
  ht.Add(e1);

  TestHashTable::Position pos;
  TestEntry **ref = ht.TwoPhasePopFindRef("popme", pos);
  ASSERT(ref != nullptr);
  ASSERT((*ref)->value == 99);

  TestEntry *saved = *ref;
  ht.TwoPhasePopDelete(pos);
  ASSERT(ht.Size() == 0);

  delete saved;
}

TEST(incremental_find) {
  TestHashTable ht;
  auto *e1 = new TestEntry{"incr_find", 42};
  ht.Add(e1);

  TestHashTable::IncrementalFindState state;
  std::string key = "incr_find";
  ht.IncrementalFindInit(state, key);

  while (ht.IncrementalFindStep(state)) {}

  TestEntry *result = ht.IncrementalFindGetResult(state);
  ASSERT(result != nullptr);
  ASSERT(result->value == 42);

  std::string missing = "missing";
  ht.IncrementalFindInit(state, missing);
  while (ht.IncrementalFindStep(state)) {}
  ASSERT(ht.IncrementalFindGetResult(state) == nullptr);
}

TEST(find_ref) {
  TestHashTable ht;
  auto *e1 = new TestEntry{"ref_test", 10};
  ht.Add(e1);

  TestEntry **ref = ht.FindRef("ref_test");
  ASSERT(ref != nullptr);
  ASSERT((*ref)->value == 10);

  auto *e2 = new TestEntry{"ref_test", 20};
  delete *ref;
  *ref = e2;

  auto *found = ht.Find("ref_test");
  ASSERT(found != nullptr);
  ASSERT(found->value == 20);
}

TEST(random_entry) {
  TestHashTable ht;
  for (int i = 0; i < 50; ++i) {
    ht.Add(new TestEntry{"rand_" + std::to_string(i), i});
  }

  TestEntry *e = ht.RandomEntry();
  ASSERT(e != nullptr);
  ASSERT(e->key.substr(0, 5) == "rand_");

  TestEntry *samples[10];
  unsigned n = ht.SampleEntries(samples, 10);
  ASSERT(n > 0);
}

TEST(stats) {
  TestHashTable ht;
  for (int i = 0; i < 100; ++i) {
    ht.Add(new TestEntry{"stat_" + std::to_string(i), i});
  }

  std::string stats = ht.GetStats(true);
  ASSERT(!stats.empty());
  ASSERT(stats.find("number of entries: 100") != std::string::npos);
}

TEST(mem_usage) {
  TestHashTable ht;
  size_t base = ht.MemUsage();

  for (int i = 0; i < 100; ++i) {
    ht.Add(new TestEntry{"mem_" + std::to_string(i), i});
  }

  ASSERT(ht.MemUsage() > base);
}

TEST(move_semantics) {
  TestHashTable ht1;
  for (int i = 0; i < 20; ++i) {
    ht1.Add(new TestEntry{"move_" + std::to_string(i), i});
  }
  ASSERT(ht1.Size() == 20);

  TestHashTable ht2(std::move(ht1));
  ASSERT(ht2.Size() == 20);
  ASSERT(ht1.Size() == 0);

  TestHashTable ht3;
  ht3 = std::move(ht2);
  ASSERT(ht3.Size() == 20);
  ASSERT(ht2.Size() == 0);

  ASSERT(ht3.Find("move_5") != nullptr);
}

TEST(resize_policy) {
  TestHashTable::SetResizePolicy(ResizePolicy::Avoid);
  {
    TestHashTable ht;
    for (int i = 0; i < 100; ++i) {
      ht.Add(new TestEntry{"policy_" + std::to_string(i), i});
    }
    ASSERT(ht.Size() == 100);
  }
  TestHashTable::SetResizePolicy(ResizePolicy::Allow);
}

TEST(expand_and_shrink) {
  TestHashTable ht;
  ht.Expand(1000);

  for (int i = 0; i < 100; ++i) {
    ht.Add(new TestEntry{"es_" + std::to_string(i), i});
  }
  ASSERT(ht.Size() == 100);

  while (ht.IsRehashing()) {
    ht.RehashMicroseconds(1000);
  }

  for (int i = 0; i < 95; ++i) {
    ht.Remove("es_" + std::to_string(i));
  }

  while (ht.IsRehashing()) {
    ht.RehashMicroseconds(1000);
  }

  ASSERT(ht.Size() == 5);
}

TEST(hash_seed) {
  uint8_t seed[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  TestHashTable::SetHashFunctionSeed(seed);

  const uint8_t *got = TestHashTable::GetHashSeed();
  for (int i = 0; i < 16; ++i) {
    ASSERT(got[i] == seed[i]);
  }

  uint8_t zero[16] = {};
  TestHashTable::SetHashFunctionSeed(zero);
}

TEST(gen_hash_function) {
  const char *data = "test string";
  uint64_t h1 = TestHashTable::GenHashFunction(data, strlen(data));
  uint64_t h2 = TestHashTable::GenHashFunction(data, strlen(data));
  ASSERT(h1 == h2);

  uint64_t h3 = TestHashTable::GenHashFunction("different", 9);
  ASSERT(h1 != h3);
}

TEST(bucket_size_static_assert) {
  ASSERT(sizeof(Bucket<void *>) == 64);
  ASSERT(sizeof(Bucket<TestEntry *>) == 64);
}

TEST(large_scale_stress) {
  TestHashTable ht;
  const int N = 10000;

  for (int i = 0; i < N; ++i) {
    ht.Add(new TestEntry{"stress_" + std::to_string(i), i});
  }
  ASSERT(ht.Size() == N);

  while (ht.IsRehashing()) {
    ht.RehashMicroseconds(10000);
  }

  for (int i = 0; i < N; ++i) {
    ASSERT(ht.Find("stress_" + std::to_string(i)) != nullptr);
  }

  for (int i = 0; i < N; i += 3) {
    ht.Remove("stress_" + std::to_string(i));
  }

  while (ht.IsRehashing()) {
    ht.RehashMicroseconds(10000);
  }

  for (int i = 0; i < N; ++i) {
    auto *found = ht.Find("stress_" + std::to_string(i));
    if (i % 3 == 0)
      ASSERT(found == nullptr);
    else
      ASSERT(found != nullptr);
  }
}

// ============================================================================
// Main
// ============================================================================

int main() {
  std::printf("HashTable C++ Test Suite\n");
  std::printf("========================\n");
  std::printf("Bucket size: %zu bytes\n", sizeof(Bucket<void *>));
  std::printf("Entries per bucket: %d\n", kEntriesPerBucket);
  std::printf("\n");

  std::printf("\n========================\n");
  std::printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);

  return tests_failed > 0 ? 1 : 0;
}
