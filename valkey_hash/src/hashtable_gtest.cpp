// hashtable_gtest.cpp -- GTest-based unit tests for the cancer_redis HashTable.
// Supplements hashtable_test.cpp with more granular coverage.

#include <gtest/gtest.h>

#include "hashtable.h"

#include <algorithm>
#include <numeric>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace cancer_redis;

// ============================================================================
// Shared test entry and traits
// ============================================================================

struct Entry {
  std::string key;
  int value;
};

struct EntryTraits : DefaultHashTableTraits<Entry, std::string> {
  static const std::string &EntryGetKey(const Entry *e) { return e->key; }

  static uint64_t Hash(const std::string &key) {
    return SipHash(reinterpret_cast<const uint8_t *>(key.data()),
                   key.size(), detail::g_hash_seed);
  }

  static bool KeyEqual(const std::string &a, const std::string &b) {
    return a == b;
  }

  static constexpr bool kHasEntryDestructor = true;
  static void EntryDestructor(Entry *e) { delete e; }
};

using HT = HashTable<Entry, std::string, EntryTraits>;

// Traits WITHOUT destructor (caller manages lifetime)
struct NoDestroyTraits : EntryTraits {
  static constexpr bool kHasEntryDestructor = false;
  static void EntryDestructor(Entry *) {}
};

using HTNoDtor = HashTable<Entry, std::string, NoDestroyTraits>;

// Integer key entry for testing different types
struct IntEntry {
  int key;
  std::string data;
};

struct IntTraits : DefaultHashTableTraits<IntEntry, int> {
  static const int &EntryGetKey(const IntEntry *e) { return e->key; }

  static uint64_t Hash(const int &key) {
    return SipHash(reinterpret_cast<const uint8_t *>(&key),
                   sizeof(key), detail::g_hash_seed);
  }

  static bool KeyEqual(const int &a, const int &b) { return a == b; }

  static constexpr bool kHasEntryDestructor = true;
  static void EntryDestructor(IntEntry *e) { delete e; }
};

using IntHT = HashTable<IntEntry, int, IntTraits>;

// ============================================================================
// Test fixture
// ============================================================================

class HashTableTest : public ::testing::Test {
protected:
  void SetUp() override {
    uint8_t seed[16] = {};
    HT::SetHashFunctionSeed(seed);
    HT::SetResizePolicy(ResizePolicy::Allow);
    HT::SetCanAbortShrink(true);
  }
};

// ============================================================================
// Bucket internals
// ============================================================================

TEST_F(HashTableTest, BucketSizeIs64Bytes) {
  EXPECT_EQ(sizeof(Bucket<void *>), 64u);
  EXPECT_EQ(sizeof(Bucket<Entry>), 64u);
  EXPECT_EQ(sizeof(Bucket<int *>), 64u);
}

TEST_F(HashTableTest, BucketNumPositions) {
  Bucket<Entry> b{};
  EXPECT_EQ(b.NumPositions(), kEntriesPerBucket);
  b.chained = 1;
  EXPECT_EQ(b.NumPositions(), kEntriesPerBucket - 1);
}

TEST_F(HashTableTest, BucketPresenceBits) {
  Bucket<Entry> b{};
  for (int i = 0; i < kEntriesPerBucket; ++i) {
    EXPECT_FALSE(b.IsPositionFilled(i));
  }

  b.SetPositionFilled(0);
  b.SetPositionFilled(3);
  EXPECT_TRUE(b.IsPositionFilled(0));
  EXPECT_FALSE(b.IsPositionFilled(1));
  EXPECT_TRUE(b.IsPositionFilled(3));
  EXPECT_EQ(b.FilledCount(), 2);

  b.ClearPosition(0);
  EXPECT_FALSE(b.IsPositionFilled(0));
  EXPECT_EQ(b.FilledCount(), 1);
}

TEST_F(HashTableTest, BucketFindFreePosition) {
  Bucket<Entry> b{};
  EXPECT_EQ(b.FindFreePosition(), 0);

  for (int i = 0; i < kEntriesPerBucket; ++i) {
    b.SetPositionFilled(i);
  }
  // All full, no chaining -> no free positions
  EXPECT_TRUE(b.IsFull());
  EXPECT_EQ(b.FindFreePosition(), -1);
}

TEST_F(HashTableTest, BucketMoveEntry) {
  Bucket<Entry> src{};
  Bucket<Entry> dst{};
  Entry e{"test", 42};
  Entry *eptr = &e;

  src.entries[2] = eptr;
  src.hashes[2] = 0xAB;
  src.SetPositionFilled(2);

  Bucket<Entry>::MoveEntry(dst, 5, src, 2);
  EXPECT_EQ(dst.entries[5], eptr);
  EXPECT_EQ(dst.hashes[5], 0xAB);
  EXPECT_TRUE(dst.IsPositionFilled(5));
  EXPECT_FALSE(src.IsPositionFilled(2));
}

TEST_F(HashTableTest, BucketReset) {
  Bucket<Entry> b{};
  b.SetPositionFilled(0);
  b.SetPositionFilled(3);
  b.chained = 1;
  b.Reset();
  EXPECT_EQ(b.presence, 0);
  EXPECT_EQ(b.chained, 0);
}

// ============================================================================
// SipHash
// ============================================================================

TEST_F(HashTableTest, SipHashDeterministic) {
  uint8_t key[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  const char *data = "hello world";
  uint64_t h1 = SipHash(reinterpret_cast<const uint8_t *>(data), 11, key);
  uint64_t h2 = SipHash(reinterpret_cast<const uint8_t *>(data), 11, key);
  EXPECT_EQ(h1, h2);
}

TEST_F(HashTableTest, SipHashDifferentInputs) {
  uint8_t key[16] = {};
  uint64_t h1 = SipHash(reinterpret_cast<const uint8_t *>("abc"), 3, key);
  uint64_t h2 = SipHash(reinterpret_cast<const uint8_t *>("abd"), 3, key);
  EXPECT_NE(h1, h2);
}

TEST_F(HashTableTest, SipHashNoCaseEquivalence) {
  uint8_t key[16] = {0x42};
  uint64_t h1 = SipHashNoCase(reinterpret_cast<const uint8_t *>("Hello"), 5, key);
  uint64_t h2 = SipHashNoCase(reinterpret_cast<const uint8_t *>("hELLO"), 5, key);
  EXPECT_EQ(h1, h2);
}

TEST_F(HashTableTest, SipHashNoCaseDifferent) {
  uint8_t key[16] = {};
  uint64_t h1 = SipHashNoCase(reinterpret_cast<const uint8_t *>("abc"), 3, key);
  uint64_t h2 = SipHashNoCase(reinterpret_cast<const uint8_t *>("xyz"), 3, key);
  EXPECT_NE(h1, h2);
}

TEST_F(HashTableTest, SipHashEmptyInput) {
  uint8_t key[16] = {};
  uint64_t h = SipHash(nullptr, 0, key);
  // Should not crash and produce a valid hash
  EXPECT_NE(h, 0u);  // Very unlikely to be exactly 0
}

// ============================================================================
// SIMD MatchHash
// ============================================================================

TEST_F(HashTableTest, SimdMatchHashFindsMatch) {
  uint8_t hashes[kEntriesPerBucket] = {0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70};
  uint8_t presence = 0x7F; // all 7 filled
  auto mask = simd::MatchHash(hashes, 0x30, presence, kEntriesPerBucket);
  EXPECT_EQ(mask & (1 << 2), (1 << 2)); // position 2 matches
  EXPECT_EQ(mask & ~(1 << 2), 0);       // no other matches
}

TEST_F(HashTableTest, SimdMatchHashNoMatch) {
  uint8_t hashes[kEntriesPerBucket] = {0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70};
  uint8_t presence = 0x7F;
  auto mask = simd::MatchHash(hashes, 0xFF, presence, kEntriesPerBucket);
  EXPECT_EQ(mask, 0);
}

TEST_F(HashTableTest, SimdMatchHashRespectsPresence) {
  uint8_t hashes[kEntriesPerBucket] = {0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA};
  // Only positions 1 and 4 are filled
  uint8_t presence = (1 << 1) | (1 << 4);
  auto mask = simd::MatchHash(hashes, 0xAA, presence, kEntriesPerBucket);
  EXPECT_EQ(mask, presence);
}

TEST_F(HashTableTest, SimdMatchHashMultipleMatches) {
  uint8_t hashes[kEntriesPerBucket] = {0x42, 0x00, 0x42, 0x00, 0x42, 0x00, 0x00};
  uint8_t presence = 0x7F;
  auto mask = simd::MatchHash(hashes, 0x42, presence, kEntriesPerBucket);
  EXPECT_EQ(mask, (1 << 0) | (1 << 2) | (1 << 4));
}

// ============================================================================
// Empty table operations
// ============================================================================

TEST_F(HashTableTest, EmptyTableFind) {
  HT ht;
  EXPECT_EQ(ht.Find("anything"), nullptr);
  EXPECT_EQ(ht.FindRef("anything"), nullptr);
}

TEST_F(HashTableTest, EmptyTableRemove) {
  HT ht;
  EXPECT_FALSE(ht.Remove("nothing"));
}

TEST_F(HashTableTest, EmptyTablePop) {
  HT ht;
  EXPECT_EQ(ht.Pop("nothing"), nullptr);
}

TEST_F(HashTableTest, EmptyTableScan) {
  HT ht;
  int count = 0;
  size_t cursor = ht.Scan(0, [&](void *) { count++; });
  EXPECT_EQ(cursor, 0u);
  EXPECT_EQ(count, 0);
}

TEST_F(HashTableTest, EmptyTableIterator) {
  HT ht;
  auto it = ht.GetIterator();
  EXPECT_FALSE(static_cast<bool>(it));
}

TEST_F(HashTableTest, EmptyTableRandomEntry) {
  HT ht;
  EXPECT_EQ(ht.RandomEntry(), nullptr);
}

TEST_F(HashTableTest, EmptyTableSize) {
  HT ht;
  EXPECT_EQ(ht.Size(), 0u);
  EXPECT_TRUE(ht.Empty());
  EXPECT_EQ(ht.MemUsage(), sizeof(HT));
}

// ============================================================================
// Single entry edge cases
// ============================================================================

TEST_F(HashTableTest, SingleEntryLifecycle) {
  HT ht;
  ht.Add(new Entry{"only", 1});
  EXPECT_EQ(ht.Size(), 1u);
  EXPECT_NE(ht.Find("only"), nullptr);

  // Iterator should yield exactly one entry
  {
    auto it = ht.GetIterator();
    ASSERT_TRUE(static_cast<bool>(it));
    EXPECT_EQ((*it)->key, "only");
    ++it;
    EXPECT_FALSE(static_cast<bool>(it));
  }

  // Scan should yield exactly one entry
  int count = 0;
  size_t cursor = 0;
  do {
    cursor = ht.Scan(cursor, [&](void *) { count++; });
  } while (cursor != 0);
  EXPECT_EQ(count, 1);

  ht.Remove("only");
  EXPECT_EQ(ht.Size(), 0u);
  EXPECT_TRUE(ht.Empty());
}

// ============================================================================
// Integer key type
// ============================================================================

TEST_F(HashTableTest, IntegerKeyType) {
  IntHT ht;
  for (int i = 0; i < 200; ++i) {
    ht.Add(new IntEntry{i, "val_" + std::to_string(i)});
  }
  EXPECT_EQ(ht.Size(), 200u);

  for (int i = 0; i < 200; ++i) {
    auto *e = ht.Find(i);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->data, "val_" + std::to_string(i));
  }
  EXPECT_EQ(ht.Find(999), nullptr);
}

// ============================================================================
// No-destructor traits
// ============================================================================

TEST_F(HashTableTest, NoDestructorTraits) {
  std::vector<std::unique_ptr<Entry>> pool;
  {
    HTNoDtor ht;
    for (int i = 0; i < 50; ++i) {
      auto e = std::make_unique<Entry>(Entry{"nd_" + std::to_string(i), i});
      ht.Add(e.get());
      pool.push_back(std::move(e));
    }
    EXPECT_EQ(ht.Size(), 50u);

    // Pop doesn't call destructor either way
    Entry *popped = ht.Pop("nd_0");
    EXPECT_NE(popped, nullptr);
    // We still own it via pool
  }
  // HTNoDtor destructor doesn't call delete, pool handles cleanup
}

// ============================================================================
// Clear with callback
// ============================================================================

TEST_F(HashTableTest, ClearWithCallback) {
  HT ht;
  for (int i = 0; i < 100; ++i) {
    ht.Add(new Entry{"clear_" + std::to_string(i), i});
  }

  bool callback_called = false;
  ht.Clear([&](HT &t) {
    callback_called = true;
    EXPECT_EQ(t.Size(), 0u);
  });
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(ht.Size(), 0u);
}

TEST_F(HashTableTest, ClearThenReuse) {
  HT ht;
  for (int i = 0; i < 100; ++i) {
    ht.Add(new Entry{"first_" + std::to_string(i), i});
  }
  ht.Clear(nullptr);
  EXPECT_EQ(ht.Size(), 0u);

  // Re-populate
  for (int i = 0; i < 50; ++i) {
    ht.Add(new Entry{"second_" + std::to_string(i), i});
  }
  EXPECT_EQ(ht.Size(), 50u);
  EXPECT_NE(ht.Find("second_25"), nullptr);
}

// ============================================================================
// Rehash during operations
// ============================================================================

TEST_F(HashTableTest, FindDuringRehash) {
  HT ht;
  // Insert enough to trigger rehash
  for (int i = 0; i < 500; ++i) {
    ht.Add(new Entry{"rh_" + std::to_string(i), i});
  }

  // Don't complete rehash, verify find still works
  for (int i = 0; i < 500; ++i) {
    EXPECT_NE(ht.Find("rh_" + std::to_string(i)), nullptr);
  }
}

TEST_F(HashTableTest, DeleteDuringRehash) {
  HT ht;
  for (int i = 0; i < 500; ++i) {
    ht.Add(new Entry{"drh_" + std::to_string(i), i});
  }

  // Delete some entries while possibly rehashing
  for (int i = 0; i < 250; ++i) {
    EXPECT_TRUE(ht.Remove("drh_" + std::to_string(i)));
  }

  // Verify remaining
  for (int i = 250; i < 500; ++i) {
    EXPECT_NE(ht.Find("drh_" + std::to_string(i)), nullptr);
  }
}

TEST_F(HashTableTest, RehashMicrosecondsCompletes) {
  HT ht;
  ht.Expand(10000);
  for (int i = 0; i < 1000; ++i) {
    ht.Add(new Entry{"rhms_" + std::to_string(i), i});
  }

  // Rehash with generous time budget should complete
  int result = ht.RehashMicroseconds(1000000); // 1 second
  EXPECT_EQ(result, 0); // 0 = done
  EXPECT_FALSE(ht.IsRehashing());
}

// ============================================================================
// Scan during rehash
// ============================================================================

TEST_F(HashTableTest, ScanDuringRehash) {
  HT ht;
  std::unordered_set<std::string> expected;

  for (int i = 0; i < 500; ++i) {
    std::string key = "scan_rh_" + std::to_string(i);
    ht.Add(new Entry{key, i});
    expected.insert(key);
  }

  // Force rehash to start but don't complete
  ht.Expand(5000);
  EXPECT_TRUE(ht.IsRehashing());

  // Scan should still return all entries
  std::unordered_set<std::string> scanned;
  size_t cursor = 0;
  do {
    cursor = ht.Scan(cursor, [&](void *ptr) {
      auto *e = static_cast<Entry *>(ptr);
      scanned.insert(e->key);
    });
  } while (cursor != 0);

  EXPECT_EQ(scanned, expected);
}

// ============================================================================
// PauseAutoShrink / ResumeAutoShrink
// ============================================================================

TEST_F(HashTableTest, PauseAutoShrink) {
  HT ht;
  for (int i = 0; i < 500; ++i) {
    ht.Add(new Entry{"pas_" + std::to_string(i), i});
  }
  while (ht.IsRehashing()) ht.RehashMicroseconds(100000);

  size_t buckets_before = ht.BucketCount();
  ht.PauseAutoShrink();

  // Delete most entries -- shrink should NOT trigger
  for (int i = 0; i < 490; ++i) {
    ht.Remove("pas_" + std::to_string(i));
  }
  EXPECT_FALSE(ht.IsRehashing());
  EXPECT_EQ(ht.BucketCount(), buckets_before);

  // Resume -- may trigger shrink now
  ht.ResumeAutoShrink();
}

// ============================================================================
// RehashingInfo
// ============================================================================

TEST_F(HashTableTest, RehashingInfoWhenIdle) {
  HT ht;
  for (int i = 0; i < 10; ++i) {
    ht.Add(new Entry{"ri_" + std::to_string(i), i});
  }
  while (ht.IsRehashing()) ht.RehashMicroseconds(100000);

  size_t from, to;
  ht.RehashingInfo(from, to);
  EXPECT_EQ(from, 0u);
  EXPECT_EQ(to, 0u);
}

TEST_F(HashTableTest, RehashingInfoDuringExpand) {
  HT ht;
  for (int i = 0; i < 100; ++i) {
    ht.Add(new Entry{"rie_" + std::to_string(i), i});
  }
  while (ht.IsRehashing()) ht.RehashMicroseconds(100000);

  ht.Expand(10000);
  if (ht.IsRehashing()) {
    size_t from, to;
    ht.RehashingInfo(from, to);
    EXPECT_GT(to, from);
  }
}

// ============================================================================
// BucketCount / ChainedBucketCount
// ============================================================================

TEST_F(HashTableTest, BucketCountGrowsWithEntries) {
  HT ht;
  ht.Add(new Entry{"bc_0", 0});
  while (ht.IsRehashing()) ht.RehashMicroseconds(100000);
  size_t small = ht.BucketCount();

  for (int i = 1; i < 1000; ++i) {
    ht.Add(new Entry{"bc_" + std::to_string(i), i});
  }
  while (ht.IsRehashing()) ht.RehashMicroseconds(100000);
  size_t large = ht.BucketCount();

  EXPECT_GT(large, small);
}

// ============================================================================
// ReplaceReallocatedEntry
// ============================================================================

TEST_F(HashTableTest, ReplaceReallocatedEntry) {
  HTNoDtor ht;
  auto *e1 = new Entry{"replace_me", 1};
  ht.Add(e1);

  auto *e2 = new Entry{"replace_me", 2};
  bool replaced = ht.ReplaceReallocatedEntry(e1, e2);
  EXPECT_TRUE(replaced);

  auto *found = ht.Find("replace_me");
  EXPECT_EQ(found, e2);
  EXPECT_EQ(found->value, 2);

  delete e1;
  delete e2;
}

TEST_F(HashTableTest, ReplaceReallocatedEntryNotFound) {
  HTNoDtor ht;
  auto *e1 = new Entry{"exists", 1};
  ht.Add(e1);

  auto *phantom = new Entry{"ghost", 99};
  auto *replacement = new Entry{"ghost", 100};
  EXPECT_FALSE(ht.ReplaceReallocatedEntry(phantom, replacement));

  delete e1;
  delete phantom;
  delete replacement;
}

// ============================================================================
// Incremental find pipelining
// ============================================================================

TEST_F(HashTableTest, IncrementalFindPipeline) {
  HT ht;
  for (int i = 0; i < 1000; ++i) {
    ht.Add(new Entry{"pipe_" + std::to_string(i), i});
  }
  while (ht.IsRehashing()) ht.RehashMicroseconds(100000);

  // Pipeline 4 lookups simultaneously
  std::string keys[4] = {"pipe_10", "pipe_500", "pipe_999", "pipe_nonexist"};
  HT::IncrementalFindState states[4];

  for (int i = 0; i < 4; ++i) {
    ht.IncrementalFindInit(states[i], keys[i]);
  }

  bool active[4] = {true, true, true, true};
  while (active[0] || active[1] || active[2] || active[3]) {
    for (int i = 0; i < 4; ++i) {
      if (active[i]) {
        active[i] = ht.IncrementalFindStep(states[i]);
      }
    }
  }

  EXPECT_NE(ht.IncrementalFindGetResult(states[0]), nullptr);
  EXPECT_EQ(ht.IncrementalFindGetResult(states[0])->value, 10);
  EXPECT_NE(ht.IncrementalFindGetResult(states[1]), nullptr);
  EXPECT_EQ(ht.IncrementalFindGetResult(states[1])->value, 500);
  EXPECT_NE(ht.IncrementalFindGetResult(states[2]), nullptr);
  EXPECT_EQ(ht.IncrementalFindGetResult(states[2])->value, 999);
  EXPECT_EQ(ht.IncrementalFindGetResult(states[3]), nullptr);
}

// ============================================================================
// Scan completeness (no duplicates, no misses)
// ============================================================================

TEST_F(HashTableTest, ScanNoDuplicates) {
  HT ht;
  const int N = 500;
  for (int i = 0; i < N; ++i) {
    ht.Add(new Entry{"snd_" + std::to_string(i), i});
  }

  std::vector<std::string> scanned;
  size_t cursor = 0;
  do {
    cursor = ht.Scan(cursor, [&](void *ptr) {
      scanned.push_back(static_cast<Entry *>(ptr)->key);
    });
  } while (cursor != 0);

  // Check no duplicates
  std::set<std::string> unique(scanned.begin(), scanned.end());
  EXPECT_EQ(unique.size(), scanned.size());
  EXPECT_EQ(unique.size(), static_cast<size_t>(N));
}

// ============================================================================
// Iterator completeness
// ============================================================================

TEST_F(HashTableTest, IteratorVisitsAllEntries) {
  HT ht;
  const int N = 300;
  std::unordered_set<std::string> expected;
  for (int i = 0; i < N; ++i) {
    std::string k = "itall_" + std::to_string(i);
    ht.Add(new Entry{k, i});
    expected.insert(k);
  }

  std::unordered_set<std::string> seen;
  {
    auto it = ht.GetIterator();
    while (it) {
      seen.insert((*it)->key);
      ++it;
    }
  }
  EXPECT_EQ(seen, expected);
}

// ============================================================================
// Safe iterator: delete all during iteration
// ============================================================================

TEST_F(HashTableTest, SafeIteratorDeleteAll) {
  HT ht;
  for (int i = 0; i < 100; ++i) {
    ht.Add(new Entry{"delall_" + std::to_string(i), i});
  }

  {
    auto it = ht.GetIterator(kIterSafe);
    while (it) {
      Entry *e = *it;
      ++it;
      ht.Remove(e->key);
    }
  }
  EXPECT_EQ(ht.Size(), 0u);
}

// ============================================================================
// Two-phase insert: abort (don't insert after finding position)
// ============================================================================

TEST_F(HashTableTest, TwoPhaseInsertAbort) {
  HT ht;
  ht.Add(new Entry{"existing", 1});

  HT::Position pos;
  bool ok = ht.FindPositionForInsert("new_key", pos);
  EXPECT_TRUE(ok);
  // Intentionally don't call InsertAtPosition
  // The table should still be consistent
  EXPECT_EQ(ht.Size(), 1u);
  EXPECT_NE(ht.Find("existing"), nullptr);
  EXPECT_EQ(ht.Find("new_key"), nullptr);
}

// ============================================================================
// Two-phase pop on non-existent key
// ============================================================================

TEST_F(HashTableTest, TwoPhasePopMiss) {
  HT ht;
  ht.Add(new Entry{"exists", 1});

  HT::Position pos;
  Entry **ref = ht.TwoPhasePopFindRef("nope", pos);
  EXPECT_EQ(ref, nullptr);
  EXPECT_EQ(ht.Size(), 1u);
}

// ============================================================================
// Bucket helper functions
// ============================================================================

TEST_F(HashTableTest, NumBucketsCalculation) {
  EXPECT_EQ(NumBuckets(-1), 0u);
  EXPECT_EQ(NumBuckets(0), 1u);
  EXPECT_EQ(NumBuckets(1), 2u);
  EXPECT_EQ(NumBuckets(4), 16u);
  EXPECT_EQ(NumBuckets(10), 1024u);
}

TEST_F(HashTableTest, ExpToMaskCalculation) {
  EXPECT_EQ(ExpToMask(-1), 0u);
  EXPECT_EQ(ExpToMask(0), 0u);
  EXPECT_EQ(ExpToMask(1), 1u);
  EXPECT_EQ(ExpToMask(4), 15u);
  EXPECT_EQ(ExpToMask(10), 1023u);
}

TEST_F(HashTableTest, NextBucketExpMinimum) {
  EXPECT_EQ(NextBucketExp(0), -1);
  EXPECT_GE(NextBucketExp(1), 0);
  // For any capacity c, NumBuckets(NextBucketExp(c)) * kEntriesPerBucket >= c
  for (size_t c = 1; c <= 10000; c *= 3) {
    int8_t exp = NextBucketExp(c);
    size_t actual_cap = NumBuckets(exp) * kEntriesPerBucket;
    EXPECT_GE(actual_cap, c) << "capacity=" << c;
  }
}

TEST_F(HashTableTest, ReverseBitsRoundTrip) {
  // Double reverse should be identity
  for (size_t v : {0ul, 1ul, 42ul, 12345ul, SIZE_MAX, SIZE_MAX / 2}) {
    EXPECT_EQ(ReverseBits(ReverseBits(v)), v);
  }
}

TEST_F(HashTableTest, NextCursorCoversAllBuckets) {
  // For a table of 16 buckets, NextCursor should visit all 16 indices
  size_t mask = 15; // 16 buckets
  std::set<size_t> visited;
  size_t cursor = 0;
  do {
    visited.insert(cursor & mask);
    cursor = NextCursor(cursor, mask);
  } while (cursor != 0);
  EXPECT_EQ(visited.size(), 16u);
}

// ============================================================================
// HighBits
// ============================================================================

TEST_F(HashTableTest, HighBitsExtraction) {
  EXPECT_EQ(HighBits(0xFF00000000000000ULL), 0xFF);
  EXPECT_EQ(HighBits(0x4200000000000000ULL), 0x42);
  EXPECT_EQ(HighBits(0x00FFFFFFFFFFFFFFULL), 0x00);
}

// ============================================================================
// GenHashFunction / GenCaseHashFunction
// ============================================================================

TEST_F(HashTableTest, GenHashFunctionConsistency) {
  uint64_t h1 = HT::GenHashFunction("test", 4);
  uint64_t h2 = HT::GenHashFunction("test", 4);
  EXPECT_EQ(h1, h2);

  uint64_t h3 = HT::GenHashFunction("Test", 4);
  EXPECT_NE(h1, h3); // case-sensitive
}

TEST_F(HashTableTest, GenCaseHashFunctionIgnoresCase) {
  uint64_t h1 = HT::GenCaseHashFunction("Hello World", 11);
  uint64_t h2 = HT::GenCaseHashFunction("HELLO WORLD", 11);
  uint64_t h3 = HT::GenCaseHashFunction("hello world", 11);
  EXPECT_EQ(h1, h2);
  EXPECT_EQ(h2, h3);
}

// ============================================================================
// Resize policy: Forbid
// ============================================================================

TEST_F(HashTableTest, ResizePolicyForbid) {
  HT ht;
  // First insert triggers initial allocation (allowed even under Forbid)
  ht.Add(new Entry{"seed", 0});
  while (ht.IsRehashing()) ht.RehashMicroseconds(100000);

  HT::SetResizePolicy(ResizePolicy::Forbid);

  // Under Forbid, expand should fail
  bool expanded = ht.Expand(10000);
  EXPECT_FALSE(expanded);

  HT::SetResizePolicy(ResizePolicy::Allow);
}

// ============================================================================
// Move semantics: moved-from state
// ============================================================================

TEST_F(HashTableTest, MoveConstructorLeavesSourceEmpty) {
  HT ht1;
  for (int i = 0; i < 50; ++i) {
    ht1.Add(new Entry{"mc_" + std::to_string(i), i});
  }

  HT ht2(std::move(ht1));
  EXPECT_EQ(ht1.Size(), 0u);
  EXPECT_TRUE(ht1.Empty());
  EXPECT_EQ(ht1.Find("mc_0"), nullptr);

  EXPECT_EQ(ht2.Size(), 50u);
  EXPECT_NE(ht2.Find("mc_25"), nullptr);
}

TEST_F(HashTableTest, MoveAssignmentCleansTarget) {
  HT ht1, ht2;
  for (int i = 0; i < 30; ++i) {
    ht1.Add(new Entry{"ma1_" + std::to_string(i), i});
  }
  for (int i = 0; i < 20; ++i) {
    ht2.Add(new Entry{"ma2_" + std::to_string(i), i});
  }

  ht2 = std::move(ht1);
  EXPECT_EQ(ht2.Size(), 30u);
  EXPECT_NE(ht2.Find("ma1_15"), nullptr);
  EXPECT_EQ(ht2.Find("ma2_10"), nullptr); // old ht2 entries gone
}

// ============================================================================
// Bucket chain stress (force many collisions)
// ============================================================================

TEST_F(HashTableTest, BucketChainStress) {
  // Use a small pre-allocated table to force bucket overflow
  HT ht;
  // Don't pre-expand, let it start small and chain
  for (int i = 0; i < 200; ++i) {
    ht.Add(new Entry{"chain_" + std::to_string(i), i});
  }

  // Verify all entries accessible
  for (int i = 0; i < 200; ++i) {
    auto *e = ht.Find("chain_" + std::to_string(i));
    ASSERT_NE(e, nullptr) << "Missing key chain_" << i;
    EXPECT_EQ(e->value, i);
  }

  // Delete in reverse order (stresses chain compaction)
  for (int i = 199; i >= 0; --i) {
    EXPECT_TRUE(ht.Remove("chain_" + std::to_string(i)));
  }
  EXPECT_EQ(ht.Size(), 0u);
}

// ============================================================================
// Interleaved insert and delete
// ============================================================================

TEST_F(HashTableTest, InterleavedInsertDelete) {
  HT ht;
  const int N = 1000;

  // Insert first half
  for (int i = 0; i < N / 2; ++i) {
    ht.Add(new Entry{"ild_" + std::to_string(i), i});
  }

  // Interleave: delete old, insert new
  for (int i = 0; i < N / 2; ++i) {
    ht.Remove("ild_" + std::to_string(i));
    ht.Add(new Entry{"ild_" + std::to_string(N / 2 + i), N / 2 + i});
  }

  EXPECT_EQ(ht.Size(), static_cast<size_t>(N / 2));
  for (int i = 0; i < N / 2; ++i) {
    EXPECT_EQ(ht.Find("ild_" + std::to_string(i)), nullptr);
  }
  for (int i = N / 2; i < N; ++i) {
    EXPECT_NE(ht.Find("ild_" + std::to_string(i)), nullptr);
  }
}

// ============================================================================
// EntriesPerBucket constant
// ============================================================================

TEST_F(HashTableTest, EntriesPerBucketValue) {
#if UINTPTR_MAX == UINT64_MAX
  EXPECT_EQ(HT::EntriesPerBucket(), 7u);
#elif UINTPTR_MAX == UINT32_MAX
  EXPECT_EQ(HT::EntriesPerBucket(), 12u);
#endif
}

// ============================================================================
// Hash seed affects hashing
// ============================================================================

TEST_F(HashTableTest, HashSeedAffectsResults) {
  uint8_t seed1[16] = {1};
  HT::SetHashFunctionSeed(seed1);
  uint64_t h1 = HT::GenHashFunction("test", 4);

  uint8_t seed2[16] = {2};
  HT::SetHashFunctionSeed(seed2);
  uint64_t h2 = HT::GenHashFunction("test", 4);

  EXPECT_NE(h1, h2);

  // Reset
  uint8_t zero[16] = {};
  HT::SetHashFunctionSeed(zero);
}

// ============================================================================
// Metadata access
// ============================================================================

TEST_F(HashTableTest, MetadataAccessible) {
  HT ht;
  void *meta = ht.Metadata();
  const void *cmeta = static_cast<const HT &>(ht).Metadata();
  EXPECT_NE(meta, nullptr);
  EXPECT_EQ(meta, cmeta);
}

// ============================================================================
// GetStatsHt
// ============================================================================

TEST_F(HashTableTest, GetStatsHtValues) {
  HT ht;
  for (int i = 0; i < 100; ++i) {
    ht.Add(new Entry{"st_" + std::to_string(i), i});
  }
  while (ht.IsRehashing()) ht.RehashMicroseconds(100000);

  auto stats = ht.GetStatsHt(0, true);
  EXPECT_EQ(stats.used, 100u);
  EXPECT_GT(stats.toplevel_buckets, 0u);
  EXPECT_EQ(stats.capacity, stats.toplevel_buckets * kEntriesPerBucket);
}

// ============================================================================
// Expand to exact size
// ============================================================================

TEST_F(HashTableTest, ExpandPreallocates) {
  HT ht;
  bool ok = ht.Expand(10000);
  EXPECT_TRUE(ok);
  // After expand + immediate rehash completion, we should have enough capacity
  while (ht.IsRehashing()) ht.RehashMicroseconds(100000);

  // Insert 10000 entries without triggering another rehash
  for (int i = 0; i < 10000; ++i) {
    ht.Add(new Entry{"exp_" + std::to_string(i), i});
  }
  // If we pre-expanded properly, no rehash should have started during inserts
  // (or it should have completed immediately)
}

// ============================================================================
// SampleEntries returns correct count
// ============================================================================

TEST_F(HashTableTest, SampleEntriesCount) {
  HT ht;
  for (int i = 0; i < 100; ++i) {
    ht.Add(new Entry{"smp_" + std::to_string(i), i});
  }

  Entry *samples[20];
  unsigned n = ht.SampleEntries(samples, 20);
  EXPECT_EQ(n, 20u);

  // All returned entries should be valid
  for (unsigned i = 0; i < n; ++i) {
    EXPECT_NE(samples[i], nullptr);
    EXPECT_TRUE(samples[i]->key.substr(0, 4) == "smp_");
  }
}

TEST_F(HashTableTest, SampleEntriesMoreThanSize) {
  HT ht;
  for (int i = 0; i < 5; ++i) {
    ht.Add(new Entry{"few_" + std::to_string(i), i});
  }

  Entry *samples[20];
  unsigned n = ht.SampleEntries(samples, 20);
  // Should return at most what's available (with retries may get duplicates)
  EXPECT_GT(n, 0u);
}
