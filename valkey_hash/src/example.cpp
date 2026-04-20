// example.cpp -- Usage examples for the cancer_redis HashTable.
// Demonstrates: Traits, CRUD, iteration, scan, two-phase ops,
// incremental find, rehash control, random sampling, statistics.

#include "hashtable.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

using namespace cancer_redis;

// ============================================================================
// 1. Define your entry type and traits
// ============================================================================

// A simple key-value entry. The hashtable stores Entry* pointers.
struct User {
  std::string name;  // key
  int age;
  std::string email;
};

// Traits tell the hashtable how to hash, compare, and manage entries.
struct UserTraits : DefaultHashTableTraits<User, std::string> {
  // Required: extract the key from an entry
  static const std::string &EntryGetKey(const User *u) {
    return u->name;
  }

  // Required: hash a key
  static uint64_t Hash(const std::string &key) {
    return SipHash(reinterpret_cast<const uint8_t *>(key.data()),
                   key.size(), detail::g_hash_seed);
  }

  // Required: compare two keys
  static bool KeyEqual(const std::string &a, const std::string &b) {
    return a == b;
  }

  // Optional: destructor called on Remove()
  static constexpr bool kHasEntryDestructor = true;
  static void EntryDestructor(User *u) { delete u; }
};

using UserTable = HashTable<User, std::string, UserTraits>;

// ============================================================================
// Helper
// ============================================================================

void PrintSeparator(const char *title) {
  std::printf("\n========== %s ==========\n", title);
}

// ============================================================================
// Examples
// ============================================================================

void BasicCrud() {
  PrintSeparator("Basic CRUD");
  UserTable ht;

  // Add entries
  ht.Add(new User{"Alice", 30, "alice@example.com"});
  ht.Add(new User{"Bob", 25, "bob@example.com"});
  ht.Add(new User{"Charlie", 35, "charlie@example.com"});
  std::printf("Size after 3 inserts: %zu\n", ht.Size());

  // Find
  User *u = ht.Find("Bob");
  if (u) {
    std::printf("Found Bob: age=%d, email=%s\n", u->age, u->email.c_str());
  }

  // Find missing key
  User *missing = ht.Find("Dave");
  std::printf("Find 'Dave': %s\n", missing ? "found" : "not found");

  // AddOrFind: add if not exists, get existing if it does
  User *existing = nullptr;
  auto *dave = new User{"Dave", 28, "dave@example.com"};
  bool added = ht.AddOrFind(dave, &existing);
  std::printf("AddOrFind 'Dave': %s, size=%zu\n",
              added ? "added" : "already exists", ht.Size());

  // Try adding duplicate
  auto *alice2 = new User{"Alice", 99, "fake@example.com"};
  added = ht.AddOrFind(alice2, &existing);
  std::printf("AddOrFind 'Alice' again: %s, existing age=%d\n",
              added ? "added" : "already exists", existing->age);
  delete alice2;  // not added, we own it

  // Remove
  bool removed = ht.Remove("Bob");
  std::printf("Remove 'Bob': %s, size=%zu\n",
              removed ? "ok" : "not found", ht.Size());

  // Pop: remove without calling destructor, caller takes ownership
  User *popped = ht.Pop("Charlie");
  if (popped) {
    std::printf("Popped Charlie: age=%d\n", popped->age);
    delete popped;  // we own it now
  }

  std::printf("Final size: %zu\n", ht.Size());
  // Remaining entries (Alice, Dave) are freed by destructor via Clear
}

void IteratorExample() {
  PrintSeparator("Iterators");
  UserTable ht;

  for (int i = 0; i < 10; ++i) {
    ht.Add(new User{
      "user_" + std::to_string(i),
      20 + i,
      "user" + std::to_string(i) + "@test.com"
    });
  }

  // Basic iterator (unsafe - don't modify the table during iteration)
  // Scope it so it's destroyed before we modify the table with a safe iterator.
  std::printf("All users:\n");
  {
    auto it = ht.GetIterator();
    while (it) {
      User *u = *it;
      std::printf("  %s (age %d)\n", u->name.c_str(), u->age);
      ++it;
    }
  }

  // Safe iterator - allows deletion during iteration
  std::printf("\nDeleting users with even age...\n");
  {
    auto safe_it = ht.GetIterator(kIterSafe);
    while (safe_it) {
      User *u = *safe_it;
      ++safe_it;  // advance BEFORE modifying the table
      if (u->age % 2 == 0) {
        ht.Remove(u->name);
      }
    }
  } // safe_it destructor automatically resumes rehashing

  std::printf("Remaining after deletion: %zu users\n", ht.Size());
}

void ScanExample() {
  PrintSeparator("Cursor Scan");
  UserTable ht;

  for (int i = 0; i < 20; ++i) {
    ht.Add(new User{
      "scan_user_" + std::to_string(i), i, ""
    });
  }

  // Cursor-based scan: stateless, works during rehashing
  // Returns 0 when complete
  int count = 0;
  size_t cursor = 0;
  do {
    cursor = ht.Scan(cursor, [&](void *ptr) {
      auto *u = static_cast<User *>(ptr);
      count++;
      (void)u;
    });
  } while (cursor != 0);

  std::printf("Scanned %d entries (expected %zu)\n", count, ht.Size());
}

void TwoPhaseInsertExample() {
  PrintSeparator("Two-Phase Insert");
  UserTable ht;
  ht.Add(new User{"existing", 1, ""});

  // Phase 1: check if insert is possible, get position
  UserTable::Position pos;
  User *existing = nullptr;

  bool can_insert = ht.FindPositionForInsert("existing", pos, &existing);
  std::printf("FindPositionForInsert('existing'): %s\n",
              can_insert ? "slot found" : "already exists");
  if (existing) {
    std::printf("  existing entry age = %d\n", existing->age);
  }

  // Insert a new key
  can_insert = ht.FindPositionForInsert("new_user", pos);
  if (can_insert) {
    // Phase 2: create entry and insert (only if phase 1 succeeded)
    auto *u = new User{"new_user", 42, "new@test.com"};
    ht.InsertAtPosition(u, pos);
    std::printf("Inserted 'new_user' at reserved position, size=%zu\n", ht.Size());
  }
}

void TwoPhasePopExample() {
  PrintSeparator("Two-Phase Pop");
  UserTable ht;
  ht.Add(new User{"target", 99, "target@test.com"});

  // Phase 1: find the entry ref and save position
  UserTable::Position pos;
  User **ref = ht.TwoPhasePopFindRef("target", pos);
  if (ref) {
    std::printf("Found ref to 'target', age=%d\n", (*ref)->age);
    // Can inspect or modify entry before removing

    // Phase 2: remove it
    User *saved = *ref;
    ht.TwoPhasePopDelete(pos);
    std::printf("Popped, size=%zu\n", ht.Size());
    delete saved;
  }
}

void IncrementalFindExample() {
  PrintSeparator("Incremental Find (MLP)");
  UserTable ht;

  for (int i = 0; i < 100; ++i) {
    ht.Add(new User{"ifind_" + std::to_string(i), i, ""});
  }

  // Incremental find splits a lookup into multiple steps.
  // Between steps, CPU can work on other lookups (memory-level parallelism).
  // Useful for batch operations like MGET.

  // Example: pipeline two lookups
  UserTable::IncrementalFindState state1, state2;
  std::string key1 = "ifind_42", key2 = "ifind_99";

  ht.IncrementalFindInit(state1, key1);
  ht.IncrementalFindInit(state2, key2);

  // Interleave steps
  bool more1 = true, more2 = true;
  while (more1 || more2) {
    if (more1) more1 = ht.IncrementalFindStep(state1);
    if (more2) more2 = ht.IncrementalFindStep(state2);
  }

  User *r1 = ht.IncrementalFindGetResult(state1);
  User *r2 = ht.IncrementalFindGetResult(state2);
  std::printf("Lookup '%s': %s (age=%d)\n", key1.c_str(),
              r1 ? "found" : "not found", r1 ? r1->age : -1);
  std::printf("Lookup '%s': %s (age=%d)\n", key2.c_str(),
              r2 ? "found" : "not found", r2 ? r2->age : -1);
}

void RehashControlExample() {
  PrintSeparator("Rehash Control");
  UserTable ht;

  // Pre-expand to avoid rehashing during bulk insert
  ht.Expand(1000);
  std::printf("Pre-expanded, IsRehashing=%s\n",
              ht.IsRehashing() ? "yes" : "no");

  // Bulk insert
  for (int i = 0; i < 500; ++i) {
    ht.Add(new User{"rehash_" + std::to_string(i), i, ""});
  }

  // Complete any pending rehash with time budget
  while (ht.IsRehashing()) {
    ht.RehashMicroseconds(1000);  // 1ms budget per call
  }
  std::printf("After bulk insert + rehash: size=%zu, rehashing=%s\n",
              ht.Size(), ht.IsRehashing() ? "yes" : "no");

  // Resize policy: Avoid (for fork safety, like during BGSAVE)
  UserTable::SetResizePolicy(ResizePolicy::Avoid);
  for (int i = 500; i < 600; ++i) {
    ht.Add(new User{"rehash_" + std::to_string(i), i, ""});
  }
  std::printf("Under Avoid policy: size=%zu\n", ht.Size());

  // Restore normal policy
  UserTable::SetResizePolicy(ResizePolicy::Allow);

  // Pause/resume auto-shrink
  ht.PauseAutoShrink();
  for (int i = 0; i < 550; ++i) {
    ht.Remove("rehash_" + std::to_string(i));
  }
  std::printf("After bulk delete (shrink paused): size=%zu\n", ht.Size());
  ht.ResumeAutoShrink();  // may trigger shrink now
}

void RandomSamplingExample() {
  PrintSeparator("Random Sampling");
  UserTable ht;

  for (int i = 0; i < 100; ++i) {
    ht.Add(new User{"sample_" + std::to_string(i), i, ""});
  }

  // Get one random entry
  User *random = ht.RandomEntry();
  std::printf("Random entry: %s (age=%d)\n",
              random->name.c_str(), random->age);

  // Sample multiple entries
  User *samples[5];
  unsigned n = ht.SampleEntries(samples, 5);
  std::printf("Sampled %u entries:\n", n);
  for (unsigned i = 0; i < n; ++i) {
    std::printf("  %s\n", samples[i]->name.c_str());
  }
}

void StatisticsExample() {
  PrintSeparator("Statistics");
  UserTable ht;

  for (int i = 0; i < 500; ++i) {
    ht.Add(new User{"stat_" + std::to_string(i), i, ""});
  }

  // Complete rehash for clean stats
  while (ht.IsRehashing()) {
    ht.RehashMicroseconds(10000);
  }

  std::printf("Size: %zu\n", ht.Size());
  std::printf("Buckets: %zu\n", ht.BucketCount());
  std::printf("Entries per bucket: %u\n", UserTable::EntriesPerBucket());
  std::printf("Memory usage: %zu bytes\n", ht.MemUsage());
  std::printf("Per-entry overhead: %.1f bytes\n",
              static_cast<double>(ht.MemUsage()) / ht.Size());

  // Detailed stats with chain length distribution
  std::string stats = ht.GetStats(true);
  std::printf("\n%s", stats.c_str());
}

void FindRefExample() {
  PrintSeparator("FindRef (in-place update)");
  UserTable ht;
  ht.Add(new User{"mutable", 10, "old@test.com"});

  // FindRef returns a pointer to the entry slot in the bucket.
  // You can swap the entry pointer for defrag or in-place replacement.
  User **ref = ht.FindRef("mutable");
  if (ref) {
    std::printf("Before: age=%d\n", (*ref)->age);
    // Replace with a new entry (same key!)
    User *old = *ref;
    *ref = new User{"mutable", 20, "new@test.com"};
    delete old;
    std::printf("After:  age=%d\n", (*ref)->age);
  }
}

void HashSeedExample() {
  PrintSeparator("Hash Seed");

  // Set a custom hash seed (should be done once at startup)
  uint8_t seed[16] = {0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04,
                       0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C};
  UserTable::SetHashFunctionSeed(seed);

  // Use the global hash function directly
  uint64_t h1 = UserTable::GenHashFunction("hello", 5);
  uint64_t h2 = UserTable::GenCaseHashFunction("HELLO", 5);
  std::printf("Hash of 'hello':  0x%016llx\n", (unsigned long long)h1);
  std::printf("Hash of 'HELLO' (case-insensitive): 0x%016llx\n",
              (unsigned long long)h2);
  std::printf("Case-insensitive match: %s\n", (h1 == h2) ? "yes" : "no");

  // Reset seed
  uint8_t zero[16] = {};
  UserTable::SetHashFunctionSeed(zero);
}

void MoveSemantics() {
  PrintSeparator("Move Semantics");

  UserTable ht1;
  for (int i = 0; i < 10; ++i) {
    ht1.Add(new User{"move_" + std::to_string(i), i, ""});
  }
  std::printf("ht1 size: %zu\n", ht1.Size());

  // Move construct
  UserTable ht2(std::move(ht1));
  std::printf("After move: ht1=%zu, ht2=%zu\n", ht1.Size(), ht2.Size());

  // Move assign
  UserTable ht3;
  ht3 = std::move(ht2);
  std::printf("After assign: ht2=%zu, ht3=%zu\n", ht2.Size(), ht3.Size());
}

// ============================================================================
// Main
// ============================================================================

int main() {
  std::printf("cancer_redis HashTable Usage Examples\n");
  std::printf("=====================================\n");
  std::printf("Bucket size: %zu bytes (1 cache line)\n", sizeof(Bucket<void *>));
  std::printf("Entries per bucket: %d\n", kEntriesPerBucket);

  BasicCrud();
  IteratorExample();
  ScanExample();
  TwoPhaseInsertExample();
  TwoPhasePopExample();
  IncrementalFindExample();
  RehashControlExample();
  RandomSamplingExample();
  StatisticsExample();
  FindRefExample();
  HashSeedExample();
  MoveSemantics();

  std::printf("\n===== All examples completed =====\n");
  return 0;
}
