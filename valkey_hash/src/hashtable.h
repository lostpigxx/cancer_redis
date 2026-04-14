#pragma once

// Cache-line-aware, SIMD-accelerated hash table with incremental rehashing.
// C++ rewrite of Valkey's hashtable (src/hashtable.c).
//
// Key design properties:
//   - Each bucket is exactly 64 bytes (one CPU cache line)
//   - 7 entries per bucket on 64-bit, 12 on 32-bit
//   - 1-byte hash fingerprint (h2) per entry for fast SIMD filtering
//   - Bucket chaining for overflow (not per-entry linked lists)
//   - Two-table incremental rehashing
//   - Cursor-based scan with bit-reversal increment
//   - Incremental find for memory-level parallelism

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <type_traits>

#include "hashtable_bucket.h"
#include "siphash.h"

namespace cancer_redis {

// ============================================================================
// Resize policy
// ============================================================================

enum class ResizePolicy : uint8_t {
  Allow = 0,  // Normal: soft fill limits, rehash on read+write
  Avoid = 1,  // Fork-safe: hard fill limits, rehash on write only
  Forbid = 2, // Read-only child: no resize at all
};

// ============================================================================
// Global state (shared across all HashTable instances)
// ============================================================================

namespace detail {

inline uint8_t g_hash_seed[16] = {};
inline ResizePolicy g_resize_policy = ResizePolicy::Allow;
inline bool g_can_abort_shrink = true;

} // namespace detail

// ============================================================================
// Default traits
// ============================================================================

template <typename Entry, typename Key>
struct DefaultHashTableTraits {
  static const Key &EntryGetKey(const Entry *entry) {
    return *entry;
  }

  static uint64_t Hash(const Key &key) {
    return SipHash(reinterpret_cast<const uint8_t *>(&key), sizeof(Key),
             detail::g_hash_seed);
  }

  static bool KeyEqual(const Key &a, const Key &b) {
    return a == b;
  }

  // Optional callbacks: disabled by default
  static constexpr bool kHasValidateEntry = false;
  static bool ValidateEntry(const Entry *) { return true; }

  static constexpr bool kHasEntryDestructor = false;
  static void EntryDestructor(Entry *) {}

  static constexpr bool kHasEntryPrefetchValue = false;
  static void EntryPrefetchValue(const Entry *) {}

  static constexpr bool kHasResizeAllowed = false;
  static bool ResizeAllowed(size_t, double) { return true; }

  static constexpr bool kHasRehashingStarted = false;
  static void RehashingStarted(void *) {}

  static constexpr bool kHasRehashingCompleted = false;
  static void RehashingCompleted(void *) {}

  static constexpr bool kHasTrackMemUsage = false;
  static void TrackMemUsage(void *, ssize_t) {}

  static constexpr size_t kMetadataSize = 0;
  static constexpr bool kInstantRehashing = false;
};

// ============================================================================
// Forward declarations
// ============================================================================

template <typename Entry, typename Key,
      typename Traits = DefaultHashTableTraits<Entry, Key>,
      typename Allocator = std::allocator<Entry>>
class HashTable;

// ============================================================================
// Iterator flags
// ============================================================================

inline constexpr uint8_t kIterSafe = 1 << 0;
inline constexpr uint8_t kIterPrefetchValues = 1 << 1;
inline constexpr uint8_t kIterSkipValidation = 1 << 2;

// Scan flags
inline constexpr int kScanEmitRef = 1 << 0;

// ============================================================================
// Statistics structure
// ============================================================================

struct HashTableStats {
  int table_index = 0;
  ssize_t rehash_index = -1;
  size_t toplevel_buckets = 0;
  size_t child_buckets = 0;
  size_t capacity = 0;
  size_t used = 0;
  size_t max_chain_len = 0;
  std::unique_ptr<size_t[]> chain_len_vec;

  HashTableStats() : chain_len_vec(new size_t[kStatsVectLen]()) {}
};

// ============================================================================
// HashTable class template
// ============================================================================

template <typename Entry, typename Key, typename Traits, typename Allocator>
class HashTable {
public:
  using entry_type = Entry;
  using key_type = Key;
  using traits_type = Traits;
  using allocator_type = Allocator;
  using BucketType = Bucket<Entry>;

private:
  using BucketAllocator = typename std::allocator_traits<Allocator>::template rebind_alloc<BucketType>;

  // Internal iterator state
  struct IterState {
    HashTable *hashtable = nullptr;
    BucketType *bucket = nullptr;
    long index = -1;
    uint16_t pos_in_bucket = 0;
    uint8_t table = 0;
    uint8_t flags = 0;
    union {
      uint64_t fingerprint;
      uint64_t last_seen_size;
    };
    IterState *next_safe_iter = nullptr;
  };

public:
  // ========================================================================
  // Position (opaque, for two-phase operations)
  // ========================================================================

  struct Position {
    BucketType *bucket = nullptr;
    int pos_in_bucket = 0;
    int table_index = 0;
  };

  // ========================================================================
  // IncrementalFindState
  // ========================================================================

  struct IncrementalFindState {
    enum class State : uint8_t {
      NextBucket,
      NextEntry,
      CheckEntry,
      Found,
      NotFound
    };
    State state;
    int table = 0;
    int pos = 0;
    const HashTable *hashtable = nullptr;
    BucketType *bucket = nullptr;
    const Key *key = nullptr;
    uint64_t hash = 0;
  };

  // ========================================================================
  // Iterator (RAII, input iterator)
  // ========================================================================

  class Iterator {
  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Entry *;
    using difference_type = std::ptrdiff_t;
    using pointer = Entry **;
    using reference = Entry *;

    Iterator() = default;

    ~Iterator() {
      Cleanup();
    }

    Iterator(Iterator &&other) noexcept
      : state_(other.state_), current_(other.current_), valid_(other.valid_) {
      if (valid_ && IsSafe()) {
        // Update the safe iterator linked list to point to new location
        UpdateSafeIterLink(other);
      }
      other.valid_ = false;
      other.current_ = nullptr;
    }

    Iterator &operator=(Iterator &&other) noexcept {
      if (this != &other) {
        Cleanup();
        state_ = other.state_;
        current_ = other.current_;
        valid_ = other.valid_;
        if (valid_ && IsSafe()) {
          UpdateSafeIterLink(other);
        }
        other.valid_ = false;
        other.current_ = nullptr;
      }
      return *this;
    }

    Iterator(const Iterator &) = delete;
    Iterator &operator=(const Iterator &) = delete;

    Entry *operator*() const { return current_; }

    Iterator &operator++() {
      Advance();
      return *this;
    }

    bool operator==(const Iterator &other) const {
      if (!valid_ && !other.valid_) return true;
      return false; // Only end==end is defined for input iterators
    }

    bool operator!=(const Iterator &other) const { return !(*this == other); }

    explicit operator bool() const { return valid_ && current_ != nullptr; }

    // C-style next() interface
    bool Next(Entry **out) {
      if (!valid_ || state_.hashtable == nullptr) return false;
      if (!Advance()) return false;
      if (out) *out = current_;
      return true;
    }

    // Retarget to a different hashtable (reset iteration)
    void Retarget(HashTable &ht) {
      uint8_t flags = valid_ ? state_.flags : 0;
      Cleanup();
      state_.hashtable = &ht;
      state_.table = 0;
      state_.index = -1;
      state_.flags = flags;
      state_.next_safe_iter = nullptr;
      valid_ = true;
      current_ = nullptr;
      if (IsSafe() && state_.hashtable != nullptr) {
        state_.hashtable->TrackSafeIterator(&state_);
      }
    }

  private:
    friend class HashTable;

    IterState state_{};
    Entry *current_ = nullptr;
    bool valid_ = false;
    bool started_ = false;

    explicit Iterator(HashTable *ht, uint8_t flags) : valid_(true), started_(false) {
      state_.hashtable = ht;
      state_.table = 0;
      state_.index = -1;
      state_.pos_in_bucket = 0;
      state_.flags = flags;
      state_.next_safe_iter = nullptr;
      if (IsSafe() && ht != nullptr) {
        ht->TrackSafeIterator(&state_);
      }
      // Advance to first element
      Advance();
    }

    // Construct an end sentinel
    struct EndTag {};
    explicit Iterator(EndTag) : valid_(false) {}

    bool IsSafe() const { return state_.flags & kIterSafe; }
    bool DoPrefetchValues() const { return state_.flags & kIterPrefetchValues; }
    bool DoSkipValidation() const { return state_.flags & kIterSkipValidation; }

    void Cleanup() {
      if (state_.hashtable == nullptr) {
        valid_ = false;
        return;
      }
      // Resume rehashing / check fingerprint if we actually started iterating
      if (started_) {
        if (IsSafe()) {
          state_.hashtable->ResumeRehashing();
        } else {
          assert(state_.fingerprint == state_.hashtable->Fingerprint());
        }
        started_ = false;
      }
      if (IsSafe()) {
        state_.hashtable->UntrackSafeIterator(&state_);
      }
      state_.hashtable = nullptr;
      valid_ = false;
    }

    void UpdateSafeIterLink(Iterator &old) {
      if (!state_.hashtable) return;
      // Find old in linked list and replace with this
      IterState **pp = &state_.hashtable->safe_iterators_;
      while (*pp) {
        if (*pp == &old.state_) {
          *pp = &state_;
          return;
        }
        pp = &(*pp)->next_safe_iter;
      }
    }

    bool Advance() {
      if (!valid_ || state_.hashtable == nullptr) {
        current_ = nullptr;
        valid_ = false;
        return false;
      }

      HashTable *ht = state_.hashtable;

      while (true) {
        if (state_.index == -1 && state_.table == 0) {
          // First call
          started_ = true;
          if (IsSafe()) {
            ht->PauseRehashing();
            state_.last_seen_size = ht->used_[0];
          } else {
            state_.fingerprint = ht->Fingerprint();
          }
          if (ht->tables_[0] == nullptr) {
            current_ = nullptr;
            return false;
          }
          state_.index = 0;
          if (ht->IsRehashing()) {
            state_.index = ht->rehash_idx_;
          }
          state_.bucket = &ht->tables_[state_.table][state_.index];
          state_.pos_in_bucket = 0;
        } else {
          // Advance position
          state_.pos_in_bucket++;
          if (state_.bucket->chained &&
            state_.pos_in_bucket >= kEntriesPerBucket - 1) {
            // Move to child bucket
            state_.pos_in_bucket = 0;
            state_.bucket = state_.bucket->ChildBucket();
          } else if (state_.pos_in_bucket >= kEntriesPerBucket) {
            // Move to next bucket index
            if (IsSafe()) {
              if (ht->pause_rehash_ == 1 &&
                ht->used_[state_.table] < state_.last_seen_size) {
                ht->CompactBucketChain(state_.index, state_.table);
              }
              state_.last_seen_size = ht->used_[state_.table];
            }
            state_.pos_in_bucket = 0;
            state_.index++;
            if (static_cast<size_t>(state_.index) >=
              NumBuckets(ht->bucket_exp_[state_.table])) {
              if (ht->IsRehashing() && state_.table == 0) {
                state_.index = 0;
                state_.table = 1;
              } else {
                current_ = nullptr;
                valid_ = false;
                return false;
              }
            }
            state_.bucket =
              &ht->tables_[state_.table][state_.index];
          }
        }

        BucketType *b = state_.bucket;
        if (b == nullptr) {
          current_ = nullptr;
          valid_ = false;
          return false;
        }

        // Prefetch on new bucket
        if (state_.pos_in_bucket == 0) {
          PrefetchNextBucketEntries();
        }

        if (!b->IsPositionFilled(state_.pos_in_bucket)) continue;

        Entry *entry = b->entries[state_.pos_in_bucket];
        if constexpr (Traits::kHasValidateEntry) {
          if (!DoSkipValidation() && !Traits::ValidateEntry(entry)) {
            continue;
          }
        }

        current_ = entry;
        return true;
      }
    }

    void PrefetchNextBucketEntries() {
      HashTable *ht = state_.hashtable;
      if (!ht) return;
      size_t nb = NumBuckets(ht->bucket_exp_[state_.table]);
      long next_idx = state_.index + 1;
      if (static_cast<size_t>(next_idx) < nb) {
        BucketType *next_b = &ht->tables_[state_.table][next_idx];
        // Prefetch next bucket's entries
        for (int i = 0; i < kEntriesPerBucket; ++i) {
          if (next_b->IsPositionFilled(i)) {
            simd::Prefetch(next_b->entries[i]);
          }
        }
        if constexpr (Traits::kHasEntryPrefetchValue) {
          if (DoPrefetchValues()) {
            for (int i = 0; i < kEntriesPerBucket; ++i) {
              if (next_b->IsPositionFilled(i)) {
                Traits::EntryPrefetchValue(next_b->entries[i]);
              }
            }
          }
        }
        // Prefetch bucket after next (metadata only)
        long next_next_idx = next_idx + 1;
        if (static_cast<size_t>(next_next_idx) < nb) {
          simd::Prefetch(&ht->tables_[state_.table][next_next_idx]);
        }
      }
    }
  };

  // ========================================================================
  // Construction / Destruction
  // ========================================================================

  explicit HashTable(const Allocator &alloc = Allocator())
    : bucket_alloc_(alloc) {}

  ~HashTable() {
    Clear(nullptr);
  }

  HashTable(const HashTable &) = delete;
  HashTable &operator=(const HashTable &) = delete;

  HashTable(HashTable &&other) noexcept
    : rehash_idx_(other.rehash_idx_),
      used_{other.used_[0], other.used_[1]},
      bucket_exp_{other.bucket_exp_[0], other.bucket_exp_[1]},
      pause_rehash_(other.pause_rehash_),
      pause_auto_shrink_(other.pause_auto_shrink_),
      child_buckets_{other.child_buckets_[0], other.child_buckets_[1]},
      safe_iterators_(other.safe_iterators_),
      bucket_alloc_(std::move(other.bucket_alloc_)) {
    tables_[0] = other.tables_[0];
    tables_[1] = other.tables_[1];
    other.tables_[0] = nullptr;
    other.tables_[1] = nullptr;
    other.rehash_idx_ = -1;
    other.used_[0] = other.used_[1] = 0;
    other.bucket_exp_[0] = other.bucket_exp_[1] = -1;
    other.child_buckets_[0] = other.child_buckets_[1] = 0;
    other.safe_iterators_ = nullptr;
  }

  HashTable &operator=(HashTable &&other) noexcept {
    if (this != &other) {
      Clear(nullptr);
      rehash_idx_ = other.rehash_idx_;
      tables_[0] = other.tables_[0];
      tables_[1] = other.tables_[1];
      used_[0] = other.used_[0];
      used_[1] = other.used_[1];
      bucket_exp_[0] = other.bucket_exp_[0];
      bucket_exp_[1] = other.bucket_exp_[1];
      pause_rehash_ = other.pause_rehash_;
      pause_auto_shrink_ = other.pause_auto_shrink_;
      child_buckets_[0] = other.child_buckets_[0];
      child_buckets_[1] = other.child_buckets_[1];
      safe_iterators_ = other.safe_iterators_;
      bucket_alloc_ = std::move(other.bucket_alloc_);
      other.tables_[0] = other.tables_[1] = nullptr;
      other.rehash_idx_ = -1;
      other.used_[0] = other.used_[1] = 0;
      other.bucket_exp_[0] = other.bucket_exp_[1] = -1;
      other.child_buckets_[0] = other.child_buckets_[1] = 0;
      other.safe_iterators_ = nullptr;
    }
    return *this;
  }

  // ========================================================================
  // Clear / Empty
  // ========================================================================

  void Clear(std::function<void(HashTable &)> callback) {
    for (int t = 0; t < 2; ++t) {
      if (tables_[t] == nullptr) continue;
      if constexpr (Traits::kHasEntryDestructor) {
        // Destroy all entries
        size_t nb = NumBuckets(bucket_exp_[t]);
        for (size_t i = 0; i < nb; ++i) {
          BucketType *b = &tables_[t][i];
          while (b != nullptr) {
            for (int pos = 0; pos < kEntriesPerBucket; ++pos) {
              if (b->IsPositionFilled(pos) &&
                !(b->chained && pos == kEntriesPerBucket - 1)) {
                Traits::EntryDestructor(b->entries[pos]);
              }
            }
            b = b->ChildBucket();
          }
        }
      }
      // Free child buckets
      FreeChildBuckets(t);
      DeallocateBucketArray(tables_[t], NumBuckets(bucket_exp_[t]));
      tables_[t] = nullptr;
      bucket_exp_[t] = -1;
      used_[t] = 0;
      child_buckets_[t] = 0;
    }
    rehash_idx_ = -1;
    if (callback) callback(*this);
    InvalidateAllSafeIterators();
  }

  // ========================================================================
  // Size / Status queries
  // ========================================================================

  size_t Size() const { return used_[0] + used_[1]; }

  bool Empty() const { return Size() == 0; }

  size_t BucketCount() const {
    return NumBuckets(bucket_exp_[0]) + NumBuckets(bucket_exp_[1]);
  }

  size_t ChainedBucketCount(int table) const {
    assert(table == 0 || table == 1);
    return child_buckets_[table];
  }

  static constexpr unsigned EntriesPerBucket() { return kEntriesPerBucket; }

  size_t MemUsage() const {
    size_t mem = sizeof(*this);
    for (int t = 0; t < 2; ++t) {
      mem += NumBuckets(bucket_exp_[t]) * sizeof(BucketType);
      mem += child_buckets_[t] * sizeof(BucketType);
    }
    return mem;
  }

  bool IsRehashing() const { return rehash_idx_ != -1; }
  bool IsRehashingPaused() const { return pause_rehash_ > 0; }

  ssize_t GetRehashingIndex() const { return rehash_idx_; }

  void RehashingInfo(size_t &from_size, size_t &to_size) const {
    if (!IsRehashing()) {
      from_size = to_size = 0;
      return;
    }
    from_size = NumBuckets(bucket_exp_[0]) * kEntriesPerBucket;
    to_size = NumBuckets(bucket_exp_[1]) * kEntriesPerBucket;
  }

  void PauseAutoShrink() { ++pause_auto_shrink_; }
  void ResumeAutoShrink() {
    --pause_auto_shrink_;
    if (pause_auto_shrink_ == 0) {
      ShrinkIfNeeded();
    }
  }

  // Metadata access (if Traits::kMetadataSize > 0)
  void *Metadata() { return metadata_; }
  const void *Metadata() const { return metadata_; }

  // ========================================================================
  // Find
  // ========================================================================

  Entry *Find(const Key &key) {
    if (Size() == 0) return nullptr;
    uint64_t h = HashKey(key);
    int pos_in_bucket = 0;
    BucketType *b = FindBucket(h, key, pos_in_bucket, nullptr);
    if (b) return b->entries[pos_in_bucket];
    return nullptr;
  }

  const Entry *Find(const Key &key) const {
    return const_cast<HashTable *>(this)->Find(key);
  }

  // FindRef: returns pointer to the entry slot
  Entry **FindRef(const Key &key) {
    if (Size() == 0) return nullptr;
    uint64_t h = HashKey(key);
    int pos_in_bucket = 0;
    BucketType *b = FindBucket(h, key, pos_in_bucket, nullptr);
    if (b) return &b->entries[pos_in_bucket];
    return nullptr;
  }

  // ========================================================================
  // Add / Insert
  // ========================================================================

  bool Add(Entry *entry) {
    const Key &key = Traits::EntryGetKey(entry);
    uint64_t h = HashKey(key);

    // Check if already exists
    int pos = 0;
    BucketType *existing = FindBucket(h, key, pos, nullptr);
    if (existing) return false;

    Insert(h, entry);
    return true;
  }

  bool AddOrFind(Entry *entry, Entry **existing = nullptr) {
    const Key &key = Traits::EntryGetKey(entry);
    uint64_t h = HashKey(key);

    int pos = 0;
    BucketType *b = FindBucket(h, key, pos, nullptr);
    if (b) {
      if (existing) *existing = b->entries[pos];
      return false;
    }

    Insert(h, entry);
    return true;
  }

  // ========================================================================
  // Two-phase insert
  // ========================================================================

  bool FindPositionForInsert(const Key &key, Position &position,
                 Entry **existing = nullptr) {
    uint64_t h = HashKey(key);

    // Check if key already exists
    int pos = 0;
    BucketType *b = FindBucket(h, key, pos, nullptr);
    if (b) {
      if (existing) *existing = b->entries[pos];
      return false;
    }

    // Prepare for insertion
    ExpandIfNeeded();
    RehashStepOnWriteIfNeeded();

    int table_index = 0;
    b = FindBucketForInsert(h, pos, table_index);
    assert(!b->IsPositionFilled(pos));

    // Pre-store h2 for later insertion
    b->hashes[pos] = HighBits(h);

    position.bucket = b;
    position.pos_in_bucket = pos;
    position.table_index = table_index;
    return true;
  }

  void InsertAtPosition(Entry *entry, Position &position) {
    BucketType *b = position.bucket;
    int pos = position.pos_in_bucket;
    int table = position.table_index;

    assert(!b->IsPositionFilled(pos));
    b->SetPositionFilled(pos);
    b->entries[pos] = entry;
    used_[table]++;
  }

  // ========================================================================
  // Delete / Pop
  // ========================================================================

  bool Remove(const Key &key) {
    Entry *entry = Pop(key);
    if (!entry) return false;
    if constexpr (Traits::kHasEntryDestructor) {
      Traits::EntryDestructor(entry);
    }
    return true;
  }

  Entry *Pop(const Key &key) {
    if (Size() == 0) return nullptr;
    uint64_t h = HashKey(key);

    RehashStepOnReadIfNeeded();

    int pos_in_bucket = 0;
    int table_index = 0;
    BucketType *b = FindBucket(h, key, pos_in_bucket, &table_index);
    if (!b) return nullptr;

    Entry *entry = b->entries[pos_in_bucket];
    b->ClearPosition(pos_in_bucket);
    used_[table_index]--;

    if (b->chained && !IsRehashingPaused()) {
      FillBucketHole(b, pos_in_bucket, table_index);
    }

    ShrinkIfNeeded();
    return entry;
  }

  // ========================================================================
  // Two-phase pop
  // ========================================================================

  Entry **TwoPhasePopFindRef(const Key &key, Position &position) {
    if (Size() == 0) return nullptr;
    uint64_t h = HashKey(key);

    RehashStepOnReadIfNeeded();

    int pos_in_bucket = 0;
    int table_index = 0;
    BucketType *b = FindBucket(h, key, pos_in_bucket, &table_index);
    if (!b) return nullptr;

    position.bucket = b;
    position.pos_in_bucket = pos_in_bucket;
    position.table_index = table_index;
    return &b->entries[pos_in_bucket];
  }

  void TwoPhasePopDelete(Position &position) {
    BucketType *b = position.bucket;
    int pos = position.pos_in_bucket;
    int table = position.table_index;

    b->ClearPosition(pos);
    used_[table]--;

    if (b->chained && !IsRehashingPaused()) {
      FillBucketHole(b, pos, table);
    }
    ShrinkIfNeeded();
  }

  // ========================================================================
  // Replace reallocated entry (for defrag)
  // ========================================================================

  bool ReplaceReallocatedEntry(const Entry *old_entry, Entry *new_entry) {
    const Key &key = Traits::EntryGetKey(old_entry);
    uint64_t h = HashKey(key);

    for (int table = 0; table <= 1; ++table) {
      if (tables_[table] == nullptr) continue;
      size_t mask = ExpToMask(bucket_exp_[table]);
      size_t idx = h & mask;

      if (table == 0 && rehash_idx_ >= 0 &&
        idx < static_cast<size_t>(rehash_idx_)) {
        continue;
      }

      BucketType *b = &tables_[table][idx];
      while (b != nullptr) {
        for (int pos = 0; pos < b->NumPositions(); ++pos) {
          if (b->IsPositionFilled(pos) && b->entries[pos] == old_entry) {
            b->entries[pos] = new_entry;
            return true;
          }
        }
        b = b->ChildBucket();
      }
    }
    return false;
  }

  // ========================================================================
  // Incremental Find (MLP optimization)
  // ========================================================================

  void IncrementalFindInit(IncrementalFindState &state, const Key &key) const {
    state.hashtable = this;
    state.key = &key;
    state.hash = HashKey(key);
    if (Size() == 0) {
      state.state = IncrementalFindState::State::NotFound;
    } else {
      state.state = IncrementalFindState::State::NextBucket;
      state.bucket = nullptr;
    }
  }

  // Returns true if more steps needed, false if done.
  bool IncrementalFindStep(IncrementalFindState &state) const {
    using S = typename IncrementalFindState::State;

    switch (state.state) {
    case S::CheckEntry: {
      Entry *entry = state.bucket->entries[state.pos];
      const Key &elem_key = Traits::EntryGetKey(entry);
      if (Traits::KeyEqual(*state.key, elem_key)) {
        state.state = S::Found;
        return false;
      }
      state.pos++;
    }
      [[fallthrough]];

    case S::NextEntry: {
      if (state.bucket == nullptr) {
        state.state = S::NotFound;
        return false;
      }
      BucketType *b = state.bucket;
      uint8_t h2 = HighBits(state.hash);
      int n = b->NumPositions();
      for (int pos = state.pos; pos < n; ++pos) {
        if (b->IsPositionFilled(pos) && b->hashes[pos] == h2) {
          simd::Prefetch(b->entries[pos]);
          state.pos = pos;
          state.state = S::CheckEntry;
          return true;
        }
      }
    }
      [[fallthrough]];

    case S::NextBucket: {
      if (state.bucket == nullptr) {
        // Initial bucket lookup
        state.table = 0;
        size_t mask = ExpToMask(bucket_exp_[0]);
        size_t idx = state.hash & mask;
        if (rehash_idx_ >= 0 &&
          idx < static_cast<size_t>(rehash_idx_)) {
          state.table = 1;
          mask = ExpToMask(bucket_exp_[1]);
          idx = state.hash & mask;
        }
        if (tables_[state.table] == nullptr) {
          state.state = S::NotFound;
          return false;
        }
        state.bucket = &tables_[state.table][idx];
      } else if (state.bucket->ChildBucket() != nullptr) {
        state.bucket = state.bucket->ChildBucket();
      } else if (state.table == 0 && rehash_idx_ >= 0) {
        state.table = 1;
        if (tables_[1] == nullptr) {
          state.state = S::NotFound;
          return false;
        }
        size_t mask = ExpToMask(bucket_exp_[1]);
        size_t idx = state.hash & mask;
        state.bucket = &tables_[1][idx];
      } else {
        state.state = S::NotFound;
        return false;
      }
      simd::Prefetch(state.bucket);
      state.state = S::NextEntry;
      state.pos = 0;
      return true;
    }

    case S::Found:
    case S::NotFound:
      return false;
    }
    assert(false);
    return false;
  }

  Entry *IncrementalFindGetResult(IncrementalFindState &state) const {
    using S = typename IncrementalFindState::State;
    if (state.state == S::Found) {
      return state.bucket->entries[state.pos];
    }
    return nullptr;
  }

  // ========================================================================
  // Iteration
  // ========================================================================

  Iterator GetIterator(uint8_t flags = 0) {
    return Iterator(this, flags);
  }

  Iterator Begin(uint8_t flags = 0) {
    return Iterator(this, flags);
  }

  Iterator End() {
    return Iterator(typename Iterator::EndTag{});
  }

  // ========================================================================
  // Scan (cursor-based, stateless)
  // ========================================================================

  using ScanCallback = std::function<void(void *)>;

  size_t Scan(size_t cursor, ScanCallback fn) const {
    return ScanDefrag(cursor, fn, nullptr, 0);
  }

  size_t ScanDefrag(size_t cursor, ScanCallback fn,
            std::function<void *(void *)> defragfn = nullptr,
            int flags = 0) const {
    if (Size() == 0) return 0;

    // Pause rehashing during scan
    const_cast<HashTable *>(this)->PauseRehashing();
    bool emit_ref = (flags & kScanEmitRef) != 0;

    if (!IsRehashing()) {
      // Single table scan
      size_t mask = ExpToMask(bucket_exp_[0]);
      size_t idx = cursor & mask;
      size_t used_before = used_[0];

      BucketType *b = &tables_[0][idx];
      while (b != nullptr) {
        if (fn) {
          for (int pos = 0; pos < kEntriesPerBucket; ++pos) {
            if (b->IsPositionFilled(pos)) {
              if constexpr (Traits::kHasValidateEntry) {
                if (!Traits::ValidateEntry(b->entries[pos])) continue;
              }
              if (emit_ref)
                fn(static_cast<void *>(&b->entries[pos]));
              else
                fn(static_cast<void *>(b->entries[pos]));
            }
          }
        }
        BucketType *next = b->ChildBucket();
        if (next && defragfn) {
          void *reallocated = defragfn(next);
          if (reallocated) {
            next = static_cast<BucketType *>(reallocated);
            b->SetChildBucket(next);
          }
        }
        b = next;
      }

      if (used_[0] < used_before) {
        const_cast<HashTable *>(this)->CompactBucketChain(
          static_cast<long>(idx), 0);
      }
      cursor = NextCursor(cursor, mask);
    } else {
      // Two-table scan during rehash
      int table_small, table_large;
      if (bucket_exp_[0] <= bucket_exp_[1]) {
        table_small = 0;
        table_large = 1;
      } else {
        table_small = 1;
        table_large = 0;
      }

      size_t mask_small = ExpToMask(bucket_exp_[table_small]);
      size_t mask_large = ExpToMask(bucket_exp_[table_large]);

      // Scan small table
      size_t idx = cursor & mask_small;
      if (table_small == 1 || rehash_idx_ == -1 ||
        idx >= static_cast<size_t>(rehash_idx_)) {
        size_t used_before = used_[table_small];
        ScanBucket(tables_[table_small], idx, fn, defragfn, emit_ref);
        if (used_[table_small] < used_before) {
          const_cast<HashTable *>(this)->CompactBucketChain(
            static_cast<long>(idx), table_small);
        }
      }

      // Scan expanded indices in larger table
      do {
        idx = cursor & mask_large;
        if (table_large == 1 || rehash_idx_ == -1 ||
          idx >= static_cast<size_t>(rehash_idx_)) {
          size_t used_before = used_[table_large];
          ScanBucket(tables_[table_large], idx, fn, defragfn, emit_ref);
          if (used_[table_large] < used_before) {
            const_cast<HashTable *>(this)->CompactBucketChain(
              static_cast<long>(idx), table_large);
          }
        }
        cursor = NextCursor(cursor, mask_large);
      } while (cursor & (mask_small ^ mask_large));
    }

    const_cast<HashTable *>(this)->ResumeRehashing();
    return cursor;
  }

  // ========================================================================
  // Resize
  // ========================================================================

  bool Expand(size_t min_size) {
    if (IsRehashing() || min_size < Size()) return false;
    return Resize(min_size);
  }

  bool TryExpand(size_t min_size) {
    return Expand(min_size); // Same as expand for now
  }

  bool ExpandIfNeeded() {
    if (IsRehashing()) {
      if (bucket_exp_[1] >= bucket_exp_[0]) {
        return false; // Already expanding
      } else {
        return AbortShrinkIfNeeded();
      }
    }

    size_t min_capacity = used_[0] + 1;
    size_t nb = NumBuckets(bucket_exp_[0]);
    size_t current_capacity = nb * kEntriesPerBucket;
    unsigned max_fill = (detail::g_resize_policy == ResizePolicy::Allow)
                ? kMaxFillPercentSoft
                : kMaxFillPercentHard;

    if (min_capacity * 100 <= current_capacity * max_fill) {
      return false;
    }
    return Resize(min_capacity);
  }

  bool ShrinkIfNeeded() {
    if (IsRehashing() || pause_auto_shrink_ > 0) return false;
    if (tables_[0] == nullptr) return false;

    size_t current_capacity = NumBuckets(bucket_exp_[0]) * kEntriesPerBucket;
    unsigned min_fill = (detail::g_resize_policy == ResizePolicy::Allow)
                ? kMinFillPercentSoft
                : kMinFillPercentHard;

    if (used_[0] * 100 > current_capacity * min_fill) {
      return false;
    }
    size_t target = (used_[0] > 0) ? used_[0] : 1;
    return Resize(target);
  }

  bool RightsizeIfNeeded() {
    if (ExpandIfNeeded()) return true;
    return ShrinkIfNeeded();
  }

  // ========================================================================
  // Rehash
  // ========================================================================

  // Rehash for up to `us` microseconds. Returns 1 if more work, 0 if done.
  int RehashMicroseconds(uint64_t us) {
    if (!IsRehashing()) return 0;

    auto start = std::chrono::steady_clock::now();
    int steps = 0;

    while (IsRehashing()) {
      RehashStep();
      steps++;
      if (steps % 128 == 0) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
          now - start).count();
        if (static_cast<uint64_t>(elapsed) >= us) break;
      }
    }
    return IsRehashing() ? 1 : 0;
  }

  // ========================================================================
  // Random entry sampling
  // ========================================================================

  Entry *RandomEntry() {
    if (Size() == 0) return nullptr;

    thread_local std::mt19937_64 rng(std::random_device{}());

    while (true) {
      // Pick a random table (if rehashing, weighted by used count)
      int table = 0;
      if (IsRehashing()) {
        std::uniform_int_distribution<size_t> dist(0, Size() - 1);
        size_t r = dist(rng);
        table = (r < used_[0]) ? 0 : 1;
      }
      if (tables_[table] == nullptr || used_[table] == 0) {
        table = 1 - table;
      }

      size_t nb = NumBuckets(bucket_exp_[table]);
      std::uniform_int_distribution<size_t> bucket_dist(0, nb - 1);
      size_t idx = bucket_dist(rng);

      BucketType *b = &tables_[table][idx];
      int total = 0;
      BucketType *scan = b;
      while (scan) {
        total += scan->FilledCount();
        scan = scan->ChildBucket();
      }
      if (total == 0) continue;

      std::uniform_int_distribution<int> entry_dist(0, total - 1);
      int target = entry_dist(rng);

      scan = b;
      while (scan) {
        for (int pos = 0; pos < scan->NumPositions(); ++pos) {
          if (scan->IsPositionFilled(pos)) {
            if (target == 0) return scan->entries[pos];
            target--;
          }
        }
        scan = scan->ChildBucket();
      }
    }
  }

  Entry *FairRandomEntry() {
    // For truly fair random, sample by total size
    if (Size() == 0) return nullptr;
    return RandomEntry(); // Same implementation for now
  }

  unsigned SampleEntries(Entry **dst, unsigned count) {
    if (Size() == 0 || count == 0) return 0;

    unsigned sampled = 0;
    thread_local std::mt19937_64 rng(std::random_device{}());
    unsigned max_steps = count * 10;

    while (sampled < count && max_steps > 0) {
      Entry *e = RandomEntry();
      if (e) {
        dst[sampled++] = e;
      }
      max_steps--;
    }
    return sampled;
  }

  // ========================================================================
  // Statistics
  // ========================================================================

  HashTableStats GetStatsHt(int table_index, bool full = false) const {
    HashTableStats stats;
    stats.table_index = table_index;
    stats.rehash_index = rehash_idx_;
    stats.toplevel_buckets = NumBuckets(bucket_exp_[table_index]);
    stats.child_buckets = child_buckets_[table_index];
    stats.capacity = stats.toplevel_buckets * kEntriesPerBucket;
    stats.used = used_[table_index];

    if (!full || tables_[table_index] == nullptr) return stats;

    stats.max_chain_len = 0;
    for (size_t i = 0; i < stats.toplevel_buckets; ++i) {
      BucketType *b = &tables_[table_index][i];
      size_t chain_len = 0;
      while (b->chained) {
        chain_len++;
        b = b->ChildBucket();
      }
      if (chain_len > stats.max_chain_len) {
        stats.max_chain_len = chain_len;
      }
      size_t idx = (chain_len < kStatsVectLen) ? chain_len : kStatsVectLen - 1;
      stats.chain_len_vec[idx]++;
    }
    return stats;
  }

  std::string GetStats(bool full = false) const {
    std::string result;
    char buf[4096];

    auto fmt = [&](const HashTableStats &s, const char *label) {
      int n = std::snprintf(buf, sizeof(buf),
                  "Hash table %d stats (%s):\n"
                  " table size: %zu\n"
                  " number of entries: %zu\n"
                  " rehashing index: %zd\n",
                  s.table_index, label, s.capacity, s.used,
                  s.rehash_index);
      result.append(buf, n);

      if (full && s.toplevel_buckets > 0) {
        n = std::snprintf(buf, sizeof(buf),
                  " top-level buckets: %zu\n"
                  " child buckets: %zu\n"
                  " max chain length: %zu\n"
                  " avg chain length: %.3f\n"
                  " chain length distribution:\n",
                  s.toplevel_buckets, s.child_buckets,
                  s.max_chain_len,
                  s.toplevel_buckets > 0
                    ? static_cast<double>(s.child_buckets) /
                      s.toplevel_buckets
                    : 0.0);
        result.append(buf, n);
        for (int i = 0; i < kStatsVectLen; ++i) {
          if (s.chain_len_vec[i] == 0) continue;
          n = std::snprintf(
            buf, sizeof(buf), "   %d: %zu (%.02f%%)\n", i,
            s.chain_len_vec[i],
            static_cast<double>(s.chain_len_vec[i]) /
              s.toplevel_buckets * 100.0);
          result.append(buf, n);
        }
      }
    };

    auto s0 = GetStatsHt(0, full);
    fmt(s0, "main hash table");
    if (IsRehashing()) {
      auto s1 = GetStatsHt(1, full);
      fmt(s1, "rehashing target");
    }
    return result;
  }

  // ========================================================================
  // Defrag
  // ========================================================================

  void DefragTables(std::function<void *(void *)> defragfn) {
    for (int i = 0; i < 2; ++i) {
      if (tables_[i] == nullptr) continue;
      void *p = defragfn(tables_[i]);
      if (p) tables_[i] = static_cast<BucketType *>(p);
    }
  }

  // Dismiss (madvise DONTNEED for large unused bucket arrays)
  void Dismiss() {
    // Platform-specific: on Linux could call madvise(MADV_DONTNEED)
    // For portability, this is a no-op.
  }

  // ========================================================================
  // Global state (static)
  // ========================================================================

  static void SetHashFunctionSeed(const uint8_t seed[16]) {
    std::memcpy(detail::g_hash_seed, seed, 16);
  }

  static const uint8_t *GetHashSeed() {
    return detail::g_hash_seed;
  }

  static uint64_t GenHashFunction(const char *buf, size_t len) {
    return SipHash(reinterpret_cast<const uint8_t *>(buf), len,
             detail::g_hash_seed);
  }

  static uint64_t GenCaseHashFunction(const char *buf, size_t len) {
    return SipHashNoCase(reinterpret_cast<const uint8_t *>(buf), len,
                detail::g_hash_seed);
  }

  static void SetResizePolicy(ResizePolicy policy) {
    detail::g_resize_policy = policy;
  }

  static ResizePolicy GetResizePolicy() {
    return detail::g_resize_policy;
  }

  static void SetCanAbortShrink(bool can_abort) {
    detail::g_can_abort_shrink = can_abort;
  }

private:
  friend class Iterator;

  // ========================================================================
  // Internal state
  // ========================================================================

  ssize_t rehash_idx_ = -1;
  BucketType *tables_[2] = {nullptr, nullptr};
  size_t used_[2] = {0, 0};
  int8_t bucket_exp_[2] = {-1, -1};
  int16_t pause_rehash_ = 0;
  int16_t pause_auto_shrink_ = 0;
  size_t child_buckets_[2] = {0, 0};
  IterState *safe_iterators_ = nullptr;
  [[no_unique_address]] BucketAllocator bucket_alloc_;

  // Metadata storage (compile-time sized)
  static constexpr size_t kMetadataSize =
    Traits::kMetadataSize > 0 ? Traits::kMetadataSize : 1;
  alignas(alignof(std::max_align_t)) uint8_t metadata_[kMetadataSize] = {};

  // ========================================================================
  // Hash helper
  // ========================================================================

  uint64_t HashKey(const Key &key) const {
    return Traits::Hash(key);
  }

  // ========================================================================
  // Memory management
  // ========================================================================

  BucketType *AllocateBucketArray(size_t count) {
    BucketType *p = bucket_alloc_.allocate(count);
    std::memset(p, 0, count * sizeof(BucketType));
    TrackMemUsage(static_cast<ssize_t>(count * sizeof(BucketType)));
    return p;
  }

  void DeallocateBucketArray(BucketType *p, size_t count) {
    if (!p) return;
    TrackMemUsage(-static_cast<ssize_t>(count * sizeof(BucketType)));
    bucket_alloc_.deallocate(p, count);
  }

  BucketType *AllocateChildBucket() {
    BucketType *p = bucket_alloc_.allocate(1);
    std::memset(p, 0, sizeof(BucketType));
    TrackMemUsage(static_cast<ssize_t>(sizeof(BucketType)));
    return p;
  }

  void DeallocateChildBucket(BucketType *p) {
    if (!p) return;
    TrackMemUsage(-static_cast<ssize_t>(sizeof(BucketType)));
    bucket_alloc_.deallocate(p, 1);
  }

  void TrackMemUsage(ssize_t delta) {
    if constexpr (Traits::kHasTrackMemUsage) {
      Traits::TrackMemUsage(this, delta);
    }
  }

  // Free all child buckets in a table
  void FreeChildBuckets(int table) {
    if (tables_[table] == nullptr) return;
    size_t nb = NumBuckets(bucket_exp_[table]);
    for (size_t i = 0; i < nb; ++i) {
      BucketType *b = &tables_[table][i];
      if (!b->chained) continue;
      BucketType *child = b->ChildBucket();
      b->chained = 0;
      while (child) {
        BucketType *next = child->ChildBucket();
        DeallocateChildBucket(child);
        child_buckets_[table]--;
        child = next;
      }
    }
  }

  // ========================================================================
  // Core lookup: findBucket (SIMD-accelerated)
  // ========================================================================

  BucketType *FindBucket(uint64_t hash, const Key &key,
               int &pos_in_bucket, int *table_index) {
    uint8_t h2 = HighBits(hash);

    RehashStepOnReadIfNeeded();

    for (int table = 0; table <= 1; ++table) {
      if (tables_[table] == nullptr) continue;

      size_t mask = ExpToMask(bucket_exp_[table]);
      size_t idx = hash & mask;

      // Skip already-rehashed buckets in table 0
      if (table == 0 && rehash_idx_ >= 0 &&
        idx < static_cast<size_t>(rehash_idx_)) {
        continue;
      }

      BucketType *b = &tables_[table][idx];
      while (b != nullptr) {
        // SIMD match h2 fingerprints
        uint16_t match_mask = b->MatchHash(h2);
        while (match_mask) {
#if defined(__GNUC__) || defined(__clang__)
          int pos = __builtin_ctz(match_mask);
#else
          int pos = 0;
          uint16_t tmp = match_mask;
          while (!(tmp & 1)) { tmp >>= 1; pos++; }
#endif
          Entry *entry = b->entries[pos];
          const Key &elem_key = Traits::EntryGetKey(entry);
          if (Traits::KeyEqual(key, elem_key)) {
            if constexpr (Traits::kHasValidateEntry) {
              if (!Traits::ValidateEntry(entry)) {
                match_mask &= ~(1 << pos);
                continue;
              }
            }
            pos_in_bucket = pos;
            if (table_index) *table_index = table;
            return b;
          }
          match_mask &= ~(1 << pos);
        }
        b = b->ChildBucket();
      }
    }
    return nullptr;
  }

  // ========================================================================
  // Insert helpers
  // ========================================================================

  BucketType *FindBucketForInsert(uint64_t hash, int &pos_in_bucket,
                   int &table_index) {
    int table = IsRehashing() ? 1 : 0;
    size_t mask = ExpToMask(bucket_exp_[table]);
    size_t idx = hash & mask;
    BucketType *b = &tables_[table][idx];

    while (b->IsFull()) {
      if (!b->chained) {
        ConvertToChained(b, table);
      }
      b = b->ChildBucket();
    }

    pos_in_bucket = b->FindFreePosition();
    assert(pos_in_bucket >= 0);
    table_index = table;
    return b;
  }

  void Insert(uint64_t hash, Entry *entry) {
    ExpandIfNeeded();
    RehashStepOnWriteIfNeeded();

    int pos_in_bucket = 0;
    int table_index = 0;
    BucketType *b = FindBucketForInsert(hash, pos_in_bucket, table_index);

    b->entries[pos_in_bucket] = entry;
    b->hashes[pos_in_bucket] = HighBits(hash);
    b->SetPositionFilled(pos_in_bucket);
    used_[table_index]++;
  }

  // ========================================================================
  // Bucket chain operations
  // ========================================================================

  void ConvertToChained(BucketType *b, int table) {
    assert(!b->chained);
    int last_pos = kEntriesPerBucket - 1;
    assert(b->IsPositionFilled(last_pos));

    BucketType *child = AllocateChildBucket();
    child_buckets_[table]++;

    // Move last entry to child bucket
    BucketType::MoveEntry(*child, 0, *b, last_pos);

    b->chained = 1;
    b->SetChildBucket(child);
  }

  void ConvertToUnchained(BucketType *b) {
    assert(b->chained);
    b->chained = 0;
    assert(!b->IsPositionFilled(kEntriesPerBucket - 1));
  }

  void FillBucketHole(BucketType *b, int pos_in_bucket, int table_index) {
    assert(b->chained && !b->IsPositionFilled(pos_in_bucket));

    // Find the last bucket in chain
    BucketType *before_last = b;
    BucketType *last = b->ChildBucket();
    while (last->chained) {
      before_last = last;
      last = last->ChildBucket();
    }

    // Move entry from last bucket to fill the hole
    if (last->FilledCount() > 0) {
#if defined(__GNUC__) || defined(__clang__)
      int pos_in_last = __builtin_ctz(last->presence);
#else
      int pos_in_last = 0;
      auto p = last->presence;
      while (!(p & 1)) { p >>= 1; pos_in_last++; }
#endif
      BucketType::MoveEntry(*b, pos_in_bucket, *last, pos_in_last);
    }

    // Prune last bucket if empty or near-empty
    if (last->FilledCount() <= 1) {
      PruneLastBucket(before_last, last, table_index);
    }
  }

  void PruneLastBucket(BucketType *before_last, BucketType *last,
             int table_index) {
    assert(before_last->chained);
    assert(!last->chained);

    ConvertToUnchained(before_last);

    // Move any remaining entry in last to before_last's freed slot
    if (last->FilledCount() > 0) {
#if defined(__GNUC__) || defined(__clang__)
      int pos_in_last = __builtin_ctz(last->presence);
#else
      int pos_in_last = 0;
      auto p = last->presence;
      while (!(p & 1)) { p >>= 1; pos_in_last++; }
#endif
      BucketType::MoveEntry(*before_last, kEntriesPerBucket - 1,
                  *last, pos_in_last);
    }

    DeallocateChildBucket(last);
    child_buckets_[table_index]--;
  }

  void CompactBucketChain(long bucket_index, int table_index) {
    if (tables_[table_index] == nullptr) return;
    BucketType *b = &tables_[table_index][bucket_index];

    while (b->chained) {
      BucketType *child = b->ChildBucket();

      // Remove empty middle buckets
      if (child->chained && child->FilledCount() == 0) {
        BucketType *grandchild = child->ChildBucket();
        b->SetChildBucket(grandchild);
        DeallocateChildBucket(child);
        child_buckets_[table_index]--;
        continue;
      }

      // Prune near-empty last bucket
      if (!child->chained && child->FilledCount() <= 1) {
        PruneLastBucket(b, child, table_index);
        return;
      }

      // Fill holes in current bucket from chain tail
      int n = b->NumPositions();
      bool filled_hole = false;
      for (int pos = 0; pos < n; ++pos) {
        if (!b->IsPositionFilled(pos)) {
          FillBucketHole(b, pos, table_index);
          filled_hole = true;
          if (!b->chained) return;
        }
      }

      if (!filled_hole) {
        b = child;
      }
    }
  }

  // ========================================================================
  // Resize / Rehash internals
  // ========================================================================

  bool Resize(size_t min_capacity) {
    assert(!IsRehashing());

    if (min_capacity == 0) min_capacity = 1;
    int8_t exp = NextBucketExp(min_capacity);
    size_t nb = NumBuckets(exp);
    size_t new_capacity = nb * kEntriesPerBucket;

    if (new_capacity < min_capacity) return false;
    if (exp == bucket_exp_[0]) return false;

    if (detail::g_resize_policy == ResizePolicy::Forbid &&
      tables_[0] != nullptr) {
      return false;
    }

    // Check with callback if expansion is allowed
    if constexpr (Traits::kHasResizeAllowed) {
      if (exp > bucket_exp_[0] && tables_[0] != nullptr) {
        double fill = static_cast<double>(min_capacity) /
                (NumBuckets(bucket_exp_[0]) * kEntriesPerBucket);
        if (fill * 100 < kMaxFillPercentHard &&
          !Traits::ResizeAllowed(nb * sizeof(BucketType), fill)) {
          return false;
        }
      }
    }

    BucketType *new_table = AllocateBucketArray(nb);

    bucket_exp_[1] = exp;
    tables_[1] = new_table;
    used_[1] = 0;
    child_buckets_[1] = 0;
    rehash_idx_ = 0;

    if constexpr (Traits::kHasRehashingStarted) {
      Traits::RehashingStarted(this);
    }

    // If old table is empty, complete immediately
    if (tables_[0] == nullptr ||
      (used_[0] == 0 && child_buckets_[0] == 0)) {
      OnRehashingCompleted();
    } else if constexpr (Traits::kInstantRehashing) {
      while (IsRehashing()) {
        RehashStep();
      }
    }

    return true;
  }

  void RehashStep() {
    assert(IsRehashing());
    if (pause_rehash_ > 0) return;

    if (bucket_exp_[1] < bucket_exp_[0]) {
      RehashStepShrink();
    } else {
      RehashStepExpand();
    }
  }

  void RehashStepExpand() {
    Entry *entry_buf[kFetchEntryBufferSize];
    const Key *key_buf[kFetchEntryBufferSize];
    size_t idx = static_cast<size_t>(rehash_idx_);

    BucketType *b = &tables_[0][idx];

    // Process bucket chain in batches of kFetchBucketCountOnExpand buckets
    BucketType *scan = b;
    int total_moved = 0;

    while (scan != nullptr) {
      int buf_size = 0;
      int buckets_fetched = 0;

      // Fetch up to kFetchBucketCountOnExpand buckets into buffer
      while (scan != nullptr &&
           buckets_fetched < kFetchBucketCountOnExpand) {
        for (int pos = 0; pos < scan->NumPositions(); ++pos) {
          if (scan->IsPositionFilled(pos)) {
            entry_buf[buf_size++] = scan->entries[pos];
          }
        }
        BucketType *next = scan->ChildBucket();
        if (scan != b) {
          DeallocateChildBucket(scan);
          child_buckets_[0]--;
        }
        scan = next;
        buckets_fetched++;
      }

      // Extract keys (no loop-carried dependency for ILP)
      for (int i = 0; i < buf_size; ++i) {
        key_buf[i] = &Traits::EntryGetKey(entry_buf[i]);
      }

      // Hash and reinsert into table 1
      for (int i = 0; i < buf_size; ++i) {
        uint64_t h = HashKey(*key_buf[i]);
        uint8_t h2 = HighBits(h);
        RehashInsert(entry_buf[i], h, h2);
      }

      total_moved += buf_size;
    }

    // Clean up source bucket
    b->Reset();
    used_[0] -= total_moved;

    RehashStepFinalize();
  }

  void RehashStepShrink() {
    // Skip empty buckets (up to 10)
    int empty_visits = 10;
    while (true) {
      size_t idx = static_cast<size_t>(rehash_idx_);
      BucketType *b = &tables_[0][idx];
      if (b->FilledCount() > 0 || b->chained) break;
      RehashStepFinalize();
      if (!IsRehashing()) return;
      if (--empty_visits == 0) return;
    }

    size_t idx = static_cast<size_t>(rehash_idx_);
    BucketType *b = &tables_[0][idx];

    // Rehash all entries using bucket index as hash (no recomputation)
    while (b != nullptr) {
      for (int pos = 0; pos < b->NumPositions(); ++pos) {
        if (b->IsPositionFilled(pos)) {
          // For shrink, the hash doesn't need recomputation.
          // The bucket index determines where entries go.
          uint8_t h2 = b->hashes[pos];
          size_t mask1 = ExpToMask(bucket_exp_[1]);
          size_t target_idx = idx & mask1;

          // Insert into table 1
          BucketType *target = &tables_[1][target_idx];
          while (target->IsFull()) {
            if (!target->chained) {
              ConvertToChained(target, 1);
            }
            target = target->ChildBucket();
          }
          int free_pos = target->FindFreePosition();
          target->entries[free_pos] = b->entries[pos];
          target->hashes[free_pos] = h2;
          target->SetPositionFilled(free_pos);
          used_[1]++;
          used_[0]--;
        }
      }
      BucketType *next = b->ChildBucket();
      if (b != &tables_[0][idx]) {
        DeallocateChildBucket(b);
        child_buckets_[0]--;
      }
      b = next;
    }

    // Clean up source bucket
    tables_[0][idx].Reset();
    RehashStepFinalize();
  }

  void RehashInsert(Entry *entry, uint64_t hash, uint8_t h2) {
    size_t mask = ExpToMask(bucket_exp_[1]);
    size_t idx = hash & mask;
    BucketType *b = &tables_[1][idx];

    while (b->IsFull()) {
      if (!b->chained) {
        ConvertToChained(b, 1);
      }
      b = b->ChildBucket();
    }

    int pos = b->FindFreePosition();
    b->entries[pos] = entry;
    b->hashes[pos] = h2;
    b->SetPositionFilled(pos);
    used_[1]++;
  }

  void RehashStepFinalize() {
    rehash_idx_++;
    if (static_cast<size_t>(rehash_idx_) >= NumBuckets(bucket_exp_[0])) {
      OnRehashingCompleted();
    }
  }

  void OnRehashingCompleted() {
    if constexpr (Traits::kHasRehashingCompleted) {
      Traits::RehashingCompleted(this);
    }

    if (tables_[0]) {
      DeallocateBucketArray(tables_[0], NumBuckets(bucket_exp_[0]));
    }

    // Swap tables: table 1 becomes table 0
    tables_[0] = tables_[1];
    tables_[1] = nullptr;
    bucket_exp_[0] = bucket_exp_[1];
    bucket_exp_[1] = -1;
    used_[0] += used_[1]; // used_[0] should be 0 at this point
    used_[1] = 0;
    child_buckets_[0] = child_buckets_[1];
    child_buckets_[1] = 0;
    rehash_idx_ = -1;
  }

  bool AbortShrinkIfNeeded() {
    if (!detail::g_can_abort_shrink) return false;
    if (!IsRehashing() || bucket_exp_[1] >= bucket_exp_[0]) return false;
    if (safe_iterators_) return false;

    size_t num_elements = used_[0] + used_[1] + 1;
    size_t ht1_capacity = NumBuckets(bucket_exp_[1]) * kEntriesPerBucket;

    if (num_elements * 100 <= ht1_capacity * kMaxFillPercentHard) {
      return false;
    }

    // Swap tables: abort shrink, convert to expand
    std::swap(tables_[0], tables_[1]);
    std::swap(bucket_exp_[0], bucket_exp_[1]);
    std::swap(used_[0], used_[1]);
    std::swap(child_buckets_[0], child_buckets_[1]);
    rehash_idx_ = 0;
    return true;
  }

  // Rehash step triggers based on resize policy
  void RehashStepOnReadIfNeeded() {
    if (!IsRehashing() || pause_rehash_ > 0) return;
    if (detail::g_resize_policy != ResizePolicy::Allow) return;
    RehashStep();
  }

  void RehashStepOnWriteIfNeeded() {
    if (!IsRehashing() || pause_rehash_ > 0) return;
    if (detail::g_resize_policy != ResizePolicy::Avoid) return;
    RehashStep();
  }

  // ========================================================================
  // Rehash pause/resume
  // ========================================================================

  void PauseRehashing() { ++pause_rehash_; }
  void ResumeRehashing() { --pause_rehash_; }

  // ========================================================================
  // Safe iterator tracking
  // ========================================================================

  void TrackSafeIterator(IterState *it) {
    assert(it->next_safe_iter == nullptr);
    it->next_safe_iter = safe_iterators_;
    safe_iterators_ = it;
  }

  void UntrackSafeIterator(IterState *it) {
    if (safe_iterators_ == it) {
      safe_iterators_ = it->next_safe_iter;
    } else {
      IterState *current = safe_iterators_;
      while (current && current->next_safe_iter != it) {
        current = current->next_safe_iter;
      }
      if (current) {
        current->next_safe_iter = it->next_safe_iter;
      }
    }
    it->next_safe_iter = nullptr;
  }

  void InvalidateAllSafeIterators() {
    while (safe_iterators_) {
      IterState *next = safe_iterators_->next_safe_iter;
      safe_iterators_->hashtable = nullptr;
      safe_iterators_->next_safe_iter = nullptr;
      safe_iterators_ = next;
    }
  }

  // ========================================================================
  // Fingerprint (for unsafe iterator integrity check)
  // ========================================================================

  uint64_t Fingerprint() const {
    uint64_t integers[6] = {
      reinterpret_cast<uint64_t>(tables_[0]),
      reinterpret_cast<uint64_t>(tables_[1]),
      static_cast<uint64_t>(bucket_exp_[0]),
      static_cast<uint64_t>(bucket_exp_[1]),
      used_[0],
      used_[1],
    };
    uint64_t hash = 0;
    for (auto v : integers) {
      // Simple hash combine
      hash ^= v;
      hash *= UINT64_C(6364136223846793005);
      hash += UINT64_C(1442695040888963407);
    }
    return hash;
  }

  // ========================================================================
  // Scan helper
  // ========================================================================

  void ScanBucket(BucketType *table, size_t idx, const ScanCallback &fn,
          const std::function<void *(void *)> &defragfn,
          bool emit_ref) const {
    if (table == nullptr) return;
    BucketType *b = &table[idx];
    while (b != nullptr) {
      if (fn) {
        for (int pos = 0; pos < kEntriesPerBucket; ++pos) {
          if (b->IsPositionFilled(pos)) {
            if constexpr (Traits::kHasValidateEntry) {
              if (!Traits::ValidateEntry(b->entries[pos]))
                continue;
            }
            if (emit_ref)
              fn(static_cast<void *>(&b->entries[pos]));
            else
              fn(static_cast<void *>(b->entries[pos]));
          }
        }
      }
      BucketType *next = b->ChildBucket();
      if (next && defragfn) {
        void *reallocated = defragfn(next);
        if (reallocated) {
          next = static_cast<BucketType *>(reallocated);
          b->SetChildBucket(next);
        }
      }
      b = next;
    }
  }
};

} // namespace cancer_redis
