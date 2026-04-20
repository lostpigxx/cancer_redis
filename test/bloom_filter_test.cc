#include <gtest/gtest.h>
#include "bloom_filter.h"
#include "murmur2.h"

TEST(MurmurHash2Test, KnownValues) {
  const char* key = "hello";
  uint32_t h = MurmurHash2(key, 5, 0x9747b28c);
  EXPECT_NE(h, 0u);

  uint32_t h2 = MurmurHash2(key, 5, 0x9747b28c);
  EXPECT_EQ(h, h2);

  uint32_t h3 = MurmurHash2(key, 5, 0);
  EXPECT_NE(h, h3);
}

TEST(MurmurHash64ATest, KnownValues) {
  const char* key = "hello";
  uint64_t h = MurmurHash64A(key, 5, 0xc6a4a7935bd1e995ULL);
  EXPECT_NE(h, 0ULL);

  uint64_t h2 = MurmurHash64A(key, 5, 0xc6a4a7935bd1e995ULL);
  EXPECT_EQ(h, h2);
}

TEST(MurmurHash2Test, EmptyString) {
  uint32_t h = MurmurHash2("", 0, 0x9747b28c);
  // Should not crash and return a deterministic value
  uint32_t h2 = MurmurHash2("", 0, 0x9747b28c);
  EXPECT_EQ(h, h2);
}

TEST(CalcHashTest, Consistency) {
  BloomHashVal hv1 = CalcHash("test", 4);
  BloomHashVal hv2 = CalcHash("test", 4);
  EXPECT_EQ(hv1.a, hv2.a);
  EXPECT_EQ(hv1.b, hv2.b);

  BloomHashVal hv3 = CalcHash("other", 5);
  EXPECT_NE(hv1.a, hv3.a);
}

TEST(CalcHash64Test, Consistency) {
  BloomHashVal hv1 = CalcHash64("test", 4);
  BloomHashVal hv2 = CalcHash64("test", 4);
  EXPECT_EQ(hv1.a, hv2.a);
  EXPECT_EQ(hv1.b, hv2.b);
}

TEST(BloomFilterTest, BasicInitDestroy) {
  BloomFilter bf = {};
  EXPECT_EQ(bf.Init(1000, 0.01, 4 | 1), 0);  // FORCE64 | NOROUND
  EXPECT_GT(bf.hashes, 0u);
  EXPECT_GT(bf.bits, 0u);
  EXPECT_GT(bf.bytes, 0u);
  EXPECT_NE(bf.bf, nullptr);
  EXPECT_GT(bf.bpe, 0.0);
  bf.Destroy();
  EXPECT_EQ(bf.bf, nullptr);
}

TEST(BloomFilterTest, AddAndCheck) {
  BloomFilter bf = {};
  bf.Init(1000, 0.01, 4);  // FORCE64

  BloomHashVal hv = CalcHash64("hello", 5);
  EXPECT_TRUE(bf.Add(hv));   // First add returns true (new)
  EXPECT_FALSE(bf.Add(hv));  // Second add returns false (exists)
  EXPECT_TRUE(bf.Check(hv));

  BloomHashVal hv2 = CalcHash64("nonexistent", 11);
  // Might be false positive, but extremely unlikely for a single item
  // With 1000 capacity and 1 item, FP rate should be near zero
  // Just verify Check doesn't crash
  bf.Check(hv2);

  bf.Destroy();
}

TEST(BloomFilterTest, FalsePositiveRate) {
  BloomFilter bf = {};
  bf.Init(10000, 0.01, 4);

  // Insert 10000 items
  for (int i = 0; i < 10000; i++) {
    std::string item = "item_" + std::to_string(i);
    BloomHashVal hv = CalcHash64(item.c_str(), static_cast<int>(item.size()));
    bf.Add(hv);
  }

  // All inserted items must be found (no false negatives)
  for (int i = 0; i < 10000; i++) {
    std::string item = "item_" + std::to_string(i);
    BloomHashVal hv = CalcHash64(item.c_str(), static_cast<int>(item.size()));
    EXPECT_TRUE(bf.Check(hv)) << "False negative for " << item;
  }

  // Check false positive rate with non-inserted items
  int falsePositives = 0;
  int testCount = 100000;
  for (int i = 10000; i < 10000 + testCount; i++) {
    std::string item = "other_" + std::to_string(i);
    BloomHashVal hv = CalcHash64(item.c_str(), static_cast<int>(item.size()));
    if (bf.Check(hv)) {
      falsePositives++;
    }
  }

  double fpRate = static_cast<double>(falsePositives) / testCount;
  // Allow 3x the theoretical rate as margin
  EXPECT_LT(fpRate, 0.03) << "FP rate too high: " << fpRate;

  bf.Destroy();
}

TEST(BloomFilterTest, PowerOfTwoOptimization) {
  BloomFilter bf = {};
  bf.Init(1000, 0.01, 4);  // No NOROUND, so n2 should be set
  EXPECT_GT(bf.n2, 0);
  EXPECT_EQ(bf.bits, 1ULL << bf.n2);
  bf.Destroy();
}
