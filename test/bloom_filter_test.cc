#include <gtest/gtest.h>
#include "bloom_filter.h"
#include "murmur2.h"

TEST(MurmurHash2Test, Deterministic) {
  const char* key = "hello";
  uint32_t h1 = MurmurHash2(key, 5, 0x9747b28c);
  uint32_t h2 = MurmurHash2(key, 5, 0x9747b28c);
  EXPECT_NE(h1, 0u);
  EXPECT_EQ(h1, h2);

  uint32_t h3 = MurmurHash2(key, 5, 0);
  EXPECT_NE(h1, h3);
}

TEST(MurmurHash64ATest, Deterministic) {
  const char* key = "hello";
  uint64_t h1 = MurmurHash64A(key, 5, 0xc6a4a7935bd1e995ULL);
  uint64_t h2 = MurmurHash64A(key, 5, 0xc6a4a7935bd1e995ULL);
  EXPECT_NE(h1, 0ULL);
  EXPECT_EQ(h1, h2);
}

TEST(MurmurHash2Test, EmptyInput) {
  uint32_t h1 = MurmurHash2("", 0, 0x9747b28c);
  uint32_t h2 = MurmurHash2("", 0, 0x9747b28c);
  EXPECT_EQ(h1, h2);
}

TEST(HashPairTest, Consistency32) {
  HashPair a = ComputeHash32("test", 4);
  HashPair b = ComputeHash32("test", 4);
  EXPECT_EQ(a.primary, b.primary);
  EXPECT_EQ(a.secondary, b.secondary);

  HashPair c = ComputeHash32("other", 5);
  EXPECT_NE(a.primary, c.primary);
}

TEST(HashPairTest, Consistency64) {
  HashPair a = ComputeHash64("test", 4);
  HashPair b = ComputeHash64("test", 4);
  EXPECT_EQ(a.primary, b.primary);
  EXPECT_EQ(a.secondary, b.secondary);
}

TEST(BloomLayerTest, SetupTeardown) {
  BloomLayer layer = {};
  EXPECT_EQ(layer.Setup(1000, 0.01, 4 | 1), 0);
  EXPECT_GT(layer.hashCount, 0u);
  EXPECT_GT(layer.totalBits, 0u);
  EXPECT_GT(layer.dataSize, 0u);
  EXPECT_NE(layer.bitArray, nullptr);
  EXPECT_GT(layer.bitsPerEntry, 0.0);
  layer.Teardown();
  EXPECT_EQ(layer.bitArray, nullptr);
}

TEST(BloomLayerTest, InsertAndTest) {
  BloomLayer layer = {};
  layer.Setup(1000, 0.01, 4);

  HashPair hp = ComputeHash64("hello", 5);
  EXPECT_TRUE(layer.Insert(hp));
  EXPECT_FALSE(layer.Insert(hp));
  EXPECT_TRUE(layer.Test(hp));

  layer.Teardown();
}

TEST(BloomLayerTest, FalsePositiveRate) {
  BloomLayer layer = {};
  layer.Setup(10000, 0.01, 4);

  for (int i = 0; i < 10000; i++) {
    std::string item = "item_" + std::to_string(i);
    HashPair hp = ComputeHash64(item.c_str(), static_cast<int>(item.size()));
    layer.Insert(hp);
  }

  for (int i = 0; i < 10000; i++) {
    std::string item = "item_" + std::to_string(i);
    HashPair hp = ComputeHash64(item.c_str(), static_cast<int>(item.size()));
    EXPECT_TRUE(layer.Test(hp)) << "False negative for " << item;
  }

  int falsePositives = 0;
  int testCount = 100000;
  for (int i = 10000; i < 10000 + testCount; i++) {
    std::string item = "other_" + std::to_string(i);
    HashPair hp = ComputeHash64(item.c_str(), static_cast<int>(item.size()));
    if (layer.Test(hp)) falsePositives++;
  }

  double fpRate = static_cast<double>(falsePositives) / testCount;
  EXPECT_LT(fpRate, 0.03) << "FP rate too high: " << fpRate;

  layer.Teardown();
}

TEST(BloomLayerTest, PowerOfTwoSizing) {
  BloomLayer layer = {};
  layer.Setup(1000, 0.01, 4);
  EXPECT_GT(layer.log2Bits, 0);
  EXPECT_EQ(layer.totalBits, 1ULL << layer.log2Bits);
  layer.Teardown();
}
