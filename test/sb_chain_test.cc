#include <gtest/gtest.h>
#include "sb_chain.h"

#include <string>
#include <vector>

TEST(ScalingBloomTest, NewAndFree) {
  auto* filter = ScalingBloomFilter::New(1000, 0.01, kUse64Bit | kNoRound, 2);
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(filter->numLayers, 1u);
  EXPECT_EQ(filter->totalItems, 0u);
  EXPECT_EQ(filter->expansionFactor, 2u);
  EXPECT_GT(filter->TotalCapacity(), 0u);
  filter->Free();
}

TEST(ScalingBloomTest, PutAndContains) {
  auto* filter = ScalingBloomFilter::New(1000, 0.01, kUse64Bit | kNoRound, 2);
  ASSERT_NE(filter, nullptr);

  EXPECT_EQ(filter->Put("hello", 5), 1);
  EXPECT_EQ(filter->Put("hello", 5), 0);
  EXPECT_EQ(filter->Contains("hello", 5), 1);
  EXPECT_EQ(filter->totalItems, 1u);

  filter->Free();
}

TEST(ScalingBloomTest, NoFalseNegatives) {
  auto* filter = ScalingBloomFilter::New(5000, 0.01, kUse64Bit | kNoRound, 2);
  ASSERT_NE(filter, nullptr);

  std::vector<std::string> items;
  for (int i = 0; i < 5000; i++) {
    items.push_back("item_" + std::to_string(i));
  }

  for (const auto& item : items) {
    filter->Put(item.c_str(), item.size());
  }

  for (const auto& item : items) {
    EXPECT_EQ(filter->Contains(item.c_str(), item.size()), 1)
      << "False negative for " << item;
  }

  filter->Free();
}

TEST(ScalingBloomTest, AutoExpansion) {
  auto* filter = ScalingBloomFilter::New(100, 0.01, kUse64Bit | kNoRound, 2);
  ASSERT_NE(filter, nullptr);

  for (int i = 0; i < 500; i++) {
    std::string item = "expand_" + std::to_string(i);
    filter->Put(item.c_str(), item.size());
  }

  EXPECT_GT(filter->numLayers, 1u);

  for (int i = 0; i < 500; i++) {
    std::string item = "expand_" + std::to_string(i);
    EXPECT_EQ(filter->Contains(item.c_str(), item.size()), 1)
      << "False negative after expansion for " << item;
  }

  filter->Free();
}

TEST(ScalingBloomTest, FixedSizeRejectsOverflow) {
  auto* filter = ScalingBloomFilter::New(100, 0.01,
                                          kUse64Bit | kNoRound | kFixedSize, 2);
  ASSERT_NE(filter, nullptr);

  int inserted = 0;
  for (int i = 0; i < 200; i++) {
    std::string item = "fixed_" + std::to_string(i);
    int rv = filter->Put(item.c_str(), item.size());
    if (rv == -1) break;
    if (rv == 1) inserted++;
  }

  EXPECT_EQ(filter->numLayers, 1u);
  EXPECT_LE(inserted, 100);

  filter->Free();
}

TEST(ScalingBloomTest, TotalCapacity) {
  auto* filter = ScalingBloomFilter::New(1000, 0.01, kUse64Bit | kNoRound, 2);
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(filter->TotalCapacity(), 1000u);
  filter->Free();
}

TEST(ScalingBloomTest, BytesUsed) {
  auto* filter = ScalingBloomFilter::New(1000, 0.01, kUse64Bit | kNoRound, 2);
  ASSERT_NE(filter, nullptr);
  EXPECT_GT(filter->BytesUsed(), sizeof(ScalingBloomFilter));
  filter->Free();
}

TEST(ScalingBloomTest, SerializeDeserializeHeader) {
  auto* filter = ScalingBloomFilter::New(1000, 0.01, kUse64Bit | kNoRound, 2);
  ASSERT_NE(filter, nullptr);

  for (int i = 0; i < 100; i++) {
    std::string item = "ser_" + std::to_string(i);
    filter->Put(item.c_str(), item.size());
  }

  size_t hdrSize = ComputeHeaderSize(filter);
  std::vector<char> buf(hdrSize);
  SerializeHeader(filter, buf.data());

  auto* restored = DeserializeHeader(buf.data(), buf.size());
  ASSERT_NE(restored, nullptr);

  EXPECT_EQ(restored->totalItems, filter->totalItems);
  EXPECT_EQ(restored->numLayers, filter->numLayers);
  EXPECT_EQ(restored->flags, filter->flags);
  EXPECT_EQ(restored->expansionFactor, filter->expansionFactor);

  for (size_t i = 0; i < restored->numLayers; i++) {
    EXPECT_EQ(restored->layers[i].bloom.capacity, filter->layers[i].bloom.capacity);
    EXPECT_EQ(restored->layers[i].bloom.hashCount, filter->layers[i].bloom.hashCount);
    EXPECT_EQ(restored->layers[i].bloom.totalBits, filter->layers[i].bloom.totalBits);
  }

  restored->Free();
  filter->Free();
}
