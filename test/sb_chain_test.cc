#include <gtest/gtest.h>
#include "sb_chain.h"

#include <string>
#include <vector>

static std::span<const std::byte> ToSpan(const std::string& s) {
  return AsBytes(s.data(), s.size());
}

static auto DefaultFlags() {
  return BloomFlags::Use64Bit | BloomFlags::NoRound;
}

TEST(ScalingBloomTest, ConstructAndDestruct) {
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(1000, 0.01, DefaultFlags(), 2);
  EXPECT_TRUE(mem->IsValid());
  EXPECT_EQ(mem->NumLayers(), 1u);
  EXPECT_EQ(mem->TotalItems(), 0u);
  EXPECT_EQ(mem->ExpansionFactor(), 2u);
  EXPECT_GT(mem->TotalCapacity(), 0u);
  mem->~ScalingBloomFilter();
  free(mem);
}

TEST(ScalingBloomTest, PutAndContains) {
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(1000, 0.01, DefaultFlags(), 2);

  auto r1 = mem->Put(AsBytes("hello", 5));
  ASSERT_TRUE(r1.has_value());
  EXPECT_TRUE(*r1);

  auto r2 = mem->Put(AsBytes("hello", 5));
  ASSERT_TRUE(r2.has_value());
  EXPECT_FALSE(*r2);

  EXPECT_TRUE(mem->Contains(AsBytes("hello", 5)));
  EXPECT_EQ(mem->TotalItems(), 1u);

  mem->~ScalingBloomFilter();
  free(mem);
}

TEST(ScalingBloomTest, NoFalseNegatives) {
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(5000, 0.01, DefaultFlags(), 2);

  std::vector<std::string> items;
  for (int i = 0; i < 5000; i++) {
    items.push_back("item_" + std::to_string(i));
  }
  for (const auto& item : items) mem->Put(ToSpan(item));
  for (const auto& item : items) {
    EXPECT_TRUE(mem->Contains(ToSpan(item))) << "False negative for " << item;
  }

  mem->~ScalingBloomFilter();
  free(mem);
}

TEST(ScalingBloomTest, AutoExpansion) {
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(100, 0.01, DefaultFlags(), 2);

  for (int i = 0; i < 500; i++) {
    auto item = "expand_" + std::to_string(i);
    mem->Put(ToSpan(item));
  }
  EXPECT_GT(mem->NumLayers(), 1u);

  for (int i = 0; i < 500; i++) {
    auto item = "expand_" + std::to_string(i);
    EXPECT_TRUE(mem->Contains(ToSpan(item)));
  }

  mem->~ScalingBloomFilter();
  free(mem);
}

TEST(ScalingBloomTest, FixedSizeRejectsOverflow) {
  auto flg = DefaultFlags() | BloomFlags::FixedSize;
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(100, 0.01, flg, 2);

  int inserted = 0;
  for (int i = 0; i < 200; i++) {
    auto item = "fixed_" + std::to_string(i);
    auto result = mem->Put(ToSpan(item));
    if (!result.has_value()) break;
    if (*result) inserted++;
  }
  EXPECT_EQ(mem->NumLayers(), 1u);
  EXPECT_LE(inserted, 100);

  mem->~ScalingBloomFilter();
  free(mem);
}

TEST(ScalingBloomTest, OptionalReturnSemantics) {
  auto flg = DefaultFlags() | BloomFlags::FixedSize;
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(2, 0.01, flg, 2);

  auto r1 = mem->Put(AsBytes("a", 1));
  EXPECT_TRUE(r1.has_value());
  EXPECT_TRUE(*r1);

  auto r2 = mem->Put(AsBytes("a", 1));
  EXPECT_TRUE(r2.has_value());
  EXPECT_FALSE(*r2);

  mem->Put(AsBytes("b", 1));
  auto r3 = mem->Put(AsBytes("c", 1));
  EXPECT_FALSE(r3.has_value());

  mem->~ScalingBloomFilter();
  free(mem);
}

TEST(ScalingBloomTest, TotalCapacity) {
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(1000, 0.01, DefaultFlags(), 2);
  EXPECT_EQ(mem->TotalCapacity(), 1000u);
  mem->~ScalingBloomFilter();
  free(mem);
}

TEST(ScalingBloomTest, BytesUsed) {
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(1000, 0.01, DefaultFlags(), 2);
  EXPECT_GT(mem->BytesUsed(), sizeof(ScalingBloomFilter));
  mem->~ScalingBloomFilter();
  free(mem);
}

TEST(ScalingBloomTest, SerializeDeserializeHeader) {
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(1000, 0.01, DefaultFlags(), 2);

  for (int i = 0; i < 100; i++) {
    auto item = "ser_" + std::to_string(i);
    mem->Put(ToSpan(item));
  }

  size_t hdrSize = ComputeHeaderSize(*mem);
  std::vector<char> buf(hdrSize);
  SerializeHeader(*mem, buf.data());

  auto* restored = DeserializeHeader(buf.data(), buf.size());
  ASSERT_NE(restored, nullptr);

  EXPECT_EQ(restored->TotalItems(), mem->TotalItems());
  EXPECT_EQ(restored->NumLayers(), mem->NumLayers());
  EXPECT_EQ(ToUnderlying(restored->Flags()), ToUnderlying(mem->Flags()));
  EXPECT_EQ(restored->ExpansionFactor(), mem->ExpansionFactor());

  for (size_t i = 0; i < restored->NumLayers(); i++) {
    EXPECT_EQ(restored->Layers()[i].bloom.GetCapacity(), mem->Layers()[i].bloom.GetCapacity());
    EXPECT_EQ(restored->Layers()[i].bloom.GetHashCount(), mem->Layers()[i].bloom.GetHashCount());
    EXPECT_EQ(restored->Layers()[i].bloom.GetTotalBits(), mem->Layers()[i].bloom.GetTotalBits());
  }

  restored->~ScalingBloomFilter();
  free(restored);
  mem->~ScalingBloomFilter();
  free(mem);
}

TEST(ScalingBloomTest, SpanInterface) {
  auto* mem = static_cast<ScalingBloomFilter*>(malloc(sizeof(ScalingBloomFilter)));
  new (mem) ScalingBloomFilter(1000, 0.01, DefaultFlags(), 2);

  std::string data = "test_span";
  auto result = mem->Put(ToSpan(data));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(*result);
  EXPECT_TRUE(mem->Contains(ToSpan(data)));

  mem->~ScalingBloomFilter();
  free(mem);
}
