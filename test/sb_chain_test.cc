#include <gtest/gtest.h>
#include "sb_chain.h"

#include <string>
#include <vector>

TEST(SBChainTest, CreateAndDestroy) {
  auto* chain = SBChain::Create(1000, 0.01,
                                 kBloomOptForce64 | kBloomOptNoRound, 2);
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->nfilters, 1u);
  EXPECT_EQ(chain->size, 0u);
  EXPECT_EQ(chain->growth, 2u);
  EXPECT_GT(chain->Capacity(), 0u);
  chain->Destroy();
}

TEST(SBChainTest, AddAndCheck) {
  auto* chain = SBChain::Create(1000, 0.01,
                                 kBloomOptForce64 | kBloomOptNoRound, 2);
  ASSERT_NE(chain, nullptr);

  EXPECT_EQ(chain->Add("hello", 5), 1);   // New item
  EXPECT_EQ(chain->Add("hello", 5), 0);   // Duplicate
  EXPECT_EQ(chain->Check("hello", 5), 1);
  EXPECT_EQ(chain->size, 1u);

  chain->Destroy();
}

TEST(SBChainTest, NoFalseNegatives) {
  auto* chain = SBChain::Create(5000, 0.01,
                                 kBloomOptForce64 | kBloomOptNoRound, 2);
  ASSERT_NE(chain, nullptr);

  std::vector<std::string> items;
  for (int i = 0; i < 5000; i++) {
    items.push_back("item_" + std::to_string(i));
  }

  for (const auto& item : items) {
    chain->Add(item.c_str(), item.size());
  }

  for (const auto& item : items) {
    EXPECT_EQ(chain->Check(item.c_str(), item.size()), 1)
      << "False negative for " << item;
  }

  chain->Destroy();
}

TEST(SBChainTest, Scaling) {
  // Small initial capacity to force scaling
  auto* chain = SBChain::Create(100, 0.01,
                                 kBloomOptForce64 | kBloomOptNoRound, 2);
  ASSERT_NE(chain, nullptr);

  for (int i = 0; i < 500; i++) {
    std::string item = "scale_" + std::to_string(i);
    chain->Add(item.c_str(), item.size());
  }

  EXPECT_GT(chain->nfilters, 1u) << "Should have scaled to multiple filters";

  // Verify all items still exist
  for (int i = 0; i < 500; i++) {
    std::string item = "scale_" + std::to_string(i);
    EXPECT_EQ(chain->Check(item.c_str(), item.size()), 1)
      << "False negative after scaling for " << item;
  }

  chain->Destroy();
}

TEST(SBChainTest, NonScaling) {
  auto* chain = SBChain::Create(100, 0.01,
                                 kBloomOptForce64 | kBloomOptNoRound | kBloomOptNoScaling, 2);
  ASSERT_NE(chain, nullptr);

  // Fill up the filter
  int added = 0;
  for (int i = 0; i < 200; i++) {
    std::string item = "ns_" + std::to_string(i);
    int rv = chain->Add(item.c_str(), item.size());
    if (rv == -1) break;
    if (rv == 1) added++;
  }

  EXPECT_EQ(chain->nfilters, 1u);
  EXPECT_LE(added, 100);

  chain->Destroy();
}

TEST(SBChainTest, Capacity) {
  auto* chain = SBChain::Create(1000, 0.01,
                                 kBloomOptForce64 | kBloomOptNoRound, 2);
  ASSERT_NE(chain, nullptr);
  EXPECT_EQ(chain->Capacity(), 1000u);
  chain->Destroy();
}

TEST(SBChainTest, MemUsage) {
  auto* chain = SBChain::Create(1000, 0.01,
                                 kBloomOptForce64 | kBloomOptNoRound, 2);
  ASSERT_NE(chain, nullptr);
  EXPECT_GT(chain->MemUsage(), sizeof(SBChain));
  chain->Destroy();
}

TEST(SBChainTest, DumpAndLoadHeader) {
  auto* chain = SBChain::Create(1000, 0.01,
                                 kBloomOptForce64 | kBloomOptNoRound, 2);
  ASSERT_NE(chain, nullptr);

  for (int i = 0; i < 100; i++) {
    std::string item = "dump_" + std::to_string(i);
    chain->Add(item.c_str(), item.size());
  }

  size_t hdrSize = SBChainDumpHeaderSize(chain);
  std::vector<char> buf(hdrSize);
  SBChainDumpHeader(chain, buf.data());

  auto* loaded = SBChainLoadHeader(buf.data(), buf.size());
  ASSERT_NE(loaded, nullptr);

  EXPECT_EQ(loaded->size, chain->size);
  EXPECT_EQ(loaded->nfilters, chain->nfilters);
  EXPECT_EQ(loaded->options, chain->options);
  EXPECT_EQ(loaded->growth, chain->growth);

  for (size_t i = 0; i < loaded->nfilters; i++) {
    EXPECT_EQ(loaded->filters[i].inner.entries, chain->filters[i].inner.entries);
    EXPECT_EQ(loaded->filters[i].inner.hashes, chain->filters[i].inner.hashes);
    EXPECT_EQ(loaded->filters[i].inner.bits, chain->filters[i].inner.bits);
  }

  loaded->Destroy();
  chain->Destroy();
}
