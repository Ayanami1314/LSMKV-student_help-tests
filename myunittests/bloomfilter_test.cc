#include "../bloomfilter.h"
#include <gtest/gtest.h>
class BFTest : public ::testing::Test {
protected:
  void SetUp() override {}

  void TearDown() override {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }
  BloomFilter bf_l{1000, 5};
  BloomFilter bf_s{5, 1};
};

TEST_F(BFTest, BasicSet) {
  bf_l.insert_u64(1);
  bf_l.insert_u64(2);
  bf_l.insert_u64(114);
  bf_l.insert_u64(514);

  EXPECT_TRUE(bf_l.find_u64(114));
  EXPECT_TRUE(bf_l.find_u64(1));
  EXPECT_TRUE(bf_l.find_u64(2));
  EXPECT_TRUE(bf_l.find_u64(514));

  bf_l.clear();

  EXPECT_FALSE(bf_l.find_u64(114));
  EXPECT_FALSE(bf_l.find_u64(514));
  EXPECT_FALSE(bf_l.find_u64(1));
  EXPECT_FALSE(bf_l.find_u64(2));
}

TEST_F(BFTest, OverFlow) {
  bf_s.insert_u64(1);
  bf_s.insert_u64(2);
  bf_s.insert_u64(114);
  bf_s.insert_u64(514);

  EXPECT_TRUE(bf_s.find_u64(114));
  EXPECT_TRUE(bf_s.find_u64(1));
  EXPECT_TRUE(bf_s.find_u64(2));
  EXPECT_TRUE(bf_s.find_u64(514));

  bf_s.clear();

  EXPECT_FALSE(bf_s.find_u64(114));
  EXPECT_FALSE(bf_s.find_u64(514));
  EXPECT_FALSE(bf_s.find_u64(1));
  EXPECT_FALSE(bf_s.find_u64(2));
}
