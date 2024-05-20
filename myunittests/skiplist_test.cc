#include "../skiplist.h"
#include <gtest/gtest.h>
#include <list>
#include <numeric>
#include <utility>
class SkipListTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Code here will be called immediately after the constructor (right
    // before each test).
    for (int i = 0; i < 1000; ++i) {
      sl.put(i, std::to_string(i));
    }
    for (int i = 0; i < 10; ++i) {
      sl_small.put(i, std::to_string(i));
    }
  }

  void TearDown() override {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }
  skiplist::skiplist_type sl;
  skiplist::skiplist_type sl_small;
};

// Demonstrate some basic assertions.
TEST_F(SkipListTest, BasicGet) {
  // Expect two strings not to be equal.
  for (int i = 0; i < 1000; ++i) {
    EXPECT_EQ(sl.get(i), std::to_string(i));
  }
  EXPECT_EQ(sl.size(), 1000);
}

TEST_F(SkipListTest, GetNonExist) { EXPECT_EQ(sl.get(10000), ""); }
TEST_F(SkipListTest, GetNonExist2) { EXPECT_EQ(sl.get(-1), ""); }

TEST_F(SkipListTest, BasicPut) {
  sl.put(10000, "10000");
  EXPECT_EQ("10000", sl.get(10000));
  sl.put(10000, "something");
  std::string newstring = sl.get(10000);
  EXPECT_EQ(newstring, "something");

  sl.put(65, "aaa");
  sl.put(67, "bbbb");
  sl.put(70, "ccccc");
  sl.put(66, "mid");
  std::string longtext =
      "lllll><>DWQ<?<DW:{L{DWLodjwasnaYGFllllllllllllllllll46asd5a+"
      "s4d23a1s4dw5d788d8w22&&d21321*^*$&%#%"
      "llllllllllllllllllllllasdasdasdllll";
  sl.put(100, longtext);
  EXPECT_EQ(sl.get(100), longtext);
  EXPECT_EQ(sl.get(65), "aaa");
  EXPECT_EQ(sl.get(70), "ccccc");
  EXPECT_EQ(sl.get(66), "mid");
  EXPECT_EQ(sl.get(67), "bbbb");
}
TEST_F(SkipListTest, BasicKeySet) {
  std::list<uint64_t> keyset(10);
  std::iota(keyset.begin(), keyset.end(), 0);
  EXPECT_EQ(sl_small.get_keylist(), keyset);
  keyset.push_back(1);
  EXPECT_NE(sl.get_keylist(), keyset);
}

TEST_F(SkipListTest, BasicScan) {
  constexpr const int startkey = 333;
  constexpr const int endkey = 343;
  constexpr const int len = endkey - startkey + 1;
  std::list<uint64_t> keyset(len);
  std::iota(keyset.begin(), keyset.end(), startkey);
  std::list<skiplist::kvpair> pairset;
  for (auto &k : keyset) {
    pairset.push_back(std::make_pair(k, sl.get(k)));
  }

  EXPECT_EQ(sl.scan(startkey, endkey), pairset);

  std::list<skiplist::kvpair> empty;
  EXPECT_EQ(sl.scan(startkey, startkey - 1), empty);

  empty.push_front(std::make_pair(endkey, sl.get(endkey)));
  EXPECT_EQ(sl.scan(endkey, endkey), empty);
}
