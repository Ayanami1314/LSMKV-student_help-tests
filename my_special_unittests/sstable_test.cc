#include "../sstable.h"
#include "../type.h"
#include <gtest/gtest.h>
class SSTableTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Code here will be called immediately after the constructor (right
    // before each test).
    for (int i = 0; i < 10; ++i) {
      kes.push_back(
          {static_cast<TKey>(i), static_cast<TOff>(0), static_cast<TLen>(1)});
    }
    sstable = SSTable::sstable_type(kes, 0);
    if (!std::filesystem::exists(save_dir)) {
      std::filesystem::create_directory(save_dir);
    }
  }

  void TearDown() override {
    // Code here will be called immediately after each test (right
    // before the destructor).
    if (std::filesystem::exists(save_dir)) {
      std::filesystem::remove_all(save_dir);
    }
  }
  kEntrys kes;

  SSTable::sstable_type sstable;
  const std::string save_dir = "./tmp";
  const std::string save_path = std::filesystem::path(save_dir) / "test.sst";
};

TEST_F(SSTableTest, defaultSize) {
  // 32B Header, default 8KB BF
  SSTable::sstable_type new_table;
  EXPECT_EQ(new_table.size(), 32 + 8 * 1024);
}

TEST_F(SSTableTest, defaultCalSize) {
  EXPECT_EQ(SSTable::sstable_type::cal_size(10),
            10 * sizeof(kEntry) + 32 + 8 * 1024);
}

#include <fstream> // Include the necessary header file

TEST_F(SSTableTest, saveLoadTest) {
  auto BF1 = sstable.getBF();
  auto bf1_size = sstable.getBFSize();
  BF1.print();
  sstable.save(save_path);
  sstable.clear();
  sstable.load(save_path);
  auto BF2 = sstable.getBF();
  BF2.print();
  ASSERT_FALSE(sstable.getKEntryNum() == 0) << "sstable is empty";
  ASSERT_EQ(bf1_size, sstable.getBFSize());
  for (int i = 0; i < 10; ++i) {
    kEntry ref = {static_cast<TKey>(i), static_cast<TOff>(0),
                  static_cast<TLen>(1)};
    EXPECT_EQ(sstable.query(i), ref);
  }
  auto not_found_ref = type::ke_not_found;
  EXPECT_EQ(sstable.query(10), not_found_ref);

  auto new_save_path = std::filesystem::path(save_dir) / "new_test.sst";
  sstable.save(new_save_path);
  std::ifstream ifs1(save_path, std::ios::binary | std::ios::ate);
  std::ifstream ifs2(new_save_path, std::ios::binary | std::ios::ate);

  EXPECT_EQ(ifs1.tellg(), ifs2.tellg());

  std::vector<char> v1(ifs1.tellg());
  std::vector<char> v2(ifs2.tellg());
  ifs1.seekg(0);
  ifs2.seekg(0);
  ifs1.read(v1.data(), v1.size());
  ifs2.read(v2.data(), v2.size());
  EXPECT_EQ(v1, v2);

  SSTable::Header h1;
  ifs1.seekg(0);
  ifs1.read(reinterpret_cast<char *>(&h1), sizeof(h1));
  EXPECT_EQ(h1.getMinKey(), 0);
  EXPECT_EQ(h1.getMaxKey(), 9);
  EXPECT_EQ(h1.getNumOfKV(), 10);
}