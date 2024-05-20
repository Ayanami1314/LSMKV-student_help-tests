#include "../sstable.h"
#include "../utils.h"
#include "../vlog.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
class vLogTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Code here will be called immediately after the constructor (right
    // before each test).
    vpath = "../tmp/vlog";
    if (!std::filesystem::exists(vpath)) {
      std::filesystem::create_directories("../tmp");
    }
    vl = std::make_unique<vLogs>(vpath);
  }

  void TearDown() override {
    // Code here will be called immediately after each test (right
    // before the destructor).
    if (std::filesystem::exists("../tmp")) {
      std::filesystem::remove_all("../tmp");
    }
  }
  TPath vpath;
  std::unique_ptr<vLogs> vl;
};

TEST_F(vLogTest, createFile) {
  vl->clear();
  ASSERT_TRUE(std::filesystem::exists(vpath));
  ASSERT_EQ(std::filesystem::file_size(vpath), 0);
}

TEST_F(vLogTest, addVlog) {
  vl->clear();

  std::ofstream ovfs(vpath, std::ios::binary | std::ios::app);
  ASSERT_TRUE(ovfs.is_open());
  std::string vvalue = "Hello, LSM!";
  vEntryProps v;
  v.key = 0;
  v.vvalue = vvalue;
  v.vlen = vvalue.size();

  vl->addVlog(v);
  system("xxd -i ./tmp/vlog");
  auto [ref_vkey, ref_vlen, ref_vvalue] = v;
  auto ref_magic = vl->getMagic();
  TCheckSum ref_checksum = 0;
  vl->cal_bytes(v, ref_checksum);

  ovfs.close();

  std::ifstream ivfs(vpath, std::ios::binary);
  ASSERT_TRUE(ivfs.is_open());
  std::vector<char> buffer((std::istreambuf_iterator<char>(ivfs)),
                           (std::istreambuf_iterator<char>()));
  TMagic magic = 0;
  std::memcpy(&magic, buffer.data(), sizeof(magic));
  EXPECT_EQ(magic, ref_magic);

  TCheckSum checksum = 0;
  std::memcpy(&checksum, buffer.data() + sizeof(magic), sizeof(checksum));
  EXPECT_EQ(checksum, ref_checksum);

  TKey vkey = 0;
  std::memcpy(&vkey, buffer.data() + sizeof(magic) + sizeof(checksum),
              sizeof(vkey));
  EXPECT_EQ(vkey, ref_vkey);
  TLen vlen = 0;
  std::memcpy(&vlen,
              buffer.data() + sizeof(magic) + sizeof(checksum) + sizeof(vkey),
              sizeof(vlen));
  EXPECT_EQ(vlen, ref_vlen);
  EXPECT_EQ(vlen, vvalue.size());

  std::string vvalue_str(buffer.data() + sizeof(magic) + sizeof(checksum) +
                             sizeof(vlen) + sizeof(vkey),
                         ref_vlen);
  EXPECT_EQ(vvalue_str, vvalue);
  EXPECT_EQ(vvalue, ref_vvalue);

  ivfs.close();
}

TEST_F(vLogTest, loadSaveTest) {
  vl->clear();
  std::string vvalue = "Hello, LSM!";
  vEntryProps v;
  v.key = 0;
  v.vvalue = vvalue;
  v.vlen = vvalue.size();
  TOff off = vl->addVlog(v);
  system("xxd -i ./tmp/vlog");
  kEntry ke = {.key = 0, .offset = off, .len = v.vlen};
  TValue res_v = vl->query(ke);
  EXPECT_EQ(res_v, vvalue);
}

TEST_F(vLogTest, readTest) {
  vl->clear();
  vEntrys ves;

  for (int i = 0; i < 10; ++i) {
    vl->addVlog({static_cast<TKey>(i),
                 static_cast<TLen>(std::to_string(i).size()),
                 std::to_string(i)});
  }
  system("xxd -i ../tmp/vlog");
  std::vector<TOff> locs;
  const int size_of_prefix = 15;
  int ref_loc = 0;
  vl->readVlogs(vl->getTail(), ves, 200, locs);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(locs[i], ref_loc);
    ref_loc += (15 + 1);
  }
}

TEST_F(vLogTest, readTest2) {
  vl->clear();
  vEntrys ves;

  for (int i = 0; i < 10; ++i) {
    vl->addVlog({static_cast<TKey>(i),
                 static_cast<TLen>(std::to_string(i).size()),
                 std::to_string(i)});
  }
  system("xxd -i ./tmp/vlog");
  std::vector<TOff> locs;
  const int size_of_prefix = 15;
  int ref_loc = 0;
  vl->readVlogs(vl->getTail(), ves, 100, locs);
  // NOTE: 16 * 7 = 112 > 100
  for (int i = 0; i < 6; ++i) {
    EXPECT_EQ(locs[i], ref_loc);
    ref_loc += (15 + 1);
  }
}

TEST_F(vLogTest, queryTest) {
  vl->clear();

  for (int i = 0; i < 10; ++i) {
    vl->addVlog({static_cast<TKey>(i),
                 static_cast<TLen>(std::to_string(i).size()),
                 std::to_string(i)});
  }
  for (int i = 9; i >= 0; --i) {
    kEntry ke = {.key = static_cast<TKey>(i),
                 .offset = static_cast<TOff>(i * 16),
                 .len = 1};
    TValue v = vl->query(ke);
    EXPECT_EQ(v, std::to_string(i));
  }
  EXPECT_EQ(vl->getTail(), 0);
}

TEST_F(vLogTest, gcTest) {
  vl->clear();
  for (int i = 0; i < 10; ++i) {
    vl->addVlog({static_cast<TKey>(i),
                 static_cast<TLen>(std::to_string(i).size()),
                 std::to_string(i)});
  }
  for (int i = 0; i < 10; ++i) {
    kEntry ke = {.key = static_cast<TKey>(i),
                 .offset = static_cast<TOff>(i * 16),
                 .len = 1};
    TValue v = vl->query(ke);
    EXPECT_EQ(v, std::to_string(i));
  }
  EXPECT_EQ(vl->getTail(), 0);
  vl->gc(5 * 16);
  std::string not_found = "";
  for (int i = 0; i < 5; ++i) {
    kEntry ke = {.key = static_cast<TKey>(i),
                 .offset = static_cast<TOff>(i * 16),
                 .len = 1};
    TValue v = vl->query(ke);
    EXPECT_EQ(v, not_found);
  }
  for (int i = 5; i < 10; ++i) {
    kEntry ke = {.key = static_cast<TKey>(i),
                 .offset = static_cast<TOff>(i * 16),
                 .len = 1};
    TValue v = vl->query(ke);
    EXPECT_EQ(v, std::to_string(i));
  }
  EXPECT_EQ(vl->getTail(), 5 * 16);
}

TEST_F(vLogTest, gcWithReloc) {
  vl->clear();
  for (int i = 0; i < 10; ++i) {
    vl->addVlog({static_cast<TKey>(i),
                 static_cast<TLen>(std::to_string(i).size()),
                 std::to_string(i)});
  }
  for (int i = 0; i < 10; ++i) {
    kEntry ke = {.key = static_cast<TKey>(i),
                 .offset = static_cast<TOff>(i * 16),
                 .len = 1};
    TValue v = vl->query(ke);
    EXPECT_EQ(v, std::to_string(i));
  }
  EXPECT_EQ(vl->getTail(), 0);
  vl->gc(5 * 16 - 13);
  std::string not_found = "";
  for (int i = 0; i < 5; ++i) {
    kEntry ke = {.key = static_cast<TKey>(i),
                 .offset = static_cast<TOff>(i * 16),
                 .len = 1};
    TValue v = vl->query(ke);
    EXPECT_EQ(v, not_found);
  }
  for (int i = 5; i < 10; ++i) {
    kEntry ke = {.key = static_cast<TKey>(i),
                 .offset = static_cast<TOff>(i * 16),
                 .len = 1};
    TValue v = vl->query(ke);
    EXPECT_EQ(v, std::to_string(i));
  }
  EXPECT_EQ(vl->getTail(), 5 * 16 - 13);
}

TEST_F(vLogTest, persistenceTest) {
  vl->clear();
  for (int i = 0; i < 10; ++i) {
    vl->addVlog({static_cast<TKey>(i),
                 static_cast<TLen>(std::to_string(i).size()),
                 std::to_string(i)});
  }
  vl->clear_mem();
  vl->reload_mem();
  vl = std::make_unique<vLogs>(vpath);
  for (int i = 0; i < 10; ++i) {
    kEntry ke = {.key = static_cast<TKey>(i),
                 .offset = static_cast<TOff>(i * 16),
                 .len = 1};
    TValue v = vl->query(ke);
    EXPECT_EQ(v, std::to_string(i));
  }
  EXPECT_EQ(vl->getTail(), 0);
  EXPECT_EQ(vl->getHead(), 16 * 10);
}