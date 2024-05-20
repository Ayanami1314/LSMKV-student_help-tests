#ifndef __TYPE_H
#define __TYPE_H
#include <cstdint>
#include <filesystem>
#include <list>
#include <string>
#include <vector>
using u64 = uint64_t;
using u32 = uint32_t;
using u16 = uint16_t;
using u8 = uint8_t;
using TKey = u64;
using TOff = u64;
using TLen = u32;
using TCheckSum = u16;
using TMagic = u8;
using TValue = std::string;
using TBytes = std::vector<u8>;
using kEntry = struct kEntry {
  TKey key;
  TOff offset;
  TLen len;
  bool operator==(const kEntry &rhs) const {
    return key == rhs.key && offset == rhs.offset && len == rhs.len;
  }
  // NOTE：a < b. a's priority < b. a's key > b's or a's key == b's and a's
  // offset < b's(older) to keep the min key at the top of the priority queue(if
  // equal, the newest one at the top)
  bool operator<(const kEntry &rhs) const {
    return key > rhs.key || (key == rhs.key && offset < rhs.offset);
  }
  bool operator>(const kEntry &rhs) const {
    return key < rhs.key || (key == rhs.key && offset > rhs.offset);
  }
  bool is_deleted() { return len == 0; }
};
using kEntrys = std::vector<kEntry>;
using vEntryPrefix = struct prefix {
  TMagic magic;
  TCheckSum checksum;
  TKey key;
  TLen vlen;
};

using vEntry = struct VEntry {
  TMagic magic;
  TCheckSum checksum;
  TKey key;
  TLen vlen;
  TValue vvalue;
};
using vEntryProps = struct VEntryProps {
  TKey key;
  TLen vlen;
  TValue vvalue;
};
using TPath = std::filesystem::path;
// vEntry:{char magic, uint16 checksum, uint32 vlen, string}

using vEntrys = std::list<vEntry>;
namespace type {
static kEntry ke_not_found = {0, 0, 0};
static vEntry ve_not_found = {0, 0, 0, 0, ""};
constexpr static u64 ve_prefix_len =
    sizeof(TMagic) + sizeof(TCheckSum) + sizeof(TKey) + sizeof(TLen);

} // namespace type
namespace config {

static int bf_default_k = 3;
static u64 bf_default_size = 8 * 1024 * 8; // 8KB = 8*8*1024 bit
static bool use_bf = true;
static bool use_cache = true;
using ConfigParam = struct ConfigParam {
  int bf_default_size;
  int bf_default_k;
  bool use_bf;
  bool use_cache;
  friend std::ostream &operator<<(std::ostream &os, const ConfigParam &conf);
};
inline std::ostream &operator<<(std::ostream &os, const ConfigParam &conf) {
  auto out_format = "use_bf: %s, use_cache: %s\nbf_size: %d, bf_func_num: %d\n";
  char out[256];
  auto tf_tos = [](bool cond) { return cond ? "true" : "false"; };

  sprintf(out, out_format, tf_tos(conf.use_bf), tf_tos(conf.use_cache),
          conf.bf_default_size, conf.bf_default_k);
  os << out;
  return os;
}
static inline void reConfig(const config::ConfigParam &newConfig) {
  auto [use_bf, use_cache, bf_default_size, bf_default_k] = newConfig;
  if (!use_cache) {
    config::use_cache = false;
    config::use_bf = false;
    config::bf_default_k = 0;
    config::bf_default_size = 0;
  };
  if (use_cache && !use_bf) {
    config::use_cache = true;
    config::use_bf = false;
    config::bf_default_k = 0;
    config::bf_default_size = 0;
  }
  if (use_cache && use_bf) {
    config::use_cache = true;
    config::use_bf = true;
    config::bf_default_k = bf_default_k;
    config::bf_default_size = bf_default_size;
  }
}
} // namespace config

#endif