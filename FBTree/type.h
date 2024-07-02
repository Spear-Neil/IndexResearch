#ifndef INDEXRESEARCH_TYPE_H
#define INDEXRESEARCH_TYPE_H

#include <cstdint>
#include <cassert>
#include <cstring>
#include "strutil.h"

using util::compare;

namespace FeatureBTree {

template<typename K, typename V>
struct KVPair;

template<typename V>
struct KVPair<uint64_t, V> {
  uint64_t key;
  V value;
};

template<typename V>
struct KVPair<int64_t, V> {
  int64_t key;
  V value;
};

template<typename V>
struct KVPair<uint32_t, V> {
  uint32_t key;
  V value;
};

template<typename V>
struct KVPair<int32_t, V> {
  int32_t key;
  V value;
};

struct String {
  int len;      // string length
  char str[];   // character array

  String() = delete;

  bool operator<(const String& k) {
    return compare(str, len, (char*) k.str, k.len) < 0;
  }

  bool operator>(const String& k) {
    return compare(str, len, (char*) k.str, k.len) > 0;
  }

  bool operator==(const String& k) {
    return !compare(str, len, (char*) k.str, k.len);
  }

  bool operator!=(const String& k) {
    return compare(str, len, (char*) k.str, k.len);
  }
};

template<typename V>
struct KVPair<String, V> {
  V value;
  String key;

  KVPair() = delete;
};

String* make_string(char* str, int len) {
  assert(str != nullptr && len >= 0);
  String* ret = (String*) malloc(len + sizeof(String));
  ret->len = len;
  memcpy(ret->str, str, len);
  return ret;
}


/* store anchor keys in a contiguous memory block */
class Extent {
  int mlen_;   // total size of available space
  int used_;   // used size of available space
  int free_;   // freed size (anchor remove/update)
  int huge_;   // offset, points to huge prefix
  char mem_[]; // available space

 public:
  void init(int len) {
    mlen_ = len - sizeof(Extent);
    used_ = 0, free_ = 0, huge_ = 0;
  }

  int size() { return mlen_ + sizeof(Extent); }

  int used() { return used_ - free_ + sizeof(Extent); }

  int left() { return mlen_ - used_; }

  String* huge() { return (String*) (mem_ + huge_); }

  void huge(String* key) {
    assert((ptrdiff_t(key) < ptrdiff_t(mem_ + used_)
            && ptrdiff_t(key) >= ptrdiff_t(mem_)) || key == nullptr);
    huge_ = ptrdiff_t(key) - ptrdiff_t(mem_);
  }

  String* make_anchor(String* key) {
    // contiguously malloc a memory block
    if(mlen_ - used_ >= key->len + sizeof(String)) {
      String* ret = (String*) (mem_ + used_);
      ret->len = key->len;
      memcpy(ret->str, key->str, key->len);
      used_ += key->len + sizeof(String);
      return ret;
    }
    // no more space, require realloc
    return nullptr;
  }

  void ruin_anchor(String* key) {
    assert(ptrdiff_t(key) < ptrdiff_t(mem_ + used_)
             && ptrdiff_t(key) >= ptrdiff_t(mem_));
    // only record how many bytes are freed, because these bytes
    // may be accessed by other threads, realloc if necessary
    free_ += key->len + sizeof(String);
  }
};

}

#endif //INDEXRESEARCH_TYPE_H
