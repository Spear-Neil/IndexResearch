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

  bool operator<(String& k) {
    return compare(str, len, k.str, k.len) < 0;
  }

  bool operator>(String& k) {
    return compare(str, len, k.str, k.len) > 0;
  }

  bool operator==(String& k) {
    return !compare(str, len, k.str, k.len);
  }

  bool operator!=(String& k) {
    return compare(str, len, k.str, k.len);
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

}

#endif //INDEXRESEARCH_TYPE_H
