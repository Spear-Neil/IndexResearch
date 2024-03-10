#ifndef INDEXRESEARCH_TYPE_H
#define INDEXRESEARCH_TYPE_H

#include <cstdint>

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

template<typename V>
struct KVPair<char*, V> {
  V value;
  int klen;   // key size
  char key[]; // key array
};

}

#endif //INDEXRESEARCH_TYPE_H
