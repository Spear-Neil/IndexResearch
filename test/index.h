#ifndef INDEXRESEARCH_INDEX_H
#define INDEXRESEARCH_INDEX_H

#include <string>
#include "../FBTree/type.h"

/* for experiment, garbage collection is not considered; insert/update
 * operations only modify the index, key-value is not considered; in
 * addition, some index does not implement remove interface */
template<typename K, typename V>
class Index {
 public:
  using KVType = FeatureBTree::KVPair<K, V>;

  virtual ~Index() = default;

  virtual std::string index_type() = 0;

  virtual void insert(KVType* kv) = 0;

  virtual void update(KVType* kv) = 0;

  virtual bool lookup(const K& key, V& value) = 0;

  virtual int scan(const K& key, int num) = 0;
};

enum INDEX_TYPE {
  ARTOLC = 0, HOT = 1,      // trie based
  BTREEOLC = 2, FBTREE = 3, // b+tree
  MASSTREE = 4,             // hybrid trie/b+tree
  WORMHOLE = 5,             // hybrid hash/b+tree
  GBTREE = 6, STXBTREE = 7, // memory optimized b+tree (concurrency unsafe)
  ARTOptiQL = 8             // ARTOLC with Optimistic Queuing lock
};

template<typename K, typename V>
class IndexART : public Index<K, V> {};

template<typename K, typename V>
class IndexHOT : public Index<K, V> {};

template<typename K, typename V>
class IndexBTreeOLC : public Index<K, V> {};

template<typename K, typename V>
class IndexFBTree : public Index<K, V> {};

template<typename K, typename V>
class IndexMASS : public Index<K, V> {};

template<typename K, typename V>
class IndexWH : public Index<K, V> {};

template<typename K, typename V>
class IndexGBTree : public Index<K, V> {};

template<typename K, typename V>
class IndexSTX : public Index<K, V> {};

template<typename K, typename V>
class IndexARTOptiQL : public Index<K, V> {};

template<typename K, typename V>
class IndexFactory {
 public:
  static Index<K, V>* get_index(INDEX_TYPE type);
};

#endif //INDEXRESEARCH_INDEX_H
