#ifndef INDEXRESEARCH_INDEX_H
#define INDEXRESEARCH_INDEX_H

#include <string>

template<typename K, typename V>
class Index {
 public:
  virtual ~Index() {};

  virtual std::string index_type() = 0;

  virtual void insert(K& key, V value) = 0;

  virtual void update(K& key, V value) = 0;

  virtual void remove(K& key) = 0;

  virtual bool lookup(K& key, V& value) = 0;

  virtual int scan(K& key, int num) = 0;
};

enum INDEX_TYPE {
  ARTOLC = 0, HOT = 1,      // trie based
  BTREEOLC = 2, FBTREE = 3, // b+tree
  MASSTREE = 4,             // hybrid trie/b+tree
  WORMHOLE = 5,             // hybrid hash/b+tree
  GBTREE = 6, STXBTREE = 7  // memory optimized b+tree (concurrency unsafe)
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
class IndexFactory {
 public:
  static Index<K, V>* get_index(INDEX_TYPE type);
};

#endif //INDEXRESEARCH_INDEX_H
