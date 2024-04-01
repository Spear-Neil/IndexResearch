#ifndef TREERESEARCH_HOT_H
#define TREERESEARCH_HOT_H

#include <hot/rowex/HOTRowex.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

template<typename K, typename V>
class HotTree;

/* K must be basic type
 * V must be basic type or pointer */
template<typename K, typename V>
class HotTree {
  struct kv_t {
    K key;
    V value;
  };

  template<typename KV>
  struct KeyExtractor {
    typedef K KeyType;

    inline KeyType operator()(const kv_t* kv) {
      return kv->key;
    }
  };

  using Trie_t = hot::rowex::HOTRowex<kv_t*, KeyExtractor>;
  Trie_t tree;

 public:
  /* if V is pointer type: garbage collection isn't taken into account */
  bool upsert(K key, V value) {
    kv_t* kv = new kv_t;
    kv->key = key;
    kv->value = value;
    idx::contenthelpers::OptionalValue<kv_t*> ret = tree.upsert(kv);
    if(ret.mIsValid) return false;
    return true;
  }

  bool search(K key, V& value) {
    idx::contenthelpers::OptionalValue<kv_t*> ret = tree.lookup(key);
    if(ret.mIsValid) {
      value = ret.mValue->value;
      return true;
    }
    return false;
  }
};

template<typename V>
class HotTree<std::string, V> {
  struct kv_t {
    V value;
    int klen;
    char key[];  // cstring with \0
  };

  template<typename KV>
  struct KeyExtractor {
    typedef const char* KeyType;

    inline KeyType operator()(const kv_t* kv) {
      return kv->key;
    }
  };

  using Trie_t = hot::rowex::HOTRowex<kv_t*, KeyExtractor>;

  Trie_t tree;
 public:
  bool upsert(char* key, int klen, V value) {
    kv_t* kv = (kv_t*) malloc(sizeof(kv_t) + klen + 1);
    kv->value = value, kv->klen = klen;
    std::memcpy(kv->key, key, klen);
    kv->key[klen] = 0;
    idx::contenthelpers::OptionalValue<kv_t*> ret = tree.upsert(kv);
    if(ret.mIsValid) return false;
    return true;
  }

  bool upsert(std::string& key, V value) {
    return upsert(key.data(), key.size(), value);
  }

  bool upsert(std::string&& key, V value) {
    return upsert(key.data(), key.size(), value);
  }

  bool search(char* key, V& value) {
    idx::contenthelpers::OptionalValue<kv_t*> ret = tree.lookup(key);
    if(ret.mIsValid) {
      value = ret.mValue->value;
      return true;
    }
    return false;
  }

  bool search(std::string& key, V& value) {
    return search(key.data(), value);
  }

  bool search(std::string&& key, V& value) {
    return search(key.data(), value);
  }

  void statistics() { tree.statistics(); }
};

#endif //TREERESEARCH_HOT_H
