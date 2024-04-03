#include <string>
#include <limits>
#include <atomic>
#include <mutex>

#include "index.h"
#include "../FBTree/type.h"
#include "../ARTOLC/Tree.h"
#include "hot/rowex/HOTRowex.hpp"
#include "idx/contenthelpers/OptionalValue.hpp"
#include "../BTreeOLC/BTreeOLC_child_layout.h"
#include "../FBTree/fbtree.h"

#undef prefetch
#undef likely
#undef unlikely

#include "../MassTree/masstree.hh"
#include "../MassTree/kvthread.hh"
#include "../MassTree/masstree_struct.hh"
#include "../MassTree/masstree_insert.hh"
#include "../MassTree/masstree_remove.hh"
#include "../MassTree/masstree_scan.hh"
#include "../wormhole/lib.h"
#include "../wormhole/kv.h"
#include "../wormhole/wh.h"
#include "../GoogleBTree/btree_map.h"
#include "../STX/tlx/tlx/container.hpp"

template<>
class IndexART<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  ART_OLC::Tree tree;
  Key max_key;

  using pair = FeatureBTree::KVPair<uint64_t, uint64_t>;

 public:
  IndexART() : tree([](TID tid, Key& key) { key.setInt(((pair*) tid)->key); }) {
    max_key.setInt(std::numeric_limits<uint64_t>::max());
  }

  ~IndexART() {}

  std::string index_type() { return "ART"; }

  void insert(uint64_t& key, uint64_t value) {
    static thread_local auto t = tree.getThreadInfo();
    void* tid = new pair{key, value};
    Key k;
    k.setInt(key);
    tree.insert(k, (TID) tid, t);
  }

  void update(uint64_t& key, uint64_t value) {
    static thread_local auto t = tree.getThreadInfo();
    Key k;
    k.setInt(key);
    pair* tid = (pair*) tree.lookup(k, t);
    if(tid != nullptr) {
      ((std::atomic<uint64_t>&) tid->value).exchange(value);
    }
  }

  void remove(uint64_t& key) {
    static thread_local auto t = tree.getThreadInfo();
    Key k;
    k.setInt(key);
    TID tid = tree.lookup(k, t);
    tree.remove(k, tid, t);
    if((void*) tid != nullptr) {
      t.getEpoche().markNodeForDeletion((void*) tid, t);
    }
  }

  bool lookup(uint64_t& key, uint64_t& value) {
    static thread_local auto t = tree.getThreadInfo();
    Key k;
    k.setInt(key);
    pair* tid = (pair*) tree.lookup(k, t);
    if(tid == nullptr) return false;
    value = tid->value;
    return true;
  }

  int scan(uint64_t& key, int num) {
    static thread_local auto t = tree.getThreadInfo();
    Key starts;
    Key continues;
    starts.setInt(key);

    TID tids[num];
    size_t count;
    tree.lookupRange(starts, max_key, continues, tids, num, count, t);

    return count;
  }
};

template<>
class IndexART<std::string, uint64_t> : public Index<std::string, uint64_t> {
  ART_OLC::Tree tree;
  Key max_key;

  static constexpr int max_len = 256;
  using String = FeatureBTree::String;
  using pair = FeatureBTree::KVPair<String, uint64_t>;

 private:
  void set_key(char* str, int len, Key& key) {
    key.setKeyLen(max_len);
    memcpy(key.data, str, len);
    memset(key.data + len, 0, max_len - len);
  }

 public:
  IndexART() : tree([](TID tid, Key& key) {
    key.setKeyLen(max_len);
    char* str = ((pair*) tid)->key.str;
    int len = ((pair*) tid)->key.len;
    memcpy(key.data, str, len);
    memset(key.data + len, 0, max_len - len);
  }) {
    char str[max_len];
    memset(str, 0xFF, max_len);
    max_key.set(str, max_len);
  }

  ~IndexART() {}

  std::string index_type() { return "ART"; }

  void insert(std::string& key, uint64_t value) {
    static thread_local auto t = tree.getThreadInfo();
    pair* tid = (pair*) malloc(sizeof(pair) + key.size());
    tid->value = value, tid->key.len = key.size();
    memcpy(tid->key.str, key.data(), key.size());

    Key k;
    set_key(key.data(), key.size(), k);
    tree.insert(k, (TID) tid, t);
  }

  void update(std::string& key, uint64_t value) {
    static thread_local auto t = tree.getThreadInfo();
    Key k;
    set_key(key.data(), key.size(), k);
    pair* tid = (pair*) tree.lookup(k, t);

    if(tid != nullptr) {
      ((std::atomic<uint64_t>&) tid->value).exchange(value);
    }
  }

  void remove(std::string& key) {
    static thread_local auto t = tree.getThreadInfo();
    Key k;
    set_key(key.data(), key.size(), k);
    TID tid = tree.lookup(k, t);
    tree.remove(k, tid, t);
    if((void*) tid != nullptr) {
      t.getEpoche().markNodeForDeletion((void*) tid, t);
    }
  }

  bool lookup(std::string& key, uint64_t& value) {
    static thread_local auto t = tree.getThreadInfo();
    Key k;
    set_key(key.data(), key.size(), k);
    pair* tid = (pair*) tree.lookup(k, t);
    if(tid == nullptr) return false;

    value = tid->value;
    return true;
  }

  int scan(std::string& key, int num) {
    static thread_local auto t = tree.getThreadInfo();
    Key starts;
    Key continues;
    set_key(key.data(), key.size(), starts);

    TID tids[num];
    size_t count;
    tree.lookupRange(starts, max_key, continues, tids, num, count, t);

    return count;
  }
};

template<>
class IndexHOT<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  using pair = FeatureBTree::KVPair<uint64_t, uint64_t>;

  template<class KV>
  struct KeyExtractor {
    typedef uint64_t KeyType;

    inline KeyType operator()(const pair* kv) {
      return kv->key;
    }
  };

  using Trie_t = hot::rowex::HOTRowex<pair*, KeyExtractor>;
  Trie_t tree;

 public:
  IndexHOT() {}

  ~IndexHOT() {}

  std::string index_type() { return "HOT"; }

  void insert(uint64_t& key, uint64_t value) {
    pair* kv = new pair{key, value};
    tree.upsert(kv);
  }

  void update(uint64_t& key, uint64_t value) {
    idx::contenthelpers::OptionalValue<pair*> ret = tree.lookup(key);
    if(ret.mIsValid) {
      ((std::atomic<uint64_t>&) ret.mValue->value).exchange(value);
    }
  }

  void remove(uint64_t& key) {}

  bool lookup(uint64_t& key, uint64_t& value) {
    idx::contenthelpers::OptionalValue<pair*> ret = tree.lookup(key);
    if(ret.mIsValid) {
      value = ret.mValue->value;
      return true;
    }
    return false;
  }

  int scan(uint64_t& key, int num) {
    auto iterator = tree.lower_bound(key);
    int count = 0;
    for(size_t i = 0; i < num; i++) {
      if(iterator == tree.end()) break;
      count++, ++iterator;
    }
    return count;
  }
};

template<>
class IndexHOT<std::string, uint64_t> : public Index<std::string, uint64_t> {
  using String = FeatureBTree::String;
  using pair = FeatureBTree::KVPair<String, uint64_t>;

  template<class KV>
  struct KeyExtractor {
    typedef const char* KeyType;

    inline KeyType operator()(const pair* kv) {
      return kv->key.str;
    }
  };

  using Trie_t = hot::rowex::HOTRowex<pair*, KeyExtractor>;
  Trie_t tree;

 public:
  IndexHOT() {}

  ~IndexHOT() {}

  std::string index_type() { return "HOT"; }

  void insert(std::string& key, uint64_t value) {
    pair* kv = (pair*) malloc(sizeof(pair) + key.size() + 1);
    kv->value = value, kv->key.len = key.size();
    memcpy(kv->key.str, key.data(), key.size());
    kv->key.str[key.size()] = 0;
    tree.upsert(kv);
  }

  void update(std::string& key, uint64_t value) {
    idx::contenthelpers::OptionalValue<pair*> ret = tree.lookup(key.data());
    if(ret.mIsValid) {
      ((std::atomic<uint64_t>&) ret.mValue->value).exchange(value);
    }
  }

  void remove(std::string& key) {}

  bool lookup(std::string& key, uint64_t& value) {
    idx::contenthelpers::OptionalValue<pair*> ret = tree.lookup(key.data());
    if(ret.mIsValid) {
      value = ret.mValue->value;
      return true;
    }
    return false;
  }

  int scan(std::string& key, int num) {
    auto iterator = tree.lower_bound(key.data());
    int count = 0;
    for(size_t i = 0; i < num; i++) {
      if(iterator == tree.end()) break;
      count++, ++iterator;
    }
    return count;
  }
};

template<>
class IndexBTreeOLC<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  btreeolc::BTree<uint64_t, uint64_t> tree;

 public:
  IndexBTreeOLC() {}

  ~IndexBTreeOLC() {}

  std::string index_type() { return "BTreeOLC"; }

  void insert(uint64_t& key, uint64_t value) {
    tree.insert(key, value);
  }

  void update(uint64_t& key, uint64_t value) {
    tree.insert(key, value);
  }

  void remove(uint64_t& key) {}

  bool lookup(uint64_t& key, uint64_t& value) {
    return tree.lookup(key, value);
  }

  int scan(uint64_t& key, int num) {
    uint64_t out[num];
    return tree.scan(key, num, out);
  }
};

template<>
class IndexBTreeOLC<std::string, uint64_t> : public Index<std::string, uint64_t> {
  struct String {
    using KEY = FeatureBTree::String;
    KEY* key;

    void construct(char* str, int len) {
      key = (KEY*) malloc(sizeof(KEY) + len);
      key->len = len;
      memcpy(key->str, str, len);
    }

    bool operator<(const String& k) {
      return *key < *k.key;
    }

    bool operator>(const String& k) {
      return *key > *k.key;
    }

    bool operator==(const String& k) {
      return *key == *k.key;
    }

    bool operator!=(const String& k) {
      return *key != *k.key;
    }
  };

  btreeolc::BTree<String, uint64_t> tree;

 public:
  IndexBTreeOLC() {}

  ~IndexBTreeOLC() {}

  std::string index_type() { return "BTreeOLC"; }

  void insert(std::string& key, uint64_t value) {
    String k;
    k.construct(key.data(), key.size());
    tree.insert(k, value);
  }

  void update(std::string& key, uint64_t value) {
    String k;
    k.construct(key.data(), key.size());
    tree.insert(k, value);
    free(k.key);
  }

  void remove(std::string& key) {}

  bool lookup(std::string& key, uint64_t& value) {
    String k;
    k.construct(key.data(), key.size());
    bool find = tree.lookup(k, value);
    free(k.key);
    return find;
  }

  int scan(std::string& key, int num) {
    String k;
    k.construct(key.data(), key.size());

    uint64_t out[num];
    int count = tree.scan(k, num, out);
    free(k.key);
    return count;
  }
};

template<>
class IndexFBTree<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  FeatureBTree::FBTree<uint64_t, uint64_t> tree;

 public:
  IndexFBTree() {}

  ~IndexFBTree() {}

  std::string index_type() { return "FBTree"; }

  void insert(uint64_t& key, uint64_t value) {
    EpochGuard guard(tree.get_epoch());
    tree.upsert(key, value);
  }

  void update(uint64_t& key, uint64_t value) {
    EpochGuard guard(tree.get_epoch());
    auto kv = tree.lookup(key);
    if(kv != nullptr) {
      ((std::atomic<uint64_t>&) kv->value).exchange(value);
    }
  }

  void remove(uint64_t& key) {
    EpochGuard guard(tree.get_epoch());
    auto kv = tree.remove(key);
    guard.retire(kv);
  }

  bool lookup(uint64_t& key, uint64_t& value) {
    EpochGuard guard(tree.get_epoch());
    auto kv = tree.lookup(key);
    if(kv == nullptr) return false;
    value = kv->value;
    return true;
  }

  int scan(uint64_t& key, int num) {
    EpochGuard guard(tree.get_epoch());
    auto it = tree.lower_bound(key);
    int count = 0;
    for(int i = 0; i < num; i++) {
      if(it.end()) break;
      count++, it.advance();
    }
    return count;
  }
};

template<>
class IndexFBTree<std::string, uint64_t> : public Index<std::string, uint64_t> {
  FeatureBTree::FBTree<FeatureBTree::String, uint64_t> tree;

 public:
  IndexFBTree() {}

  ~IndexFBTree() {}

  std::string index_type() { return "FBTree"; }

  void insert(std::string& key, uint64_t value) {
    EpochGuard guard(tree.get_epoch());
    tree.upsert(key, value);
  }

  void update(std::string& key, uint64_t value) {
    EpochGuard guard(tree.get_epoch());
    auto kv = tree.lookup(key);
    if(kv != nullptr) {
      ((std::atomic<uint64_t>&) kv->value).exchange(value);
    }
  }

  void remove(std::string& key) {
    EpochGuard guard(tree.get_epoch());
    auto kv = tree.remove(key);
    guard.retire(kv);
  }

  bool lookup(std::string& key, uint64_t& value) {
    EpochGuard guard(tree.get_epoch());
    auto kv = tree.lookup(key);
    if(kv == nullptr) return false;
    value = kv->value;
    return true;
  }

  int scan(std::string& key, int num) {
    EpochGuard guard(tree.get_epoch());
    auto it = tree.lower_bound(key);
    int count = 0;
    for(int i = 0; i < num; i++) {
      if(it.end()) break;
      count++, it.advance();
    }
    return count;
  }
};


volatile uint64_t globalepoch = 1;     // global epoch, updated by main thread regularly
volatile uint64_t active_epoch = 1;

class MassTreeBase {
  struct NodeParam : Masstree::nodeparams<> {
    typedef uint64_t value_type;
    typedef threadinfo threadinfo_type;
  };

  typedef Masstree::basic_table<NodeParam> Tree_t;
  typedef typename Masstree::basic_table<NodeParam>::cursor_type locked_cursor_t;
  typedef typename Masstree::basic_table<NodeParam>::unlocked_cursor_type unlocked_cursor_t;

  Tree_t tree_;
  std::atomic<int> nthd;
  std::mutex lock_;
  inline static thread_local threadinfo* thd_info = nullptr;

  struct Scanner {
    int count;
    int num;

    Scanner(int size) : count(0), num(size) {}

    template<typename SS, typename K>
    void visit_leaf(const SS&, const K&, threadinfo&) {
    }

    bool visit_value(lcdf::Str key, uint64_t value, threadinfo&) {
      if(++count < num) return true;

      return false;
    }
  };

 private:
  void make_info(int purpose, int index) {
    if(thd_info == nullptr) {
      std::lock_guard<std::mutex> guard(lock_);
      thd_info = threadinfo::make(purpose, index);
    }
  }

  threadinfo* get_info() {
    if(thd_info == nullptr) {
      make_info(threadinfo::TI_PROCESS, nthd++);
    }
    assert(thd_info != nullptr);
    return thd_info;
  }

 public:
  MassTreeBase() : nthd(0) {
    make_info(threadinfo::TI_MAIN, -1);
    tree_.initialize(*thd_info);
  }

  void upsert(char* key, int len, uint64_t value) {
    threadinfo* info = get_info();
    info->rcu_start();
    locked_cursor_t lp(tree_, key, len);
    lp.find_insert(*info);
    lp.value() = value;
    lp.finish(1, *info);
    info->rcu_stop();
  }

  bool lookup(char* key, int len, uint64_t& value) {
    threadinfo* info = get_info();
    info->rcu_start();
    unlocked_cursor_t lp(tree_, key, len);
    bool find = lp.find_unlocked(*info);
    if(find) value = lp.value();
    info->rcu_stop();
    return find;
  }

  void remove(char* key, int len) {
    threadinfo* info = get_info();
    info->rcu_start();
    locked_cursor_t lp(tree_, key, len);
    lp.find_locked(*info);
    lp.finish(-1, *info);
    info->rcu_stop();
  }

  int scan(char* key, int len, int num) {
    threadinfo* info = get_info();
    info->rcu_start();
    lcdf::Str first(key, len);
    Scanner scanner(num);
    int count = tree_.scan(first, true, scanner, *info);
    info->rcu_stop();
    return count;
  }
};

template<>
class IndexMASS<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  MassTreeBase tree;

 public:
  IndexMASS() {}

  ~IndexMASS() {}

  std::string index_type() { return "MassTree"; }

  void insert(uint64_t& key, uint64_t value) {
    uint64_t k = byte_swap(key);
    tree.upsert((char*) &k, 8, value);
  }

  void update(uint64_t& key, uint64_t value) {
    uint64_t k = byte_swap(key);
    tree.upsert((char*) &k, 8, value);
  }

  void remove(uint64_t& key) {
    uint64_t k = byte_swap(key);
    tree.remove((char*) &k, 8);
  }

  bool lookup(uint64_t& key, uint64_t& value) {
    uint64_t k = byte_swap(key);
    return tree.lookup((char*) &k, 8, value);
  }

  int scan(uint64_t& key, int num) {
    uint64_t k = byte_swap(key);
    return tree.scan((char*) &k, 8, num);
  }
};

template<>
class IndexMASS<std::string, uint64_t> : public Index<std::string, uint64_t> {
  MassTreeBase tree;

 public:
  IndexMASS() {}

  ~IndexMASS() {}

  std::string index_type() { return "MassTree"; }

  void insert(std::string& key, uint64_t value) {
    tree.upsert(key.data(), key.size(), value);
  }

  void update(std::string& key, uint64_t value) {
    tree.upsert(key.data(), key.size(), value);
  }

  void remove(std::string& key) {
    tree.remove(key.data(), key.size());
  }

  bool lookup(std::string& key, uint64_t& value) {
    return tree.lookup(key.data(), key.size(), value);
  }

  int scan(std::string& key, int num) {
    return tree.scan(key.data(), key.size(), num);
  }
};

template<>
class IndexWH<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  wormhole* wh;
  inline static thread_local wormref* whref = nullptr;

  void make_ref() {
    if(whref == nullptr) {
      whref = wh_ref(wh);
    }
  }

 public:
  IndexWH() { wh = wh_create(); }

  ~IndexWH() {}

  std::string index_type() { return "WormHole"; }

  void insert(uint64_t& key, uint64_t value) {
    make_ref();
    uint64_t k = byte_swap(key);
    wh_put(whref, &k, 8, &value, 8);
  }

  void update(uint64_t& key, uint64_t value) {
    make_ref();
    uint64_t k = byte_swap(key);
    wh_put(whref, &k, 8, &value, 8);
  }

  void remove(uint64_t& key) {
    make_ref();
    uint64_t k = byte_swap(key);
    wh_del(whref, &k, 8);
  }

  bool lookup(uint64_t& key, uint64_t& value) {
    make_ref();
    uint64_t k = byte_swap(key);
    uint32_t vlen;
    return wh_get(whref, &k, 8, &value, 8, &vlen);
  }

  int scan(uint64_t& key, int num) {
    make_ref();
    uint64_t k = byte_swap(key);

    wormhole_iter* iter = wh_iter_create(whref);
    wh_iter_seek(iter, &k, 8);
    int count = 0;
    for(int i = 0; i < num; i++) {
      if(!wh_iter_valid(iter)) break;
      count++, wh_iter_skip1(iter);
    }
    wh_iter_destroy(iter);

    return count;
  }
};

template<>
class IndexWH<std::string, uint64_t> : public Index<std::string, uint64_t> {
  wormhole* wh;
  inline static thread_local wormref* whref = nullptr;

  void make_ref() {
    if(whref == nullptr) {
      whref = wh_ref(wh);
    }
  }

 public:
  IndexWH() { wh = wh_create(); }

  ~IndexWH() {}

  std::string index_type() { return "WormHole"; }

  void insert(std::string& key, uint64_t value) {
    make_ref();
    wh_put(whref, key.data(), key.size(), &value, 8);
  }

  void update(std::string& key, uint64_t value) {
    make_ref();
    wh_put(whref, key.data(), key.size(), &value, 8);
  }

  void remove(std::string& key) {
    make_ref();
    wh_del(whref, key.data(), key.size());
  }

  bool lookup(std::string& key, uint64_t& value) {
    make_ref();
    uint32_t vlen;
    return wh_get(whref, key.data(), key.size(), &value, 8, &vlen);
  }

  int scan(std::string& key, int num) {
    make_ref();

    wormhole_iter* iter = wh_iter_create(whref);
    wh_iter_seek(iter, key.data(), key.size());
    int count = 0;
    for(int i = 0; i < num; i++) {
      if(!wh_iter_valid(iter)) break;
      count++, wh_iter_skip1(iter);
    }
    wh_iter_destroy(iter);

    return count;
  }
};

template<>
class IndexGBTree<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  btree::btree_map<uint64_t, uint64_t> tree;
  std::mutex lock;

 public:
  IndexGBTree() {}

  ~IndexGBTree() {}

  std::string index_type() { return "GoogleBTree"; }

  void insert(uint64_t& key, uint64_t value) {
    std::lock_guard<std::mutex> guard(lock);
    tree.insert(std::make_pair(key, value));
  }

  void update(uint64_t& key, uint64_t value) {
    std::lock_guard<std::mutex> guard(lock);
    auto it = tree.find(key);
    if(it != tree.end()) {
      it->second = value;
    }
  }

  void remove(uint64_t& key) {
    std::lock_guard<std::mutex> guard(lock);
    tree.erase(key);
  }

  bool lookup(uint64_t& key, uint64_t& value) {
    auto it = tree.find(key);
    if(it == tree.end()) return false;
    value = it->second;
    return true;
  }

  int scan(uint64_t& key, int num) {
    auto it = tree.lower_bound(key);
    int count = 0;
    for(int i = 0; i < num; i++) {
      if(it == tree.end()) break;
      count++, it++;
    }
    return count;
  }
};

template<>
class IndexGBTree<std::string, uint64_t> : public Index<std::string, uint64_t> {
  struct String {
    using KEY = FeatureBTree::String;
    KEY* key;

    void construct(char* str, int len) {
      key = (KEY*) malloc(sizeof(KEY) + len);
      key->len = len;
      memcpy(key->str, str, len);
    }

    String() : key(nullptr) {}

    explicit String(const std::string& key) {
      construct((char*) key.data(), key.size());
    }

    ~String() {
      free(key);
      key = nullptr;
    }

    String(const String& str) {
      if(str.key != nullptr)
        construct(str.key->str, str.key->len);
    }

    String(String&& str) {
      key = str.key;
      str.key = nullptr;
    }

    String& operator=(const String& str) {
      free(key);
      construct(str.key->str, str.key->len);
    }

    String& operator=(String&& str) {
      free(key);
      key = str.key;
      str.key = nullptr;
    }

    friend bool operator<(const String& k1, const String& k2) {
      return *k1.key < *k2.key;
    }
  };

  btree::btree_map<String, uint64_t> tree;
  std::mutex lock;

 public:
  IndexGBTree() {}

  ~IndexGBTree() {}

  std::string index_type() { return "GoogleBTree"; }

  void insert(std::string& key, uint64_t value) {
    std::lock_guard<std::mutex> guard(lock);
    tree.insert(std::make_pair(String(key), value));
  }

  void update(std::string& key, uint64_t value) {
    std::lock_guard<std::mutex> guard(lock);
    auto it = tree.find(String(key));
    if(it != tree.end()) {
      it->second = value;
    }
  }

  void remove(std::string& key) {
    std::lock_guard<std::mutex> guard(lock);
    tree.erase(String(key));
  }

  bool lookup(std::string& key, uint64_t& value) {
    auto it = tree.find(String(key));
    if(it == tree.end()) return false;
    value = it->second;
    return true;
  }

  int scan(std::string& key, int num) {
    auto it = tree.lower_bound(String(key));
    int count = 0;
    for(int i = 0; i < num; i++) {
      if(it == tree.end()) break;
      count++, it++;
    }
    return count;
  }
};

template<>
class IndexSTX<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  tlx::btree_map<uint64_t, uint64_t> tree;
  std::mutex lock;

 public:
  IndexSTX() {}

  ~IndexSTX() {}

  std::string index_type() { return "STX BTree"; }

  void insert(uint64_t& key, uint64_t value) {
    std::lock_guard<std::mutex> guard(lock);
    tree.insert(std::make_pair(key, value));
  }

  void update(uint64_t& key, uint64_t value) {
    std::lock_guard<std::mutex> guard(lock);
    auto it = tree.find(key);
    if(it != tree.end()) {
      it->second = value;
    }
  }

  void remove(uint64_t& key) {
    std::lock_guard<std::mutex> guard(lock);
    tree.erase(key);
  }

  bool lookup(uint64_t& key, uint64_t& value) {
    auto it = tree.find(key);
    if(it == tree.end()) return false;
    value = it->second;
    return true;
  }

  int scan(uint64_t& key, int num) {
    auto it = tree.lower_bound(key);
    int count = 0;

    for(int i = 0; i < num; i++) {
      if(it == tree.end())break;
      count++, it++;
    }

    return count;
  }
};

template<>
class IndexSTX<std::string, uint64_t> : public Index<std::string, uint64_t> {
  struct String {
    using KEY = FeatureBTree::String;
    KEY* key;

    void construct(char* str, int len) {
      key = (KEY*) malloc(sizeof(KEY) + len);
      key->len = len;
      memcpy(key->str, str, len);
    }

    String() : key(nullptr) {}

    explicit String(const std::string& key) {
      construct((char*) key.data(), key.size());
    }

    ~String() {
      free(key);
      key = nullptr;
    }

    String(const String& str) {
      if(str.key != nullptr)
        construct(str.key->str, str.key->len);
    }

    String(String&& str) {
      key = str.key;
      str.key = nullptr;
    }

    String& operator=(const String& str) {
      free(key);
      construct(str.key->str, str.key->len);
    }

    String& operator=(String&& str) {
      free(key);
      key = str.key;
      str.key = nullptr;
    }

    friend bool operator<(const String& k1, const String& k2) {
      return *k1.key < *k2.key;
    }
  };

  tlx::btree_map<String, uint64_t> tree;
  std::mutex lock;

 public:
  IndexSTX() {}

  ~IndexSTX() {}

  std::string index_type() { return "STX BTree"; }

  void insert(std::string& key, uint64_t value) {
    std::lock_guard<std::mutex> guard(lock);
    tree.insert(std::make_pair(String(key), value));
  }

  void update(std::string& key, uint64_t value) {
    std::lock_guard<std::mutex> guard(lock);
    auto it = tree.find(String(key));
    if(it != tree.end()) {
      it->second = value;
    }
  }

  void remove(std::string& key) {
    std::lock_guard<std::mutex> guard(lock);
    tree.erase(String(key));
  }

  bool lookup(std::string& key, uint64_t& value) {
    auto it = tree.find(String(key));
    if(it == tree.end()) return false;
    value = it->second;
    return true;
  }

  int scan(std::string& key, int num) {
    auto it = tree.lower_bound(String(key));
    int count = 0;

    for(int i = 0; i < num; i++) {
      if(it == tree.end())break;
      count++, it++;
    }

    return count;
  }
};


template<>
Index<uint64_t, uint64_t>* IndexFactory<uint64_t, uint64_t>::get_index(INDEX_TYPE type) {
  switch(type) {
    case ARTOLC:
      return new IndexART<uint64_t, uint64_t>();
    case HOT:
      return new IndexHOT<uint64_t, uint64_t>();
    case BTREEOLC :
      return new IndexBTreeOLC<uint64_t, uint64_t>();
    case FBTREE:
      return new IndexFBTree<uint64_t, uint64_t>();
    case MASSTREE:
      return new IndexMASS<uint64_t, uint64_t>();
    case WORMHOLE:
      return new IndexWH<uint64_t, uint64_t>();
    case GBTREE:
      return new IndexGBTree<uint64_t, uint64_t>();
    case STXBTREE:
      return new IndexSTX<uint64_t, uint64_t>();
    default:
      return nullptr;
  }
}

template<>
Index<std::string, uint64_t>* IndexFactory<std::string, uint64_t>::get_index(INDEX_TYPE type) {
  switch(type) {
    case ARTOLC:
      return new IndexART<std::string, uint64_t>();
    case HOT:
      return new IndexHOT<std::string, uint64_t>();
    case BTREEOLC :
      return new IndexBTreeOLC<std::string, uint64_t>();
    case FBTREE:
      return new IndexFBTree<std::string, uint64_t>();
    case MASSTREE:
      return new IndexMASS<std::string, uint64_t>();
    case WORMHOLE:
      return new IndexWH<std::string, uint64_t>();
    case GBTREE:
      return new IndexGBTree<std::string, uint64_t>();
    case STXBTREE:
      return new IndexSTX<std::string, uint64_t>();
    default:
      return nullptr;
  }
}