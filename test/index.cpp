#include <string>
#include <limits>
#include <atomic>
#include <mutex>
#include <csignal>

#include "index.h"
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

using FeatureBTree::String;

template<>
class IndexART<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  ART_OLC::Tree tree;
  Key max_key;

 public:
  IndexART() : tree([](TID tid, Key& key) { key.setInt(((KVType*) tid)->key); }) {
    max_key.setInt(std::numeric_limits<uint64_t>::max());
  }

  ~IndexART() override {}

  std::string index_type() override { return "ART"; }

  void insert(KVType* kv) override {
    static thread_local auto t = tree.getThreadInfo();
    Key k(kv->key);
    tree.insert(k, (TID) kv, t);
  }

  void update(KVType* kv) override {
    insert(kv);
  }

  bool lookup(const uint64_t& key, uint64_t& value) override {
    static thread_local auto t = tree.getThreadInfo();
    Key k(key);
    KVType* kv = (KVType*) tree.lookup(k, t);
    if(kv == nullptr) return false;
    value = kv->value;
    return true;
  }

  int scan(const uint64_t& key, int num) override {
    static thread_local auto t = tree.getThreadInfo();
    Key start(key);
    Key finish;

    TID tids[num];
    size_t count;
    // it seems some bugs in ARTOLC, insert concurrent with scan(ycsb workload e)
    // a node was obsoleted, lookupRange reloads this node over and over
    tree.lookupRange(start, max_key, finish, tids, num, count, t);

    return count;
  }
};

template<>
class IndexART<String, uint64_t> : public Index<String, uint64_t> {
  ART_OLC::Tree tree;
  Key max_key;

  static constexpr int max_len = 255;

 private:
  static void set_key(const char* str, int len, Key& key) {
    // Notes: set key[len] = '\0' to ensure no seg fault
    // ARTOLC may access the byte beyond the length (insert)
    if(len > max_len) len = max_len;
    key.set(str, len), key[len] = '\0';
  }

 public:
  IndexART() : tree([](TID tid, Key& key) {
    KVType* kv = (KVType*) tid;
    set_key(kv->key.str, kv->key.len, key);
  }) {
    char str[max_len];
    memset(str, 0xFF, max_len);
    max_key.set(str, max_len), max_key[max_len] = '\0';
  }

  ~IndexART() override {}

  std::string index_type() override { return "ART"; }

  void insert(KVType* kv) override {
    static thread_local auto t = tree.getThreadInfo();
    Key k;
    set_key(kv->key.str, kv->key.len, k);
    tree.insert(k, (TID) kv, t);
  }

  void update(KVType* kv) override {
    insert(kv);
  }

  bool lookup(const String& key, uint64_t& value) override {
    static thread_local auto t = tree.getThreadInfo();
    Key k;
    set_key(key.str, key.len, k);
    KVType* kv = (KVType*) tree.lookup(k, t);
    if(kv == nullptr) return false;

    value = kv->value;
    return true;
  }

  int scan(const String& key, int num) override {
    static thread_local auto t = tree.getThreadInfo();
    Key start;
    Key finish;
    set_key(key.str, key.len, start);

    TID tids[num];
    size_t count;
    // it seems some bugs in ARTOLC, insert concurrent with scan(ycsb workload e)
    // a node was obsoleted, lookupRange reloads this node over and over
    tree.lookupRange(start, max_key, finish, tids, num, count, t);

    return count;
  }
};

template<>
class IndexHOT<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  template<class KV>
  struct KeyExtractor {
    typedef uint64_t KeyType;

    inline KeyType operator()(const KVType* kv) {
      return kv->key;
    }
  };

  using Trie_t = hot::rowex::HOTRowex<KVType*, KeyExtractor>;
  Trie_t tree;

 public:
  IndexHOT() {}

  ~IndexHOT() override {}

  std::string index_type() override { return "HOT"; }

  void insert(KVType* kv) override { tree.upsert(kv); }

  void update(KVType* kv) override { tree.upsert(kv); }

  bool lookup(const uint64_t& key, uint64_t& value) override {
    idx::contenthelpers::OptionalValue<KVType*> ret = tree.lookup(key);
    if(ret.mIsValid) value = ret.mValue->value;
    return ret.mIsValid;
  }

  int scan(const uint64_t& key, int num) override {
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
class IndexHOT<String, uint64_t> : public Index<String, uint64_t> {
  template<class KV>
  struct KeyExtractor {
    typedef const char* KeyType;

    inline KeyType operator()(const KVType* kv) {
      return kv->key.str;
    }
  };

  using Trie_t = hot::rowex::HOTRowex<KVType*, KeyExtractor>;
  Trie_t tree;

 public:
  IndexHOT() {}

  ~IndexHOT() override {}

  std::string index_type() override { return "HOT"; }

  void insert(KVType* kv) override { tree.upsert(kv); }

  void update(KVType* kv) override { tree.upsert(kv); }

  bool lookup(const String& key, uint64_t& value) override {
    idx::contenthelpers::OptionalValue<KVType*> ret = tree.lookup(key.str);
    if(ret.mIsValid) value = ret.mValue->value;
    return ret.mIsValid;
  }

  int scan(const String& key, int num) override {
    auto iterator = tree.lower_bound(key.str);
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
  btreeolc::BTree<uint64_t, KVType*> tree;

 public:
  IndexBTreeOLC() {}

  ~IndexBTreeOLC() override {}

  std::string index_type() override { return "BTreeOLC"; }

  void insert(KVType* kv) override { tree.insert(kv->key, kv); }

  void update(KVType* kv) override { tree.insert(kv->key, kv); }

  bool lookup(const uint64_t& key, uint64_t& value) override {
    KVType* kv = nullptr;
    bool find = tree.lookup(key, kv);
    // it seems some bugs in BTreeOLC, lookup concurrent with insert(ycsb workload d)
    if(find && kv) value = kv->value;
    return find;
  }

  int scan(const uint64_t& key, int num) override {
    uint64_t start = key;
    KVType* out[num];
    int count = 0;
    while(true) {
      assert(num > count);
      int n = tree.scan(start, num - count, out + count);
      count += n;
      if(n == 0 || count == num) break;
      start = out[count - 1]->key;
    }
    return count;
  }
};

template<>
class IndexBTreeOLC<String, uint64_t> : public Index<String, uint64_t> {
  struct Key {
    String* key;

    Key() : key(nullptr) {}

    Key(const Key& k) = default;

    explicit Key(const String& k) : key(const_cast<String*>(&k)) {}

    Key& operator=(const Key& k) = default;

    Key& operator=(const String& k) {
      key = const_cast<String*>(&k);
      return *this;
    }

    bool operator<(const Key& k) const {
      return *key < *k.key;
    }

    bool operator>(const Key& k) const {
      return *key > *k.key;
    }

    bool operator==(const Key& k) const {
      return *key == *k.key;
    }

    bool operator!=(const Key& k) const {
      return *key != *k.key;
    }
  };

  btreeolc::BTree<Key, KVType*> tree;

 public:
  IndexBTreeOLC() {}

  ~IndexBTreeOLC() override {}

  std::string index_type() override { return "BTreeOLC"; }

  void insert(KVType* kv) override { tree.insert(Key(kv->key), kv); }

  void update(KVType* kv) override { tree.insert(Key(kv->key), kv); }

  bool lookup(const String& key, uint64_t& value) override {
    KVType* kv = nullptr;
    bool find = tree.lookup(Key(key), kv);
    // it seems some bugs in BTreeOLC, lookup concurrent with insert(ycsb workload d)
    if(find && kv) value = kv->value;
    return find;
  }

  int scan(const String& key, int num) override {
    Key start(key);
    KVType* out[num];
    int count = 0;
    while(true) {
      assert(num > count);
      int n = tree.scan(start, num - count, out + count);
      count += n;
      if(n == 0 || count == num) break;
      start = out[count - 1]->key;
    }
    return count;
  }
};

template<>
class IndexFBTree<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  FeatureBTree::FBTree<uint64_t, uint64_t> tree;

 public:
  IndexFBTree() {}

  ~IndexFBTree() override {}

  std::string index_type() override { return "FBTree"; }

  void insert(KVType* kv) override {
    // guarding thread, not single insert/update/lookup/scan operation
    static thread_local EpochGuard guard(tree.get_epoch());
    tree.upsert(kv);
  }

  void update(KVType* kv) override {
    static thread_local EpochGuard guard(tree.get_epoch());
    tree.update(kv);
  }

  bool lookup(const uint64_t& key, uint64_t& value) override {
    static thread_local EpochGuard guard(tree.get_epoch());
    KVType* kv = tree.lookup(key);
    if(kv == nullptr) return false;
    value = kv->value;
    return true;
  }

  int scan(const uint64_t& key, int num) override {
    static thread_local EpochGuard guard(tree.get_epoch());
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
class IndexFBTree<String, uint64_t> : public Index<String, uint64_t> {
  FeatureBTree::FBTree<String, uint64_t> tree;

 public:
  IndexFBTree() {}

  ~IndexFBTree() override {}

  std::string index_type() override { return "FBTree"; }

  void insert(KVType* kv) override {
    static thread_local EpochGuard guard(tree.get_epoch());
    tree.upsert(kv);
  }

  void update(KVType* kv) override {
    static thread_local EpochGuard guard(tree.get_epoch());
    tree.update(kv);
  }

  bool lookup(const String& key, uint64_t& value) override {
    static thread_local EpochGuard guard(tree.get_epoch());
    auto kv = tree.lookup(const_cast<String&>(key));
    if(kv == nullptr) return false;
    value = kv->value;
    return true;
  }

  int scan(const String& key, int num) override {
    static thread_local EpochGuard guard(tree.get_epoch());
    auto it = tree.lower_bound(const_cast<String&>(key));
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
std::mutex mass_lock;
int mass_nthread = 0;

class MassTreeBase {
  struct NodeParam : Masstree::nodeparams<> {
    typedef void* value_type;
    typedef threadinfo threadinfo_type;
  };

  class ThreadGuard {
    threadinfo* info_;

   public:
    ThreadGuard() {
      std::lock_guard guard(mass_lock);
      info_ = threadinfo::make(threadinfo::TI_PROCESS, mass_nthread++);
      info_->rcu_start();
    }

    ~ThreadGuard() {
      info_->rcu_stop();
    }

    threadinfo* info() {
      return info_;
    }
  };

  struct Scanner {
    int count;
    int num;

    Scanner(int size) : count(0), num(size) {}

    template<typename SS, typename K>
    void visit_leaf(const SS&, const K&, threadinfo&) {}

    bool visit_value(lcdf::Str key, void* value, threadinfo&) {
      if(++count < num) return true;

      return false;
    }
  };

  typedef Masstree::basic_table<NodeParam> Tree_t;
  typedef typename Masstree::basic_table<NodeParam>::cursor_type locked_cursor_t;
  typedef typename Masstree::basic_table<NodeParam>::unlocked_cursor_type unlocked_cursor_t;

  Tree_t tree_;

  static void epochinc(int) {
    globalepoch += 2;
    active_epoch = threadinfo::min_active_epoch();
  }

 public:
  MassTreeBase() {
    static ThreadGuard guard;
    tree_.initialize(*guard.info());
    signal(SIGALRM, epochinc);
    itimerval timer{};
    timer.it_value.tv_sec = 1;
    timer.it_value.tv_usec = 0;
    timer.it_interval.tv_sec = 1;
    timer.it_interval.tv_usec = 0;
    int ret = setitimer(ITIMER_REAL, &timer, nullptr);
    assert(ret == 0);
  }

  void upsert(const char* key, int len, void* kv) {
    static thread_local ThreadGuard guard;
    locked_cursor_t lp(tree_, key, len);
    lp.find_insert(*guard.info());
    lp.value() = kv;
    lp.finish(1, *guard.info());
  }

  bool lookup(const char* key, int len, void*& kv) {
    static thread_local ThreadGuard guard;
    unlocked_cursor_t lp(tree_, key, len);
    bool find = lp.find_unlocked(*guard.info());
    if(find) kv = lp.value();
    return find;
  }

  int scan(const char* key, int len, int num) {
    static thread_local ThreadGuard guard;
    lcdf::Str first(key, len);
    Scanner scanner(num);
    int count = tree_.scan(first, false, scanner, *guard.info());
    return count;
  }
};

template<>
class IndexMASS<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  MassTreeBase tree;

 public:
  IndexMASS() {}

  ~IndexMASS() override {}

  std::string index_type() override { return "MassTree"; }

  void insert(KVType* kv) override {
    uint64_t k = byte_swap(kv->key);
    tree.upsert((char*) &k, 8, kv);
  }

  void update(KVType* kv) override {
    uint64_t k = byte_swap(kv->key);
    tree.upsert((char*) &k, 8, kv);
  }

  bool lookup(const uint64_t& key, uint64_t& value) override {
    uint64_t k = byte_swap(key);
    void* kv = nullptr;
    bool find = tree.lookup((char*) &k, 8, kv);
    if(find) value = ((KVType*) kv)->value;
    return find;
  }

  int scan(const uint64_t& key, int num) override {
    uint64_t k = byte_swap(key);
    return tree.scan((char*) &k, 8, num);
  }
};

template<>
class IndexMASS<String, uint64_t> : public Index<String, uint64_t> {
  MassTreeBase tree;

 public:
  IndexMASS() {}

  ~IndexMASS() override {}

  std::string index_type() override { return "MassTree"; }

  void insert(KVType* kv) override {
    tree.upsert(kv->key.str, kv->key.len, kv);
  }

  void update(KVType* kv) override { insert(kv); }

  bool lookup(const String& key, uint64_t& value) override {
    void* kv = nullptr;
    bool find = tree.lookup(key.str, key.len, kv);
    if(find)value = ((KVType*) kv)->value;
    return find;
  }

  int scan(const String& key, int num) override {
    return tree.scan(key.str, key.len, num);
  }
};

template<>
class IndexWH<uint64_t, uint64_t> : public Index<uint64_t, uint64_t> {
  wormhole* wh;

 public:
  IndexWH() { wh = wh_create(); }

  ~IndexWH() override {}

  std::string index_type() override { return "WormHole"; }

  void insert(KVType* kv) override {
    static thread_local wormref* whref = wh_ref(wh);
    uint64_t k = byte_swap(kv->key);
    wh_put(whref, &k, 8, &kv->value, 8);
  }

  void update(KVType* kv) override { insert(kv); }

  bool lookup(const uint64_t& key, uint64_t& value) override {
    static thread_local wormref* whref = wh_ref(wh);
    uint64_t k = byte_swap(key);
    uint32_t vlen;
    return wh_get(whref, &k, 8, &value, 8, &vlen);
  }

  int scan(const uint64_t& key, int num) override {
    static thread_local wormref* whref = wh_ref(wh);
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
class IndexWH<String, uint64_t> : public Index<String, uint64_t> {
  wormhole* wh;

 public:
  IndexWH() { wh = wh_create(); }

  ~IndexWH() override {}

  std::string index_type() override { return "WormHole"; }

  void insert(KVType* kv) override {
    static thread_local wormref* whref = wh_ref(wh);
    wh_put(whref, kv->key.str, kv->key.len, &kv->value, 8);
  }

  void update(KVType* kv) override { insert(kv); }

  bool lookup(const String& key, uint64_t& value) override {
    static thread_local wormref* whref = wh_ref(wh);
    uint32_t vlen;
    return wh_get(whref, key.str, key.len, &value, 8, &vlen);
  }

  int scan(const String& key, int num) override {
    static thread_local wormref* whref = wh_ref(wh);
    wormhole_iter* iter = wh_iter_create(whref);
    wh_iter_seek(iter, key.str, key.len);
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
  btree::btree_map<uint64_t, KVType*> tree;
  std::mutex lock;

 public:
  IndexGBTree() {}

  ~IndexGBTree() override {}

  std::string index_type() override { return "GoogleBTree"; }

  void insert(KVType* kv) override {
    std::lock_guard<std::mutex> guard(lock);
    tree.insert(std::make_pair(kv->key, kv));
  }

  void update(KVType* kv) override {
    std::lock_guard<std::mutex> guard(lock);
    auto it = tree.find(kv->key);
    if(it != tree.end()) it->second = kv;
  }

  bool lookup(const uint64_t& key, uint64_t& value) override {
    auto it = tree.find(key);
    if(it == tree.end()) return false;
    value = it->second->value;
    return true;
  }

  int scan(const uint64_t& key, int num) override {
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
class IndexGBTree<String, uint64_t> : public Index<String, uint64_t> {
  struct Key {
    String* key;

    Key() : key(nullptr) {}

    Key(const Key& k) = default;

    explicit Key(const String& k) : key(const_cast<String*>(&k)) {}

    Key& operator=(const Key& k) = default;

    Key& operator=(const String& k) {
      key = const_cast<String*>(&k);
      return *this;
    }

    bool operator<(const Key& k) const {
      return *key < *k.key;
    }

    bool operator>(const Key& k) const {
      return *key > *k.key;
    }

    bool operator==(const Key& k) const {
      return *key == *k.key;
    }

    bool operator!=(const Key& k) const {
      return *key != *k.key;
    }
  };

  btree::btree_map<Key, KVType*> tree;
  std::mutex lock;

 public:
  IndexGBTree() {}

  ~IndexGBTree() override {}

  std::string index_type() override { return "GoogleBTree"; }

  void insert(KVType* kv) override {
    std::lock_guard<std::mutex> guard(lock);
    tree.insert(std::make_pair(Key(kv->key), kv));
  }

  void update(KVType* kv) override {
    std::lock_guard<std::mutex> guard(lock);
    auto it = tree.find(Key(kv->key));
    if(it != tree.end()) it->second = kv;
  }

  bool lookup(const String& key, uint64_t& value) override {
    auto it = tree.find(Key(key));
    if(it == tree.end()) return false;
    value = it->second->value;
    return true;
  }

  int scan(const String& key, int num) override {
    auto it = tree.lower_bound(Key(key));
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
  tlx::btree_map<uint64_t, KVType*> tree;
  std::mutex lock;

 public:
  IndexSTX() {}

  ~IndexSTX() override {}

  std::string index_type() override { return "STX BTree"; }

  void insert(KVType* kv) override {
    std::lock_guard<std::mutex> guard(lock);
    tree.insert(std::make_pair(kv->key, kv));
  }

  void update(KVType* kv) override {
    std::lock_guard<std::mutex> guard(lock);
    auto it = tree.find(kv->key);
    if(it != tree.end()) it->second = kv;
  }

  bool lookup(const uint64_t& key, uint64_t& value) override {
    auto it = tree.find(key);
    if(it == tree.end()) return false;
    value = it->second->value;
    return true;
  }

  int scan(const uint64_t& key, int num) override {
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
class IndexSTX<String, uint64_t> : public Index<String, uint64_t> {
  struct Key {
    String* key;

    Key() : key(nullptr) {}

    Key(const Key& k) = default;

    explicit Key(const String& k) : key(const_cast<String*>(&k)) {}

    Key& operator=(const Key& k) = default;

    Key& operator=(const String& k) {
      key = const_cast<String*>(&k);
      return *this;
    }

    bool operator<(const Key& k) const {
      return *key < *k.key;
    }

    bool operator>(const Key& k) const {
      return *key > *k.key;
    }

    bool operator==(const Key& k) const {
      return *key == *k.key;
    }

    bool operator!=(const Key& k) const {
      return *key != *k.key;
    }
  };

  tlx::btree_map<Key, KVType*> tree;
  std::mutex lock;

 public:
  IndexSTX() {}

  ~IndexSTX() override {}

  std::string index_type() override { return "STX BTree"; }

  void insert(KVType* kv) override {
    std::lock_guard<std::mutex> guard(lock);
    tree.insert(std::make_pair(Key(kv->key), kv));
  }

  void update(KVType* kv) override {
    std::lock_guard<std::mutex> guard(lock);
    auto it = tree.find(Key(kv->key));
    if(it != tree.end()) it->second = kv;
  }

  bool lookup(const String& key, uint64_t& value) override {
    auto it = tree.find(Key(key));
    if(it == tree.end()) return false;
    value = it->second->value;
    return true;
  }

  int scan(const String& key, int num) override {
    auto it = tree.lower_bound(Key(key));
    int count = 0;

    for(int i = 0; i < num; i++) {
      if(it == tree.end()) break;
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
Index<String, uint64_t>* IndexFactory<String, uint64_t>::get_index(INDEX_TYPE type) {
  switch(type) {
    case ARTOLC:
      return new IndexART<String, uint64_t>();
    case HOT:
      return new IndexHOT<String, uint64_t>();
    case BTREEOLC :
      return new IndexBTreeOLC<String, uint64_t>();
    case FBTREE:
      return new IndexFBTree<String, uint64_t>();
    case MASSTREE:
      return new IndexMASS<String, uint64_t>();
    case WORMHOLE:
      return new IndexWH<String, uint64_t>();
    case GBTREE:
      return new IndexGBTree<String, uint64_t>();
    case STXBTREE:
      return new IndexSTX<String, uint64_t>();
    default:
      return nullptr;
  }
}
