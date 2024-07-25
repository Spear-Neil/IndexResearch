/*
 * Copyright (c) 2022, Chen Yuan <yuan.chen@whu.edu.cn>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#ifndef INDEXRESEARCH_FBTREE_H
#define INDEXRESEARCH_FBTREE_H

#include <iostream>
#include <string>
#include <map>
#include <deque>
#include "config.h"
#include "constant.h"
#include "control.h"
#include "inode.h"
#include "lnode.h"
#include "type.h"
#include "epoch.h"

namespace FeatureBTree {

using util::String;
using util::KVPair;
using util::Epoch;
using util::EpochGuard;

template<typename K, typename V>
class alignas(64) FBTree {
  typedef FeatureBTree::LeafNode<K, V> LeafNode;
  typedef FeatureBTree::InnerNode<K> InnerNode;
  static constexpr int kMaxHeight = 13;
  static constexpr int kPrefetchSize = 3;

  void* root_;                  // root node
  int tree_depth_;              // tree depth/height
  Epoch* epoch_;                // epoch-based memory reclaimer
  void* root_track_[kMaxHeight];// track the root node

 public:
  typedef util::KVPair<K, V> KVPair;

  class alignas(32) iterator {
    LeafNode* node_;   // the leaf node pointed by current iterator
    uint64_t version_; // the version of leaf node at the last access
    KVPair* kv_;       // the kv pointed by current iterator, null means the end
    int pos_;          // the ordinal of kv in current node (ordered view)

   public:
    iterator() : node_(nullptr), kv_(nullptr) {}

    iterator(LeafNode* node, uint64_t version, KVPair* kv, int pos) :
      node_(node), version_(version), kv_(kv), pos_(pos) {}

    iterator(const iterator& it)
      : node_(it.node_), version_(it.version_),
        kv_(it.kv_), pos_(it.pos_) {}

    // does current iterator point to the end
    bool end() { return kv_ == nullptr; }

    iterator& advance() {
      assert(kv_ != nullptr);
      LeafNode* node = node_;
      uint64_t version;
      KVPair* next;
      int pos;

      // first, try to get the next kv in current node
      std::tie(next, pos, version) = node->access(kv_, pos_ + 1, version_);

      // if we can't get the next kv in current node, go to its sibling node
      while(next == nullptr) {
        node = (LeafNode*) (node->sibling());
        if(node == nullptr) break;

        // first try to optimistically access the first kv in sibling
        version = ((Control*) node)->begin_read();
        std::tie(next, pos, version) = node->access(nullptr, 0, version);
        // left node in a consistent state, succeed to get next kv
        if(((Control*) node_)->end_read(version_)) break;

        // enforce using bound to get next kv
        std::tie(next, pos, version) = node->access(kv_, 0, 0);
      }

      node_ = node, version_ = version, kv_ = next, pos_ = pos;
      return *this;
    }

    iterator& operator=(const iterator& it) {
      node_ = it.node_, version_ = it.version_;
      kv_ = it.kv_, pos_ = it.pos_;
      return *this;
    }

    KVPair* operator->() { return kv_; }

    KVPair& operator*() { return *kv_; }
  };

 private:
  Control* control(void* node) { return (Control*) node; }

  InnerNode* inner(void* node) { return (InnerNode*) node; }

  LeafNode* leaf(void* node) { return (LeafNode*) node; }

  bool is_leaf(void* node) { return control(node)->is_leaf(); }

  void latch_exclusive(void* node) { control(node)->latch_exclusive(); }

  void unlatch_exclusive(void* node) { control(node)->unlatch_exclusive(); }

  void node_prefetch(void* node) {
    if(Config::kNodePrefetch) {
      for(int i = 0; i < kPrefetchSize; i++)
        prefetcht0((char*) node + i * 64);
    }
  }

  iterator bound(K key, bool upper) {
    // true for upper_bound, false for lower_bound
    assert(epoch_->guarded());
    K cvt_key = encode_convert(key);
    void* node = root_;
    while(!is_leaf(node)) {
      inner(node)->to_next(cvt_key, node);
      node_prefetch(node);
    }

    // reach leaf node
    uint64_t version;
    KVPair* kv;
    int pos;

    auto get_bound = [&]() {
      bool unordered = false;

      do {
        version = control(node)->begin_read();
        while(leaf(node)->to_sibling(key, node)) {
          version = control(node)->begin_read();
        }
        if(!control(node)->ordered()) {
          unordered = true;
          break;
        }
        // if kv pairs in node are ordered, try to get boundary kv without lock
        std::tie(kv, pos) = leaf(node)->bound(key, upper);
      } while(!control(node)->end_read(version));

      // if kv pairs in node are unordered, lock the node and then get boundary kv
      if(unordered) {
        latch_exclusive(node);
        void* sibling;
        while(leaf(node)->to_sibling(key, sibling)) {
          latch_exclusive(sibling);
          unlatch_exclusive(node);
          node = sibling;
        }
        leaf(node)->kv_sort();
        std::tie(kv, pos) = leaf(node)->bound(key, upper);
        version = control(node)->load_version();
        unlatch_exclusive(node);
      }
    };

    get_bound();
    // because high_key is never removed unless merge,
    // so the boundary kv may be on the sibling node
    while(kv == nullptr) {
      node = leaf(node)->sibling();
      if(node == nullptr) break;
      // if kv is null, however node has sibling
      get_bound();
    }

    return iterator((LeafNode*) node, version, kv, pos);
  }

 public:
  FBTree() {
    root_ = malloc(sizeof(LeafNode));
    new(root_) LeafNode();
    tree_depth_ = 1;
    root_track_[0] = root_;
    epoch_ = new Epoch();
  }

  //By default, kv is destruct and then the memory block of kv is freed
  ~FBTree() { // recursive destructor result in stackoverflow
    for(int rid = 0; rid < tree_depth_; rid++) {
      void* node = root_track_[rid], * sibling;
      while(node) {
        if(is_leaf(node)) {
          leaf(node)->~LeafNode();
          sibling = leaf(node)->sibling();
        } else {
          inner(node)->~InnerNode();
          sibling = inner(node)->sibling();
        }
        free(node);
        node = sibling;
      }
    }
    delete epoch_;
  }

  void node_parameter() { Constant<K>::node_parameter(); }

  void statistics() {
    std::map<std::string, double> stat;
    stat["index depth"] = tree_depth_;
    for(int rid = 0; rid < tree_depth_; rid++) {
      void* node = root_track_[rid];
      while(node) {
        if(is_leaf(node)) {
          leaf(node)->statistic(stat);
          node = leaf(node)->sibling();
        } else {
          inner(node)->statistic(stat);
          node = inner(node)->sibling();
        }
      }
    }
    stat["load factor"] = stat["kv pair num"] / (stat["leaf num"] * Constant<K>::kLeafSize);

    std::cout << "-- FBTree statistics" << std::endl;
    for(auto item : stat) {
      if(item.first == "index size") {
        size_t GB = 1024ul * 1024 * 1024;
        std::cout << "  -- " << item.first << ": " << item.second / GB << " GB" << std::endl;
      } else {
        std::cout << "  -- " << item.first << ": " << item.second << std::endl;
      }
    }
  }

  Epoch& get_epoch() { return *epoch_; }

  // kv should be allocated by malloc
  KVPair* upsert(KVPair* kv) {
    assert(epoch_->guarded());
    std::vector<void*> path_stack;
    path_stack.reserve(tree_depth_);
    K mid = encode_convert(kv->key);
    void* work, * current = root_;

    //traverse the btree to leaf node, save path to path stack
    while(!is_leaf(current)) {
      work = current;
      if(!inner(work)->to_next(mid, current))
        path_stack.push_back(work); // move to a child or a sibling
      node_prefetch(current);
    }

    // reach leaf node
    latch_exclusive(current);
    while(leaf(current)->to_sibling(kv->key, work)) {
      latch_exclusive(work);
      unlatch_exclusive(current);
      current = work;
    }

    int index, rootid = 0;// rootid: reverse traversal index
    void* rnode, * next;  // rnode: the new node
    KVPair* old = leaf(current)->upsert(kv, rnode, mid);

    while(rnode != nullptr) { // correctly insert the key to leaf node, splitting
      rootid += 1; // to upper level
      if(current == root_) {
        //root node need splitting
        work = malloc(sizeof(InnerNode));
        new(work) InnerNode();
      } else if(!path_stack.empty()) {
        work = path_stack.back();
        path_stack.pop_back();
      } else {// track to the new level
        work = root_track_[rootid];
        assert(work != nullptr);
      }

      /* set root after it has been latched, otherwise some
       * thread may read root node before it has been set */
      // make the three nodes one logical entity, so no other
      // threads can modify the global var, root, tree_depth
      latch_exclusive(work);
      if(current == root_) {
        root_track_[rootid] = work;
        root_ = work, tree_depth_++;
      }

      while(inner(work)->index_or_sibling(mid, index, next)) {
        assert(next != nullptr);
        latch_exclusive(next);
        unlatch_exclusive(work);
        work = next;
      }
      unlatch_exclusive(current);
      // inner node insertion
      rnode = inner(work)->insert(current, rnode, mid, index);
      current = work;
    }

    unlatch_exclusive(current);
    return old;
  }

  // kv should be allocated by malloc
  template<typename Value>
  KVPair* upsert(K key, const Value& value) {
    void* kv = malloc(sizeof(KVPair));
    new(kv) KVPair{key, value};
    return upsert((KVPair*) kv);
  }

  template<typename Value>
  KVPair* upsert(K key, Value&& value) {
    void* kv = malloc(sizeof(KVPair));
    new(kv) KVPair{key, std::move(value)};
    return upsert((KVPair*) kv);
  }

  KVPair* remove(K key) {
    assert(epoch_->guarded());
    std::vector<void*> path_stack;
    path_stack.reserve(tree_depth_);
    K mid = encode_convert(key);

    void* work, * current = root_;
    //traverse the btree to leaf node, save path to path stack
    while(!is_leaf(current)) {
      work = current;
      if(!inner(work)->to_next(mid, current))
        path_stack.push_back(work);
      node_prefetch(current);
    }

    // reach leaf node
    latch_exclusive(current);
    while(leaf(current)->to_sibling(key, work)) {
      latch_exclusive(work);
      unlatch_exclusive(current);
      current = work;
    }

    int index, rootid = 0;
    void* merged, * next;
    KVPair* kv = leaf(current)->remove(key, merged, mid);

    bool up = false; // need to update upper level key
    while(merged || up) {
      epoch_->retire(merged);
      rootid += 1;

      if(!path_stack.empty()) {
        work = path_stack.back();
        path_stack.pop_back();
      } else {
        work = root_track_[rootid];
      }
      assert(work != nullptr);

      latch_exclusive(work);
      while(inner(work)->index_or_sibling(mid, index, next)) {
        assert(next != nullptr);
        latch_exclusive(next);
        unlatch_exclusive(work);
        work = next;
      }
      if(work != root_) unlatch_exclusive(current);

      if(merged) merged = inner(work)->remove(mid, up, index);
      else up = inner(work)->anchor_update(mid, index);

      if(work == root_) { // work has been latched
        merged = nullptr, up = false;
        next = inner(work)->root_remove();
        if(next) {
          root_ = next, tree_depth_--;
          epoch_->retire(work);
          assert(next == current);
        }
        // ensure root is latched when setting global root, tree_depth,
        // makes the three node one logical entity (old root, the merged
        // node, new root(current)), so no other threads can modify global var
        unlatch_exclusive(current);
      }

      current = work;
    }

    unlatch_exclusive(current);
    return kv;
  }

  // kv should be allocated by malloc
  // update can also be implemented through kv returned by lookup
  KVPair* update(KVPair* kv) {
    assert(epoch_->guarded());
    K key = encode_convert(kv->key);
    void* node = root_;
    while(!is_leaf(node)) {
      inner(node)->to_next(key, node);
      node_prefetch(node);
    }

    uint64_t version;
    do {
      version = control(node)->begin_read();
      while(leaf(node)->to_sibling(kv->key, node)) {
        version = control(node)->begin_read();
      }
      KVPair* old = leaf(node)->update(kv);
      if(old != nullptr) return old; // update succeeded
    } while(!control(node)->end_read(version));

    return nullptr;  // the key doesn't exist
  }

  template<typename Value>
  KVPair* update(K key, const Value& value) {
    void* kv = malloc(sizeof(KVPair));
    new(kv) KVPair{key, value};
    KVPair* ret = update((KVPair*) kv);
    if(ret == nullptr) free(kv);
    return ret;
  }

  template<typename Value>
  KVPair* update(K key, Value&& value) {
    void* kv = malloc(sizeof(KVPair));
    new(kv) KVPair{key, std::move(value)};
    KVPair* ret = update((KVPair*) kv);
    if(ret == nullptr) free(kv);
    return ret;
  }

  KVPair* lookup(K key) {
    assert(epoch_->guarded());
    K cvt_key = encode_convert(key);
    void* node = root_;
    while(!is_leaf(node)) {
      inner(node)->to_next(cvt_key, node);
      node_prefetch(node);
    }

    uint64_t version;
    KVPair* kv;
    do {
      version = control(node)->begin_read();
      while(leaf(node)->to_sibling(key, node)) {
        version = control(node)->begin_read();
      }
      kv = leaf(node)->lookup(key);
      if(kv != nullptr) return kv; // find it
    } while(!control(node)->end_read(version));

    return nullptr; // the key doesn't exist
  }

  iterator begin() {
    assert(epoch_->guarded());
    LeafNode* node = leaf(root_track_[0]);
    assert(node != nullptr);
    iterator it(node, 0, nullptr, 0);
    it.version_ = control(node)->begin_read();
    std::tie(it.kv_, it.pos_, it.version_) =
      node->access(nullptr, 0, it.version_);
    return it;
  }

  iterator lower_bound(K key) {
    return bound(key, false);
  }

  iterator upper_bound(K key) {
    return bound(key, true);
  }
};

template<typename V>
class alignas(64) FBTree<String, V> {
  typedef FeatureBTree::LeafNode<String, V> LeafNode;
  typedef FeatureBTree::InnerNode<String> InnerNode;
  static constexpr int kMaxHeight = 13;
  static constexpr int kBufSize = 256 - sizeof(String);

  void* root_;                  // root node
  int tree_depth_;              // tree depth/height
  Epoch* epoch_;                // epoch-based memory reclaimer
  void* root_track_[kMaxHeight];// track the root node

 public:
  typedef util::KVPair<String, V> KVPair;

  class alignas(32) iterator {
    LeafNode* node_;   // the leaf node pointed by current iterator
    uint64_t version_; // the version of leaf node at the last access
    KVPair* kv_;       // the kv pointed by current iterator, null means the end
    int pos_;          // the ordinal of kv in current node (ordered view)

   public:
    iterator() : node_(nullptr), kv_(nullptr) {}

    iterator(LeafNode* node, uint64_t version, KVPair* kv, int pos) :
      node_(node), version_(version), kv_(kv), pos_(pos) {}

    iterator(const iterator& it)
      : node_(it.node_), version_(it.version_),
        kv_(it.kv_), pos_(it.pos_) {}

    // does current iterator point to the end
    bool end() { return kv_ == nullptr; }

    iterator& advance() {
      assert(kv_ != nullptr);
      LeafNode* node = node_;
      uint64_t version;
      KVPair* next;
      int pos;

      // first, try to get the next kv in current node
      std::tie(next, pos, version) = node->access(kv_, pos_ + 1, version_);

      // if we can't get the next kv in current node, go to its sibling node
      while(next == nullptr) {
        node = (LeafNode*) (node->sibling());
        if(node == nullptr) break;

        // first try to optimistically access the first kv in sibling
        version = ((Control*) node)->begin_read();
        std::tie(next, pos, version) = node->access(nullptr, 0, version);
        // left node in a consistent state, succeed to get next kv
        if(((Control*) node_)->end_read(version_)) break;

        // enforce using bound to get next kv
        std::tie(next, pos, version) = node->access(kv_, 0, 0);
      }

      node_ = node, version_ = version, kv_ = next, pos_ = pos;
      return *this;
    }

    iterator& operator=(const iterator& it) {
      node_ = it.node_, version_ = it.version_;
      kv_ = it.kv_, pos_ = it.pos_;
      return *this;
    }

    KVPair* operator->() { return kv_; }

    KVPair& operator*() { return *kv_; }
  };

 private:
  Control* control(void* node) { return (Control*) node; }

  InnerNode* inner(void* node) { return (InnerNode*) node; }

  LeafNode* leaf(void* node) { return (LeafNode*) node; }

  bool is_leaf(void* node) { return control(node)->is_leaf(); }

  void latch_exclusive(void* node) { control(node)->latch_exclusive(); }

  void unlatch_exclusive(void* node) { control(node)->unlatch_exclusive(); }

  void node_prefetch(void* node) {
    if(Config::kNodePrefetch) {
      for(int i = 0; i < Config::kPrefetchSize; i++)
        prefetcht0((char*) node + i * 64);
    }
  }

  iterator bound(String& key, bool upper) {
    // true for upper_bound, false for lower_bound
    assert(epoch_->guarded());
    void* node = root_;
    Control* parent = control(node);
    uint64_t pversion = 0;
    while(!is_leaf(node)) {
      inner(node)->to_next(key, node, pversion);
      node_prefetch(node);
    }

    // reach leaf node
    uint64_t version;
    KVPair* kv;
    int pos;

    auto get_bound = [&]() {
      bool unordered = false;

      do {
        version = control(node)->begin_read();
        while(leaf(node)->to_sibling(key, node, parent, pversion)) {
          version = control(node)->begin_read();
        }
        if(!control(node)->ordered()) {
          unordered = true;
          break;
        }
        // if kv pairs in node are ordered, try to get boundary kv without lock
        std::tie(kv, pos) = leaf(node)->bound(key, upper);
      } while(!control(node)->end_read(version));

      // if kv pairs in node are unordered, lock the node and then get boundary kv
      if(unordered) {
        latch_exclusive(node);
        void* sibling;
        while(leaf(node)->to_sibling(key, sibling, parent, pversion)) {
          latch_exclusive(sibling);
          unlatch_exclusive(node);
          node = sibling;
        }
        leaf(node)->kv_sort();
        std::tie(kv, pos) = leaf(node)->bound(key, upper);
        version = control(node)->load_version();
        unlatch_exclusive(node);
      }
    };

    get_bound();
    // because high_key is never removed unless merge,
    // so the boundary kv may be on the sibling node
    while(kv == nullptr) {
      node = leaf(node)->sibling();
      if(node == nullptr) break;
      // if kv is null, however node has sibling
      get_bound();
    }

    return iterator((LeafNode*) node, version, kv, pos);
  }

 public:
  FBTree() {
    root_ = malloc(sizeof(LeafNode));
    new(root_) LeafNode();
    tree_depth_ = 1;
    root_track_[0] = root_;
    epoch_ = new Epoch();
  }

  //By default, kv is destruct and then the memory block of kv is freed
  ~FBTree() { // recursive destructor result in stackoverflow
    for(int rid = 0; rid < tree_depth_; rid++) {
      void* node = root_track_[rid], * sibling;
      while(node) {
        if(is_leaf(node)) {
          leaf(node)->~LeafNode();
          sibling = leaf(node)->sibling();
        } else {
          inner(node)->~InnerNode();
          sibling = inner(node)->sibling();
        }
        free(node);
        node = sibling;
      }
    }
    delete epoch_;
  }

  void node_parameter() { Constant<String>::node_parameter(); }

  void statistics() {
    std::map<std::string, double> stat;
    stat["index depth"] = tree_depth_;
    for(int rid = 0; rid < tree_depth_; rid++) {
      void* node = root_track_[rid];
      while(node) {
        if(is_leaf(node)) {
          leaf(node)->statistic(stat);
          node = leaf(node)->sibling();
        } else {
          inner(node)->statistic(stat);
          node = inner(node)->sibling();
        }
      }
    }
    stat["load factor"] = stat["kv pair num"] / (stat["leaf num"] * Constant<String>::kLeafSize);

    std::cout << "-- FBTree statistics" << std::endl;
    for(auto item : stat) {
      if(item.first == "index size" || item.first == "anchor size") {
        size_t GB = 1024ul * 1024 * 1024;
        std::cout << "  -- " << item.first << ": " << item.second / GB << " GB" << std::endl;
      } else {
        std::cout << "  -- " << item.first << ": " << item.second << std::endl;
      }
    }
  }

  Epoch& get_epoch() { return *epoch_; }

  // kv should be allocated by malloc
  KVPair* upsert(KVPair* kv) {
    assert(epoch_->guarded());
    std::vector<void*> path_stack;
    path_stack.reserve(tree_depth_);

    //traverse the btree to leaf node, save path to path stack
    void* work, * current = root_;
    Control* parent = control(current); // the parent of leaf nodes
    uint64_t version = 0; // version of leaf node's parent; init with zero,
    // so an extreme case can perform correctly (no inner node here initially);
    // used to determine whether we need to move to sibling in leaf node

    while(!is_leaf(current)) {
      work = current, parent = control(current);
      if(!inner(work)->to_next(kv->key, current, version)) {
        path_stack.push_back(work);
      } // move to a child or a sibling
      node_prefetch(current);
    }

    // reach leaf node
    latch_exclusive(current);
    while(leaf(current)->to_sibling(kv->key, work, parent, version)) {
      assert(work != nullptr);
      latch_exclusive(work);
      unlatch_exclusive(current);
      current = work;
    }

    int index, rootid = 0;// rootid: reverse traversal index
    void* rnode, * next;  // rnode: the new node
    String* mid = nullptr;
    KVPair* old = leaf(current)->upsert(kv, rnode, mid);

    while(rnode != nullptr) { // correctly insert the key to leaf node, splitting
      rootid += 1; // to upper level
      if(current == root_) {
        //root node need splitting
        work = malloc(sizeof(InnerNode));
        new(work) InnerNode();
      } else if(!path_stack.empty()) {
        work = path_stack.back();
        path_stack.pop_back();
      } else {// track to the new level
        work = root_track_[rootid];
        assert(work != nullptr);
      }

      /* set root after it has been latched, otherwise some
       * thread may read root node before it has been set */
      // make the three nodes one logical entity, so no other
      // threads can modify the global var, root, tree_depth
      latch_exclusive(work);
      if(current == root_) {
        root_track_[rootid] = work;
        root_ = work, tree_depth_++;
      }

      while(inner(work)->index_or_sibling(*mid, next, index)) {
        assert(next != nullptr);
        latch_exclusive(next);
        unlatch_exclusive(work);
        work = next;
      }
      unlatch_exclusive(current);

      // inner node insertion
      rnode = inner(work)->insert(mid, current, rnode, index, epoch_);
      if(rootid == 1) { control(current)->end_splitting(); }
      current = work;
    }

    unlatch_exclusive(current);
    return old;
  }

  // kv should be allocated by malloc
  template<typename Value>
  KVPair* upsert(char* key, int len, const Value& value) {
    auto* kv = KVPair::make_kv(key, len, value);
    return upsert(kv);
  }

  template<typename Value>
  KVPair* upsert(char* key, int len, Value&& value) {
    auto* kv = KVPair::make_kv(key, len, std::move(value));
    return upsert(kv);
  }

  template<typename Value>
  KVPair* upsert(const std::string& key, const Value& value) {
    return upsert((char*) key.data(), key.size(), value);
  }

  template<typename Value>
  KVPair* upsert(const std::string& key, Value&& value) {
    return upsert((char*) key.data(), key.size(), std::move(value));
  }

  KVPair* remove(String& key) {
    assert(epoch_->guarded());
    std::vector<void*> path_stack;
    path_stack.reserve(tree_depth_);

    //traverse the btree to leaf node, save path to path stack
    void* work, * current = root_;
    Control* parent = control(current); // the parent of leaf nodes
    uint64_t version = 0; // version of leaf node's parent; init with zero,
    // so an extreme case can perform correctly (no inner node here initially);
    // used to determine whether we need to move to sibling in leaf node

    while(!is_leaf(current)) {
      work = current, parent = control(current);
      if(!inner(work)->to_next(key, current, version)) {
        path_stack.push_back(work);
      } // move to a child or a sibling
      node_prefetch(current);
    }

    // reach leaf node
    latch_exclusive(current);
    while(leaf(current)->to_sibling(key, work, parent, version)) {
      assert(work != nullptr);
      latch_exclusive(work);
      unlatch_exclusive(current);
      current = work;
    }

    int index, rootid = 0;
    void* merged, * next;
    String* mid;
    KVPair* kv = leaf(current)->remove(key, merged, mid);
    if(merged) epoch_->retire(mid); // anchor keys are only store in leaf nodes

    bool up = false; // need to update upper level key
    while(merged || up) {
      epoch_->retire(merged);
      rootid += 1;

      if(!path_stack.empty()) {
        work = path_stack.back();
        path_stack.pop_back();
      } else {
        work = root_track_[rootid];
      }
      assert(work != nullptr);

      latch_exclusive(work);
      while(inner(work)->index_or_sibling(*mid, next, index)) {
        assert(next != nullptr);
        latch_exclusive(next);
        unlatch_exclusive(work);
        work = next;
      }
      if(work != root_) unlatch_exclusive(current);

      if(merged) merged = inner(work)->remove(mid, up, index, epoch_);
      else up = inner(work)->anchor_update(mid, index, epoch_);

      if(work == root_) { // work has been latched
        merged = nullptr, up = false;
        next = inner(work)->root_remove(epoch_);
        if(next) {
          root_ = next, tree_depth_--;
          epoch_->retire(work);
          assert(next == current);
        }
        // ensure root is latched when setting global root, tree_depth,
        // makes the three node one logical entity (old root, the merged
        // node, new root(current)), so no other threads can modify global var
        unlatch_exclusive(current);
      }

      current = work;
    }

    unlatch_exclusive(current);
    return kv;
  }

  KVPair* remove(char* key, int len) {
    char buf[kBufSize + sizeof(String)];
    String* str;

    if(len <= kBufSize) str = (String*) buf;
    else str = (String*) malloc(sizeof(String) + len);

    str->len = len;
    memcpy(str->str, key, len);
    KVPair* kv = remove(*str);

    if(len > kBufSize) free(str);

    return kv;
  }

  KVPair* remove(const std::string& key) {
    return remove((char*) key.data(), key.size());
  }

  // kv should be allocated by malloc
  // update can also be implemented through kv returned by lookup
  KVPair* update(KVPair* kv) {
    assert(epoch_->guarded());
    void* node = root_;
    Control* parent = control(node);
    uint64_t pversion = 0;

    while(!is_leaf(node)) {
      parent = control(node);
      inner(node)->to_next(kv->key, node, pversion);
      node_prefetch(node);
    }

    uint64_t version;
    KVPair* old;
    do {
      version = control(node)->begin_read();
      while(leaf(node)->to_sibling(kv->key, node, parent, pversion)) {
        version = control(node)->begin_read();
      }
      old = leaf(node)->update(kv);
      if(old != nullptr) return old; // update succeeded
    } while(!control(node)->end_read(version));

    return nullptr;
  }

  template<typename Value>
  KVPair* update(char* key, int len, const Value& value) {
    auto* kv = KVPair::make_kv(key, len, value);
    KVPair* ret = update(kv);
    if(ret == nullptr) free(kv);
    return ret;
  }

  template<typename Value>
  KVPair* update(char* key, int len, Value&& value) {
    auto* kv = KVPair::make_kv(key, len, std::move(value));
    KVPair* ret = update(kv);
    if(ret == nullptr) free(kv);
    return ret;
  }

  template<typename Value>
  KVPair* update(const std::string& key, const Value& value) {
    return update((char*) key.data(), key.size(), value);
  }

  template<typename Value>
  KVPair* update(const std::string& key, Value&& value) {
    return update((char*) key.data(), key.size(), std::move(value));
  }

  KVPair* lookup(String& key) {
    assert(epoch_->guarded());
    void* node = root_;
    Control* parent = control(node);
    uint64_t pversion = 0;

    while(!is_leaf(node)) {
      parent = control(node);
      inner(node)->to_next(key, node, pversion);
      node_prefetch(node);
    }

    uint64_t version;
    KVPair* kv;
    do {
      version = control(node)->begin_read();
      while(leaf(node)->to_sibling(key, node, parent, pversion)) {
        version = control(node)->begin_read();
      }
      kv = leaf(node)->lookup(key);
      if(kv != nullptr) return kv; // find it
    } while(!control(node)->end_read(version));

    return nullptr;
  }

  KVPair* lookup(char* key, int len) {
    char buf[kBufSize + sizeof(String)];
    String* str;

    if(len <= kBufSize) str = (String*) buf;
    else str = (String*) malloc(sizeof(String) + len);

    str->len = len;
    memcpy(str->str, key, len);
    KVPair* kv = lookup(*str);

    if(len > kBufSize) free(str);

    return kv;
  }

  KVPair* lookup(const std::string& key) {
    return lookup((char*) key.data(), key.size());
  }

  iterator begin() {
    assert(epoch_->guarded());
    LeafNode* node = leaf(root_track_[0]);
    assert(node != nullptr);
    iterator it(node, 0, nullptr, 0);
    it.version_ = control(node)->begin_read();
    std::tie(it.kv_, it.pos_, it.version_) =
      node->access(nullptr, 0, it.version_);
    return it;
  }

  iterator lower_bound(String& key) {
    return bound(key, false);
  }

  iterator lower_bound(char* key, int len) {
    char buf[kBufSize + sizeof(String)];
    String* str;

    if(len <= kBufSize) str = (String*) buf;
    else str = (String*) malloc(sizeof(String) + len);

    str->len = len;
    memcpy(str->str, key, len);
    iterator it = lower_bound(*str);

    if(len > kBufSize) free(str);

    return it;
  }

  iterator lower_bound(const std::string& key) {
    return lower_bound((char*) key.data(), key.size());
  }

  iterator upper_bound(String& key) {
    return bound(key, true);
  }

  iterator upper_bound(char* key, int len) {
    char buf[kBufSize + sizeof(String)];
    String* str;

    if(len <= kBufSize) str = (String*) buf;
    else str = (String*) malloc(sizeof(String) + len);

    str->len = len;
    memcpy(str->str, key, len);
    iterator it = upper_bound(*str);

    if(len > kBufSize) free(str);

    return it;
  }

  iterator upper_bound(const std::string& key) {
    return upper_bound((char*) key.data(), key.size());
  }
};

template<typename V>
class FBTree<std::string, V> : public FBTree<String, V> {};

}

#endif //INDEXRESEARCH_FBTREE_H
