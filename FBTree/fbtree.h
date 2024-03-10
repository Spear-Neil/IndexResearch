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

template<typename K, typename V>
class alignas(64) FBTree {
  typedef FeatureBTree::LeafNode<K, V> LeafNode;
  typedef FeatureBTree::InnerNode<K> InnerNode;
  typedef FeatureBTree::KVPair<K, V> KVPair;
  static constexpr int kMaxHeight = 13;

  void* root_;                  // root node
  int tree_depth_;              // tree depth/height
  Epoch* epoch_;                // epoch-based memory reclaimer
  void* root_track_[kMaxHeight];// track the root node

 private:
  Control* control(void* node) { return (Control*) node; }

  InnerNode* inner(void* node) { return (InnerNode*) node; }

  LeafNode* leaf(void* node) { return (LeafNode*) node; }

  bool is_leaf(void* node) { return control(node)->is_leaf(); }

  void latch_exclusive(void* node) { control(node)->latch_exclusive(); }

  void unlatch_exclusive(void* node) { control(node)->unlatch_exclusive(); }

 public:
  FBTree() {
    root_ = malloc(sizeof(LeafNode));
    new(root_) LeafNode();
    tree_depth_ = 1;
    root_track_[0] = root_;
    epoch_ = new Epoch();
  }

  //By default, kv is destruct and then the memory block of kv is freed
  ~FBTree() {
    for(int rid = 0; rid < tree_depth_; rid++) {
      void* node = root_track_[rid];
      if(is_leaf(node)) leaf(node)->~LeafNode();
      else inner(node)->~InnerNode();
      free(node);
    }
    delete epoch_;
  }

  void node_parameter() { Constant<K>::node_parameter(); }

  void statistics() {
    std::map<std::string, double> stat;
    stat["index depth"] = tree_depth_;
    for(int rid = 0; rid < tree_depth_; rid++) {
      void* node = root_track_[rid];
      if(is_leaf(node)) leaf(node)->statistic(stat);
      else inner(node)->statistic(stat);
    }
    stat["load factor"] = stat["kv pair num"] / (stat["leaf num"] * Constant<K>::kNodeSize);

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
    std::vector<void*> path_stack;
    path_stack.reserve(tree_depth_);
    K mid = encode_convert(kv->key);
    void* work, * current = root_;

    //traverse the btree to leaf node, save path to path stack
    while(!is_leaf(current)) {
      work = current;
      if(!inner(work)->to_next(mid, current))
        path_stack.push_back(work); // move to a child or a sibling
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
  KVPair* upsert(K key, V value) {
    void* kv = malloc(sizeof(KVPair));
    new(kv) KVPair{key, value};
    return upsert((KVPair*) kv);
  }

  KVPair* remove(K key) {
    std::vector<void*> path_stack;
    path_stack.reserve(tree_depth_);
    K mid = encode_convert(key);

    void* work, * current = root_;
    //traverse the btree to leaf node, save path to path stack
    while(!is_leaf(current)) {
      work = current;
      if(!inner(work)->to_next(mid, current))
        path_stack.push_back(work);
    }

    // reach leaf node
    latch_exclusive(current);
    while(leaf(current)->to_sibling(key, work)) {
      latch_exclusive(work);
      unlatch_exclusive(current);
      current = work;
    }

    int index, rootid;
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
      else up = inner(work)->border_update(mid, index);

      if(work == root_) { // wor has been latched
        merged = nullptr, up = false;
        next = inner(work)->root_remove();
        if(next) {
          root_ = next, tree_depth_--;
          epoch_->retire(work);
        }
        unlatch_exclusive(current);
      }

      current = work;
    }

    unlatch_exclusive(current);
    return kv;
  }

  // kv should be allocated by malloc
  KVPair* update(KVPair* kv) {
    K key = encode_convert(kv->key);
    void* node = root_;
    while(!is_leaf(node)) {
      inner(node)->to_next(key, node);
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

  KVPair* update(K key, V value) {
    void* kv = malloc(sizeof(KVPair));
    new(kv) KVPair{key, value};
    return update((KVPair*) kv);
  }

  KVPair* lookup(K key) {
    K cvt_key = encode_convert(key);
    void* node = root_;
    while(!is_leaf(node)) {
      inner(node)->to_next(cvt_key, node);
    }

    uint64_t version;
    KVPair* kv;
    do {
      version = control(node)->begin_read();
      while(leaf(node)->to_sibling(key, node)) {
        version = control(node)->begin_read();
      }
      kv = leaf(node)->lookup(key);
    } while(!control(node)->end_read(version));

    return kv;
  }
};

}

#endif //INDEXRESEARCH_FBTREE_H
