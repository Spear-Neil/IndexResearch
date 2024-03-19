#ifndef INDEXRESEARCH_LNODE_H
#define INDEXRESEARCH_LNODE_H

#include <iostream>
#include <atomic>
#include <cassert>
#include <thread>
#include <vector>
#include <tuple>
#include <algorithm>
#include "config.h"
#include "control.h"
#include "constant.h"
#include "compare.h"
#include "type.h"
#include "epoch.h"
#include "hash.h"
#include "common.h"
#include "macro.h"
#include "log.h"

using namespace util;

namespace FeatureBTree {

template<typename K, typename V>
class alignas(Config::kAlignSize) LeafNode {
  static constexpr int kNodeSize = Constant<K>::kNodeSize;
  static constexpr int kMergeSize = Constant<K>::kMergeSize;
  static constexpr std::memory_order load_order = std::memory_order_acquire;
  static constexpr std::memory_order store_order = std::memory_order_release;
  typedef FeatureBTree::KVPair<K, V> KVPair;

  Control control_;
  uint64_t bitmap_;
  K high_key_;
  LeafNode* sibling_;
  char tags_[kNodeSize];
  std::atomic<KVPair*> kvs_[kNodeSize];

 private:
  uint64_t compare_equal(void* p, char c) {
    if(kNodeSize == 64)
      return compare_equal_64(p, c);
    else if(kNodeSize == 32)
      return compare_equal_32(p, c);
    else if(kNodeSize == 16)
      return compare_equal_16(p, c);
  }

  constexpr uint64_t half_fill() {
    if(kNodeSize == 64)
      return 0xFFFF'FFFFul;
    else if(kNodeSize == 32)
      return 0xFFFFul;
    else if(kNodeSize == 16)
      return 0xFFul;
  }

  constexpr int full_idx() {
    if(kNodeSize == 64)
      return -1;
    else if(kNodeSize == 32)
      return 32;
    else if(kNodeSize == 16)
      return 16;
  }

  uint64_t bitmap(int size) {
    if(kNodeSize == 64) {
      if(size == kNodeSize) { return 0x00ul - 1; }
      else { return (0x01ul << size) - 1; }
    } else { return (0x01ul << size) - 1; }
  }

  void merge(void*& merged, K& mid) {
    CONDITION_ERROR(merged != nullptr, "merged node is uninitialized");
    if(control_.has_sibling()) {  // only merge with the right sibling node
      LeafNode* rnode = sibling_;
      CONDITION_ERROR(rnode == nullptr, "sibling is equal to null");
      int lnkey = popcount(bitmap_);
      int rnkey = popcount(rnode->bitmap_);
      // if rnkey == 0 (the rightmost leaf), not merge immediately
      if(lnkey + rnkey <= kMergeSize || lnkey == 0) { // try to merge
        rnode->control_.latch_exclusive();
        rnkey = popcount(rnode->bitmap_);
        // ensure need to merge with right node
        if(lnkey + rnkey <= kMergeSize || lnkey == 0) {
          merged = rnode;
          mid = encode_convert(high_key_);

          // move kv in sibling to current node
          uint64_t mask = rnode->bitmap_;
          int ridx, lidx;
          while(mask) {
            ridx = index_least1(mask); // valid kv in sibling
            lidx = index_least0(bitmap_); // empty slot in current node
            tags_[lidx] = rnode->tags_[ridx];
            // using exchange, because other update operations may happen concurrently
            KVPair* kv = rnode->kvs_[ridx].exchange(nullptr);  // get the latest value, and set it to null
            kvs_[lidx].store(kv, store_order);
            bitmap_ |= (0x01ul << lidx);
            mask &= ~(0x01ul << ridx);
          }
          rnode->bitmap_ = 0;

          // set meta information
          high_key_ = rnode->high_key_;
          sibling_ = rnode->sibling_;
          rnode->sibling_ = this;

          if(!rnode->control_.has_sibling()) {
            control_.clear_sibling();
          }

          rnode->control_.set_delete();
          rnode->control_.update_version();// inform lookup thread
        }
        rnode->control_.unlatch_exclusive();
      }
    }
  }

  KVPair* access(int pos) {
    if(pos < 0 || pos >= kNodeSize) return nullptr;
    uint64_t mask = 0x01ul << pos;
    if(mask & bitmap_ == 0) return nullptr;
    return kvs_[pos].load(load_order);
  }

 public:
  LeafNode() : control_(true), bitmap_(0),
               high_key_(0), sibling_(nullptr) {}

  ~LeafNode() {
    uint64_t mask = bitmap_;
    while(mask) {
      int idx = index_least1(mask);
      KVPair* kv = kvs_[idx].load(load_order);
      kv->~KVPair();
      free(kv);
      mask &= ~(0x01ul << idx);
    }
  }

  void* sibling() {
    if(control_.has_sibling()) { return sibling_; }
    if(control_.deleted()) { return sibling_; }
    return nullptr;
  }

  void statistic(std::map<std::string, double>& stat) {
    stat["index size"] += sizeof(LeafNode);
    stat["leaf num"] += 1;
    stat["kv pair num"] += popcount(bitmap_);
  }

  func_used void exhibit() {
    std::vector<K> keys;
    int nkey = popcount(bitmap_), idx;
    keys.reserve(nkey);
    uint64_t mask = bitmap_;
    while(mask) {
      idx = index_least1(mask);
      KVPair* kv = kvs_[idx].load(load_order);
      keys.push_back(kv->key);
      mask &= ~(0x01ul << idx);
    }
    std::sort(keys.begin(), keys.end());

    std::cout << "leaf node " << control_.deleted() << " " << this << ": ";
    for(int kid = 0; kid < nkey; kid++)
      std::cout << keys[kid] << " ";
    std::cout << std::endl;
  }

  bool to_sibling(K key, void*& next) {
    // key must be normal encoding form
    if(control_.deleted()) { // current node has been deleted
      next = sibling_;
      CONDITION_ERROR(next == nullptr, "to_sibling error: next == nullptr");
      return true;
    }

    if(control_.has_sibling() && high_key_ < key) {
      next = sibling_;
      CONDITION_ERROR(next == nullptr, "to_sibling error: next == nullptr");
      return true;
    }
    return false;
  }

  // lookup can be executed concurrently with lookup, update, upsert, remove, sort
  KVPair* lookup(K key) { // key must be normal encoding form
    char tag = hash(key); // finger print generation
    uint64_t mask = bitmap_ & compare_equal(tags_, tag); // candidates

    while(mask) { // check whether the key exists or not
      int idx = index_least1(mask);
      KVPair* kv = kvs_[idx].load(load_order);
      // some other threads may be splitting or removing or sorting
      if(kv != nullptr && key == kv->key) { return kv; }
      mask &= ~(0x01ul << idx);
    }

    return nullptr;
  }

  // update can be executed concurrently with update, lookup, upsert, remove, sort
  KVPair* update(KVPair* kv) {
    char tag = hash(kv->key); // finger print generation
    uint64_t mask = bitmap_ & compare_equal(tags_, tag); // candidates

    while(mask) {
      int idx = index_least1(mask);
      KVPair* old = kvs_[idx].load(load_order);
      while(old != nullptr && kv->key == old->key) {
        if(kvs_[idx].compare_exchange_strong(old, kv)) {
          return old; // update operation succeeded
        }
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1us);
        // if failed because other threads' updates, try again
        old = kvs_[idx].load(load_order); // get the latest kv
      }
      mask &= ~(0x01ul << idx);
    }

    // failed because other threads' upsert or remove (version has changed)
    // failed because other threads' sort (version has changed)
    // failed because the key doesn't exist
    return nullptr;
  }

  // upsert can be executed concurrently with lookup, update
  KVPair* upsert(KVPair* kv, void*& rnode, K& mid) {
    /* if the key has already existed, update it and return the old kv pointer,
     * otherwise successfully insert the ky, and return a nullptr, mid must be
     * converted to suitable encoding form before return */
    rnode = nullptr; // update or normal insert
    char tag = hash(kv->key); // finger print generation
    uint64_t mask = bitmap_ & compare_equal(tags_, tag); // candidates

    int idx;
    while(mask) {  // check whether the key exists or not
      idx = index_least1(mask);
      KVPair* old = kvs_[idx].load(load_order);
      // old can't be nullptr, must be a valid pointer
      if(kv->key == old->key) {
        // using exchange, because other update operations may happen concurrently
        return kvs_[idx].exchange(kv); // get the latest value, and set it to kv
      }
      mask &= ~(0x01ul << idx);
    }

    control_.update_version();  // it has been confirmed that we need to insert the key
    // into current node even split the node, update node's version, don't need to update
    // the right node's version, because other threads can't access the right node now

    // if kv pairs were originally ordered, to insert a new kv (whether
    // to split or not) will result in unordered kv pairs, in most cases
    // and the new sibling will be initialized with unordered flag
    if(control_.ordered()) control_.clear_order();

    LeafNode* node = this;
    idx = index_least0(bitmap_); // find an empty slot
    //if idx != full_idx, the node has an empty slot
    if(idx == full_idx()) { // full, need split
      // phase 1, keys sorting
      std::vector<std::pair<K, int>> keys;
      keys.reserve(kNodeSize);
      for(int i = 0; i < kNodeSize; i++) {
        KVPair* kv = kvs_[i].load(load_order);
        keys.push_back(std::make_pair(kv->key, i));
      }

      auto less = [](std::pair<K, int>& a,
                     std::pair<K, int>& b) {
        return a.first < b.first;
      };
      std::sort(keys.begin(), keys.end(), less);

      // phase 2, splitting
      rnode = malloc(sizeof(LeafNode));
      new(rnode) LeafNode();
      if(!control_.has_sibling() && kv->key > keys.back().first) {
        /* the rightmost node without sibling and key is greater than
         * all keys especially effective for sequential insertion */
        idx = 0, node = (LeafNode*) rnode;

        /* set corresponding variables before setting flag */
        sibling_ = (LeafNode*) rnode;
        high_key_ = keys.back().first;
        control_.set_sibling();
      } else {
        // normal split, move half key-value pairs to the new node
        mask = 0x00ul;  // clear mask, mark keys moved to right node
        int i = kNodeSize / 2, rid = 0, lid;
        for(; i < kNodeSize; i++, rid++) {
          lid = keys[i].second;
          mask |= (0x01ul << lid);
          ((LeafNode*) rnode)->tags_[rid] = tags_[lid];
          // some other threads may be updating concurrently
          // using exchange to interact with these threads correctly
          // get the latest value, and set it to null
          KVPair* kv = kvs_[lid].exchange(nullptr);
          ((LeafNode*) rnode)->kvs_[rid].store(kv, store_order);
        }

        /* set corresponding variables before setting flag */
        ((LeafNode*) rnode)->bitmap_ = half_fill();
        ((LeafNode*) rnode)->sibling_ = sibling_;
        ((LeafNode*) rnode)->high_key_ = high_key_;

        CONDITION_ERROR(popcount(mask) != kNodeSize / 2, "split error");
        bitmap_ &= ~mask;  // remove keys in leaf node
        CONDITION_ERROR(popcount(bitmap_) != kNodeSize / 2, "split error");
        sibling_ = (LeafNode*) rnode;
        high_key_ = keys[kNodeSize / 2 - 1].first;

        if(!control_.has_sibling()) control_.set_sibling();
        else ((LeafNode*) rnode)->control_.set_sibling();

        if(kv->key > high_key_) {
          idx = kNodeSize / 2;
          node = (LeafNode*) rnode;
        } else { idx = lid; } // less than high key, select an empty slot in left node
      }

      mid = encode_convert(high_key_);
    }

    CONDITION_ERROR((node->bitmap_ & (0x01ul << idx)) != 0, "insert error");
    //insert the key into node
    node->kvs_[idx].store(kv, store_order);
    node->tags_[idx] = tag;
    node->bitmap_ |= (0x01ul << idx);

    return nullptr;
  }

  // upsert can be executed concurrently with lookup, update
  KVPair* remove(K key, void*& mnode, K& mid) { // mnode: merged node
    /* key must be normal encoding form, return the old kv (or nullptr),
     * mid must be converted to suitable encoding form before return */
    mnode = nullptr; // normal remove without merge operation
    char tag = hash(key); // finger print generation
    uint64_t mask = bitmap_ & compare_equal(tags_, tag); // candidates

    while(mask) { // check whether the key exists or not
      int idx = index_least1(mask);
      KVPair* kv = kvs_[idx].load(load_order);
      // kv can't be nullptr, must be a valid pointer
      if(kv->key == key) {
        control_.update_version(); // key exists, update node version
        bitmap_ &= ~(0x01ul << idx); // update bitmap
        // using exchange, because other update operations may happen concurrently
        kv = kvs_[idx].exchange(nullptr); // get the latest value, and set it to null
        merge(mnode, mid);  // try to merge with sibling

        // normal remove kv from a node never change the order, only if to merge current node
        // with its sibling, the order may change; however normal remove results in kv pairs
        // discontinuously stored, which makes scan operation difficult to implement
        if(control_.ordered()) control_.clear_order();

        return kv;
      }
      mask &= ~(0x01ul << idx);
    }

    return nullptr; // key does not exist
  }

  // sort kv pairs, current node need to be latched like remove/upsert,executed concurrently with lookup, update
  // lookup/update never change the order of kv pairs in current node, so the ordered flag never changed
  // upsert/remove may change the order of kv pairs in current node, note modification of the ordered flag
  void kv_sort() {
    if(!control_.ordered()) {
      char tags[kNodeSize];
      std::vector<std::pair<KVPair*, int>> keys;

      uint64_t mask = bitmap_;
      while(mask) {
        int idx = index_least1(mask);
        // get the latest value, and set it to null, inform update threads
        KVPair* kv = kvs_[idx].exchange(nullptr);
        // kv can't be nullptr, must be a valid pointer
        keys.push_back(std::make_pair(kv, idx));
        mask &= ~(0x01ul << idx);
      }

      auto less = [](std::pair<KVPair*, int>& a,
                     std::pair<KVPair*, int>& b) {
        return a.first->key < b.first->key;
      };
      std::sort(keys.begin(), keys.end(), less);

      for(int idx = 0; idx < keys.size(); idx++) {
        auto [kv, pos] = keys[idx];
        tags[idx] = tags_[pos];
        kvs_[idx].store(kv, store_order);
      }
      memcpy(tags_, tags, kNodeSize);
      bitmap_ = bitmap(keys.size());

      control_.set_order();
      control_.update_version();
    }
  }

  std::pair<KVPair*, int> bound(K key, bool upper) {
    // true for upper_bound, false for lower_bound
    // first try to search the key in current node
    int nkey = popcount(bitmap_);
    char tag = hash(key); // finger print generation
    uint64_t mask = bitmap_ & compare_equal(tags_, tag); // candidates

    while(mask) { // check whether the key exists or not
      int idx = index_least1(mask);
      KVPair* kv = kvs_[idx].load(load_order);
      // some other threads may be splitting or removing or sorting
      if(kv != nullptr && key == kv->key) {
        // find the key in current node
        if(upper) {
          if(idx + 1 >= nkey) std::make_pair(nullptr, 0);
          kv = kvs_[idx + 1].load(load_order);
          return std::make_pair(kv, idx + 1);
        }
        return std::make_pair(kv, idx);
      }
      mask &= ~(0x01ul << idx);
    }

    // if we can't find the key in current node
    std::vector<K> keys;
    for(int kid = 0; kid < nkey; kid++) {
      KVPair* kv = kvs_[kid].load(load_order);
      // the key has been removed or moved into other nodes
      if(kv == nullptr) return std::make_pair(nullptr, 0);
      keys.push_back(kv->key);
    }

    // upper_bound is equivalent to lower_bound
    auto it = std::upper_bound(keys.begin(), keys.end(), key);
    int kid = it - keys.begin(); // the ordinal of bound kv in ordered view
    // key is greater than all keys in current node or current node is empty
    if(kid >= nkey) return std::make_pair(nullptr, 0);
    KVPair* kv = kvs_[kid].load(load_order);
    // finally we get bound kv and its ordinal, kv may be null
    return std::make_pair(kv, kid);
  }

  auto access(KVPair* kv, int pos, uint64_t version) {
    // in most cases, kvs are ordered, access kv by pos first
    KVPair* next;
    if(control_.ordered()) {
      next = access(pos);
      // if kvs are ordered, and version hasn't changed
      if(control_.end_read(version))
        return std::tuple(next, pos, version);
    }

    // kvs are unordered or version has changed
    control_.latch_exclusive();
    kv_sort(); // sort kvs
    // kv is valid, get the next kv pair by bound
    if(kv != nullptr) {
      std::tie(next, pos) = bound(kv->key, true);
    } else { next = access(pos); }
    // kv is null, get kv pair by pos, begin()
    version = control_.load_version();
    control_.unlatch_exclusive();

    return std::tuple(next, pos, version);
  }
};

}

#endif //INDEXRESEARCH_LNODE_H
