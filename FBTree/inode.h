/*
 * Copyright (c) 2022, Chen Yuan <yuan.chen@whu.edu.cn>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#ifndef INDEXRESEARCH_INODE_H
#define INDEXRESEARCH_INODE_H

#include <iostream>
#include <cassert>
#include <cstring>
#include <vector>
#include <map>
#include "config.h"
#include "type.h"
#include "constant.h"
#include "compare.h"
#include "control.h"
#include "common.h"
#include "macro.h"
#include "log.h"
#include "hash.h"
#include "epoch.h"

namespace FeatureBTree {

using util::String;
using util::Epoch;
using util::compare;
using util::popcount;
using util::index_least1;
using util::countl_zero;
using util::hash;
using util::prefetcht0;
using util::common_prefix;
using util::aligned;
using util::roundup;

/* store anchor keys in a contiguous memory block */
class Extent {
  int mlen_;   // total size of available space
  int used_;   // used size of available space
  int free_;   // freed size (anchor remove/update)
  int huge_;   // offset, points to huge prefix
  char mem_[]; // available space

 public:
  Extent() = delete;

  void init(int len) {
    assert(len % Config::kExtentSize == 0);
    mlen_ = len - sizeof(Extent);
    used_ = 0, free_ = 0, huge_ = 0;
  }

  // total size of extent, metadata and available space
  int size() { return mlen_ + sizeof(Extent); }

  // used size of extent, metadata and valid anchors
  int used() { return used_ - free_ + sizeof(Extent); }

  // left size of available space
  int left() { return mlen_ - used_; }

  String* huge() { return (String*) (mem_ + huge_); }

  void huge(String* key) {
    assert((ptrdiff_t(key) < ptrdiff_t(mem_ + used_)
            && ptrdiff_t(key) >= ptrdiff_t(mem_)));
    huge_ = ptrdiff_t(key) - ptrdiff_t(mem_);
  }

  String* make_anchor(String* key) {
    // contiguously malloc a memory block
    if(mlen_ - used_ >= key->len + sizeof(String)) {
      auto* ret = (String*) (mem_ + used_);
      String::make_string(ret, key->str, key->len);
      used_ += key->len + sizeof(String);
      return ret;
    }
    // no more space, require realloc
    return nullptr;
  }

  void ruin_anchor(String* key) {
    assert(ptrdiff_t(key) < ptrdiff_t(mem_ + used_)
             && ptrdiff_t(key) >= ptrdiff_t(mem_));
    // only record how many bytes are freed, because these bytes
    // may be accessed by other threads, realloc if necessary
    free_ += key->len + sizeof(String);
  }
};


template<typename K>
class alignas(Config::kAlignSize) InnerNode {
  static constexpr int kNodeSize = Constant<K>::kInnerSize;
  static constexpr int kMergeSize = Constant<K>::kInnerMergeSize;
  static constexpr int kFeatureSize = Constant<K>::kFeatureSize;
  static constexpr int kBitCnt = 64; // bits number of bitmap

  Control control_; // synchronization, memory/compiler order
  int knum_;        // the number of keys
  int plen_;        // the length of prefix
  char prefix_[8];  // prefix, shift subsequent bytes left
  void* next_;      // sibling or last child
  char features_[kFeatureSize][kNodeSize];
  void* children_[kNodeSize];

 private:
  uint64_t compare_equal(void* p, char c) {
    if(kNodeSize == 64)
      return compare_equal_64(p, c);
    else if(kNodeSize == 32)
      return compare_equal_32(p, c);
    else if(kNodeSize == 16)
      return compare_equal_16(p, c);
  }

  uint64_t compare_less(void* p, char c) {
    if(kNodeSize == 64)
      return compare_less_64(p, c);
    else if(kNodeSize == 32)
      return compare_less_32(p, c);
    else if(kNodeSize == 16)
      return compare_less_16(p, c);
  }

  uint64_t bitmap() {
    CONDITION_ERROR(knum_ < 0 || knum_ > kNodeSize, "error knum");
    if(kNodeSize == 64) {
      if(knum_ == kNodeSize) { return 0x00ul - 1; }
      else { return (0x01ul << knum_) - 1; }
    } else { return (0x01ul << knum_) - 1; }
  }

  int to_next_phase1(K key, void*& next, bool& to_sibling) {
    // key must have been converted to suitable encoding form
    CONDITION_ERROR(plen_ < 0 || plen_ > kFeatureSize, "prefix length, error");
    int pid = 0;
    for(; pid < plen_; pid++) {
      if(((char*) &key)[pid] != prefix_[pid]) break;
    }

    // key is equal to prefix
    if(pid == plen_) {
      return 0;
    }

    // key is less than prefix
    if(((char*) &key)[pid] < prefix_[pid]) {
      next = children_[0];
      CONDITION_ERROR(next == nullptr, "next can't be null");
      return -1;
    }

    // key is greater than prefix
    next = next_;
    CONDITION_ERROR(next == nullptr, "next can't be null");
    if(control_.has_sibling()) to_sibling = true;
    return 1;
  }

  int index_phase1(K key, int& index, void*& next, bool& to_sibling) {
    // key must have been converted to suitable encoding form
    CONDITION_ERROR(plen_ < 0 || plen_ > kFeatureSize, "prefix length, error");
    int pid = 0;
    for(; pid < plen_; pid++) {
      if(((char*) &key)[pid] != prefix_[pid]) break;
    }

    // key is equal to prefix
    if(pid == plen_) {
      return 0;
    }

    // key is less than prefix
    if(((char*) &key)[pid] < prefix_[pid]) {
      index = 0;
      return -1;
    }

    // key is greater than prefix
    if(!control_.has_sibling()) index = knum_; // current node is the right most node
    else next = next_, to_sibling = true;

    return 1;
  }

  void key_insert(K mid, int index) {
    if(knum_ == 0) { // new node
      *(K*) prefix_ = mid, plen_ = kFeatureSize;
    } else {
      CONDITION_ERROR(knum_ - index < 0, "key insert error");
      for(int rid = 0; rid < kFeatureSize - plen_; rid++) {
        char* src = features_[rid] + index, * dst = src + 1;
        memmove(dst, src, knum_ - index);
        features_[rid][index] = ((char*) &mid)[rid + plen_];
      }

      CONDITION_ERROR(plen_ < 0 || plen_ > kFeatureSize, "error prefix length");
      if(index == 0 || index == knum_) {
        int pid = 0;
        for(; pid < plen_; pid++) {
          if(prefix_[pid] != ((char*) &mid)[pid]) break;
        }
        if(pid < plen_) { // prefix reduction
          void* src = features_[0], * dst = features_[plen_ - pid];
          memmove(dst, src, (kFeatureSize - plen_) * kNodeSize);
          CONDITION_ERROR(knum_ + 1 > kNodeSize, "error knum");
          for(int rid = 0; rid < plen_ - pid; rid++) {
            memset(features_[rid], prefix_[pid + rid], knum_ + 1);
            features_[rid][index] = ((char*) &mid)[rid + pid];
          }
          plen_ = pid;
        }
      }
    }
  }

  void key_remove(int index) {
    CONDITION_ERROR(knum_ < 2 || index >= knum_ - 1, "key remove error");
    for(int rid = 0; rid < kFeatureSize - plen_; rid++) {
      char* dst = features_[rid] + index, * src = dst + 1;
      memmove(dst, src, knum_ - index - 1);
    }

    if(index == 0) {
      int pid = 0; // prefix extension
      for(; pid < kFeatureSize - plen_; pid++) {
        if(features_[pid][0] != features_[pid][knum_ - 2]) break;
        prefix_[plen_ + pid] = features_[pid][0];
      }
      if(pid > 0) {
        void* src = features_[pid], * dst = features_[0];
        CONDITION_ERROR(kFeatureSize - plen_ - pid < 0, "key remove error");
        memmove(dst, src, (kFeatureSize - plen_ - pid) * kNodeSize);
        plen_ += pid;
      }
    }
  }

  void memmove64(void* src, void* dst, int n, bool forward) {
    CONDITION_ERROR(n < 0 || n > kNodeSize, "memmove64 error");
    assert(aligned(src, 8) && aligned(dst, 8));
    if(forward) {
      for(int idx = 0; idx < n; idx++)
        ((uint64_t*) dst)[idx] = ((uint64_t*) src)[idx];
    } else {
      for(int idx = n - 1; idx > -1; idx--)
        ((uint64_t*) dst)[idx] = ((uint64_t*) src)[idx];
    }
  }

  void memory_expand() {
    void* src = features_[0], * dst = features_[plen_];
    memmove(dst, src, (kFeatureSize - plen_) * kNodeSize);
    for(int rid = 0; rid < plen_; rid++) {
      memset(features_[rid], prefix_[rid], kNodeSize);
    }
  }

  void memory_shrink() {
    int pid = 0;
    for(; pid < kFeatureSize; pid++) {
      prefix_[pid] = features_[pid][0];
      if(prefix_[pid] != features_[pid][knum_ - 1]) break;
    }
    plen_ = pid;
    memmove(features_[0], features_[pid], (kFeatureSize - pid) * kNodeSize);
  }

  void* split(void* lchild, void* rchild, K& mid, int index) {
    void* src, * dst;
    InnerNode* rnode = (InnerNode*) malloc(sizeof(InnerNode));
    new(rnode) InnerNode();
    rnode->next_ = next_, next_ = rnode;
    /* set corresponding variables before setting flag */
    if(!control_.has_sibling()) control_.set_sibling();
    else rnode->control_.set_sibling();

    int midx;
    if(index == kNodeSize) {// index == kNodeSize can only exist in the rightmost node
      // the rightmost node without sibling and key is greater than all keys
      *(K*) rnode->prefix_ = mid;
      rnode->plen_ = kFeatureSize;
      rnode->children_[0] = lchild;
      rnode->next_ = rchild;
      rnode->knum_ = 1;

      midx = kNodeSize - 1;
    } else if(index < kNodeSize / 2) {
      memory_expand();
      // move right half separators and children to right node
      for(int rid = 0; rid < kFeatureSize; rid++) {
        src = features_[rid] + kNodeSize / 2;
        dst = rnode->features_[rid];
        memcpy(dst, src, kNodeSize / 2);
      }
      src = children_ + kNodeSize / 2;
      dst = rnode->children_;
      memmove64(src, dst, kNodeSize / 2, true);

      //insert lhigh to left inner node
      for(int rid = 0; rid < kFeatureSize; rid++) {
        src = features_[rid] + index;
        dst = features_[rid] + index + 1;
        memmove(dst, src, kNodeSize / 2 - index);
        features_[rid][index] = ((char*) &mid)[rid];
      }
      src = children_ + index, dst = children_ + index + 1;
      memmove64(src, dst, kNodeSize / 2 - index, false);
      children_[index + 1] = rchild;

      knum_ = kNodeSize / 2 + 1;
      rnode->knum_ = kNodeSize / 2;
      memory_shrink();
      rnode->memory_shrink();

      midx = kNodeSize / 2;
    } else { // kNodeSize / 2 <= index < kNodeSize
      memory_expand();
      for(int rid = 0; rid < kFeatureSize; rid++) {
        src = features_[rid] + kNodeSize / 2;
        dst = rnode->features_[rid];
        memcpy(dst, src, index - kNodeSize / 2);
        rnode->features_[rid][index - kNodeSize / 2] = ((char*) &mid)[rid];
        src = features_[rid] + index;
        dst = rnode->features_[rid] + index - kNodeSize / 2 + 1;
        memcpy(dst, src, kNodeSize - index);
      }

      src = children_ + kNodeSize / 2, dst = rnode->children_;
      memmove64(src, dst, index - kNodeSize / 2 + 1, true);
      rnode->children_[index - kNodeSize / 2 + 1] = rchild;
      src = children_ + index + 1;
      dst = rnode->children_ + index - kNodeSize / 2 + 2;
      memmove64(src, dst, kNodeSize - index - 1, true);

      knum_ = kNodeSize / 2;
      rnode->knum_ = kNodeSize / 2 + 1;
      memory_shrink();
      rnode->memory_shrink();

      midx = kNodeSize / 2 - 1;
    }

    for(int rid = 0; rid < kFeatureSize; rid++) {
      if(rid < plen_) ((char*) &mid)[rid] = prefix_[rid];
      else ((char*) &mid)[rid] = features_[rid - plen_][midx];
    }

    return rnode;
  }

  void* merge(K& mid) {
    // only merge with the right sibling node
    void* merged = nullptr;
    if(control_.has_sibling()) {
      InnerNode* rnode = (InnerNode*) next_;
      int rnkey = rnode->knum_;
      CONDITION_ERROR(knum_ < 1, "merge error");
      if(knum_ + rnkey <= kMergeSize || rnkey == 0) { // try to merge
        rnode->control_.latch_exclusive();
        rnkey = rnode->knum_;
        // if rnkey = 0 (rightmost inner), merge immediately
        // ensure need to merge with right node
        if(knum_ + rnkey <= kMergeSize || rnkey == 0) {
          merged = rnode;
          for(int rid = 0; rid < kFeatureSize; rid++) {
            if(rid < plen_) ((char*) &mid)[rid] = prefix_[rid];
            else ((char*) &mid)[rid] = features_[rid - plen_][knum_ - 1];
          }

          // move right node separator to current node
          memory_expand(), rnode->memory_expand();
          void* src, * dst;
          for(int rid = 0; rid < kFeatureSize; rid++) {
            src = rnode->features_[rid];
            dst = features_[rid] + knum_;
            memcpy(dst, src, rnkey);
          }
          src = rnode->children_, dst = children_ + knum_;
          memmove64(src, dst, rnkey, true);
          knum_ += rnkey, rnode->knum_ = 0;
          memory_shrink();

          // set meta information
          next_ = rnode->next_, rnode->next_ = this;
          if(!rnode->control_.has_sibling()) {
            control_.clear_sibling();
          }
          rnode->control_.set_delete();
          rnode->control_.update_version();
        }
        rnode->control_.unlatch_exclusive();
      }
    }

    return merged;
  }

  void bound_extension() { // prefix extension
    CONDITION_ERROR(knum_ < 1, "bound extension error");
    int pid = 0;
    for(; pid < kFeatureSize - plen_; pid++) {
      if(features_[pid][0] != features_[pid][knum_ - 1]) break;
      prefix_[plen_ + pid] = features_[pid][0];
    }
    if(pid > 0) {
      void* src = features_[pid], * dst = features_[0];
      CONDITION_ERROR(kFeatureSize - plen_ - pid < 0, "bound extension error");
      memmove(dst, src, (kFeatureSize - plen_ - pid) * kNodeSize);
      plen_ += pid;
    }
  }

  void* bound_remove(K& mid, bool& up, int index) {
    CONDITION_ERROR(up != false, "up is uninitialized");
    void* merged = nullptr;
    if(!control_.has_sibling()) {
      // current node is the right-most node
      next_ = children_[index];
      knum_ -= 1;
      if(knum_ == 0) plen_ = 0;
      else bound_extension();
    } else {
      InnerNode* rnode = (InnerNode*) next_;
      rnode->control_.latch_exclusive();
      rnode->control_.update_version();
      int rnkey = rnode->knum_;

      // if current has no key after border remove, or if sibling has no key (rightmost
      // node) or has only one key, or if the number of keys in current and sibling after
      // border remove is less than or equal to kMergeSize, merge the two node
      if(index + rnkey <= kMergeSize || index == 0
         || rnkey == 0 || rnkey == 1) {
        merged = rnode; // border remove, so do not need to reset mid.
        // move right node key to current node
        memory_expand(), rnode->memory_expand();
        void* src, * dst;
        for(int rid = 0; rid < kFeatureSize; rid++) {
          src = rnode->features_[rid];
          dst = features_[rid] + index;
          memcpy(dst, src, rnkey);
        }
        src = rnode->children_ + 1, dst = children_ + knum_;
        memmove64(src, dst, rnkey ? rnkey - 1 : 0, true);
        knum_ += rnkey - 1, rnode->knum_ = 0;
        memory_shrink();

        // set meta information
        if(rnkey != 0) next_ = rnode->next_;
        else next_ = children_[index]; // rightmost node has no key
        rnode->next_ = this;
        if(!rnode->control_.has_sibling())
          control_.clear_sibling();
        rnode->control_.set_delete();
      } else {
        // to ensure other threads execute correctly when border remove happens
        // move the last one child (the child after merge) to sibling
        up = true;
        for(int rid = 0; rid < kFeatureSize; rid++) {
          if(rid < plen_) ((char*) &mid)[rid] = prefix_[rid];
          else ((char*) &mid)[rid] = features_[rid - plen_][index - 1];
        }
        rnode->children_[0] = children_[index];
        knum_ -= 1;
        bound_extension();
      }

      rnode->control_.unlatch_exclusive();
    }

    return merged;
  }

 public:
  InnerNode() : control_(false), knum_(0), plen_(0), next_(nullptr) {}

  ~InnerNode() {}

  void* sibling() {
    if(control_.has_sibling()) { return next_; }
    return nullptr;
  }

  void statistic(std::map<std::string, double>& stat) {
    stat["index size"] += sizeof(InnerNode);
    stat["inner num"] += 1;
  }

  func_used void exhibit() {
    K keys[kNodeSize];
    for(int kid = 0; kid < knum_; kid++) {
      K key = *(K*) prefix_;
      for(int fid = 0; fid < kFeatureSize - plen_; fid++)
        ((char*) &key)[fid + plen_] = features_[fid][kid];
      keys[kid] = encode_reconvert(key);
    }
    std::cout << "inner node " << this << ": ";
    for(int kid = 0; kid < knum_; kid++) {
      std::cout << kid << " " << keys[kid] << "| ";
    }
    std::cout << std::endl;
  }

  bool to_next(K key, void*& next) {
    // key must have been converted to suitable encoding form
    bool to_sibling;
    uint64_t init_version;

    while(true) { // make sure the procedure of to_next is atomic
      to_sibling = false; // make sure to_sibling is initialized with false even retry
      init_version = control_.begin_read();

      if(control_.deleted()) { // current node has been deleted, jump to its left node
        to_sibling = true, next = next_;
        CONDITION_ERROR(next == nullptr, "next can't be null");
        break;
      }

      int pcmp = to_next_phase1(key, next, to_sibling);
      if(!pcmp) { // key is equal to prefix
        int idx, rid = 0, plen = plen_;
        uint64_t mask, eqmask = bitmap();
        for(; rid + plen < kFeatureSize; rid++) {
          mask = compare_equal(features_[rid], ((char*) &key)[rid + plen]);
          mask = mask & eqmask;
          if(mask == 0) break;
          eqmask = mask;
        }

        if(rid + plen < kFeatureSize) { // less comparison
          mask = compare_less(features_[rid], ((char*) &key)[rid + plen]);
          mask = mask & eqmask;

          // less than features corresponding to mask
          if(mask == 0) {
            if(eqmask == 0) { idx = 0; }// the right most node, all separators
              // have been deleted, but hasn't been merged to its left sibling
            else { idx = index_least1(eqmask); }
          } else { idx = kBitCnt - countl_zero(mask); }
        } else {
          CONDITION_ERROR((popcount(eqmask) != 1), "more than two candidates");
          idx = index_least1(eqmask);
        }

        if(idx == knum_) {
          next = next_; // key is greater than all separator
          if(control_.has_sibling()) to_sibling = true;
        } else { next = children_[idx]; }
      }

      CONDITION_ERROR(next == nullptr, "next can't be null");
      if(control_.end_read(init_version)) break;
    }

    return to_sibling;
  }

  bool index_or_sibling(K key, int& index, void*& next) {
    // key must have been converted to suitable encoding form
    bool to_sibling = false;

    if(control_.deleted()) { // current node has been deleted, jump to its left node
      next = next_;
      CONDITION_ERROR(next == nullptr, "next can't be null");
      return true;
    }

    int pcmp = index_phase1(key, index, next, to_sibling);
    if(!pcmp) {
      int rid = 0, plen = plen_; // rid: row index, from higher byte to lower byte
      uint64_t mask, eqmask = bitmap();
      for(; rid + plen < kFeatureSize; rid++) {
        mask = compare_equal(features_[rid], ((char*) &key)[rid + plen]);
        mask = mask & eqmask;
        if(mask == 0) break;
        eqmask = mask;
      }

      if(rid + plen < kFeatureSize) { // less comparison, key is not in this inner node
        // may be a deletion happened, or we need to jump to sibling node
        mask = compare_less(features_[rid], ((char*) &key)[rid + plen]);
        mask = mask & eqmask;

        // less than features corresponding to eqmask, a deletion may have happened
        if(mask == 0) {
          if(eqmask == 0) index = 0; // new root node; or, the right most node, all
            // separators have been deleted, but hasn't been merged to its left sibling
          else index = index_least1(eqmask);
        } else {
          index = kBitCnt - countl_zero(mask);
          // greater than all keys, meanwhile current node is not the rightmost node
          if(index == knum_ && control_.has_sibling()) {
            next = next_, to_sibling = true;
            CONDITION_ERROR(next == nullptr, "next can't be null");
          }
        }
      } else {
        CONDITION_ERROR((popcount(eqmask) != 1), "more than two candidates");
        index = index_least1(eqmask);
      }
    }

    return to_sibling;
  }

  void* insert(void* lchild, void* rchild, K& mid, int index) {
    // mid must have been converted to suitable encoding form
    control_.update_version();
    CONDITION_ERROR(lchild == nullptr || rchild == nullptr || index < 0 || index > knum_, "insert error");

    if(knum_ < kNodeSize) {  // safe, insert mid into current node
      key_insert(mid, index);
      if(index != knum_) { // new node or rightmost node
        void* src = children_ + index + 1;
        void* dst = children_ + index + 2;
        CONDITION_ERROR(knum_ - index - 1 < 0, "insert error");
        memmove64(src, dst, knum_ - index - 1, false);
        children_[index + 1] = rchild;
      } else { children_[index] = lchild, next_ = rchild; }

      knum_ += 1;
      return nullptr;
    } else {// unsafe, split the node
      return split(lchild, rchild, mid, index);
    }
  }

  void* remove(K& mid, bool& up, int index) {
    // mid must have been converted to suitable encoding form
    control_.update_version(), up = false;
    CONDITION_ERROR(index < 0 || index >= knum_, "remove error");
    if(index < knum_ - 1) {
      // knum is 2 at least, the merged node is in current node
      key_remove(index);
      void* src = children_ + index + 2;
      void* dst = children_ + index + 1;
      CONDITION_ERROR(knum_ - index - 2 < 0, "remove error");
      memmove64(src, dst, knum_ - index - 2, true);

      knum_ -= 1; // knum >= 1
      CONDITION_ERROR(knum_ < 1, "remove error");
      return merge(mid);
    }

    // index == nkey - 1
    CONDITION_ERROR(index != knum_ - 1, "remove error");
    return bound_remove(mid, up, index);
  }

  bool anchor_update(K mid, int index) {
    CONDITION_ERROR(index < 0 || index >= knum_, "anchor update error");
    control_.update_version();
    memory_expand();
    for(int rid = 0; rid < kFeatureSize; rid++)
      features_[rid][index] = ((char*) &mid)[rid];
    memory_shrink();

    return control_.has_sibling() && (knum_ - 1) == index;
  }

  void* root_remove() {
    if(knum_ == 0) {
      control_.set_delete();
      return next_; // the new root
    }
    return nullptr;
  }
};

template<>
class alignas(Config::kAlignSize) InnerNode<String> {
  static constexpr int kNodeSize = Constant<String>::kInnerSize;
  static constexpr int kMergeSize = Constant<String>::kInnerMergeSize;
  static constexpr int kFeatureSize = Constant<String>::kFeatureSize;
  static constexpr bool kExtentOpt = Config::kExtentOpt;
  static constexpr int kExtentSize = Config::kExtentSize;
  static constexpr int kEmbedPrSize = 224; // length of embedded prefix
  static constexpr int kBitCnt = 64; // bits number of bitmap
  /* for a slab memory allocator like jemalloc, malloc always allocates a memory
   * block whose size is grater than or equal to the size we need, so we use the
   * excess memory for embedded prefix: the default size of embedded prefix is 224,
   * 1536 - 32 - 4 * 64 - 8 * 64 - 8 * 64 = 224, feature size: 4, node size: 64
   * 896 - 32 - 4 * 32 - 8 * 32 - 8 * 32 = 224, feature size: 4, node size: 32 */
  /* if kExtentOpt(false), anchors are actually stored in leaf nodes, inner nodes only
   * store pointers to anchors, else store anchors with contiguous memory in extent */

  Control control_;  // synchronization, memory/compiler order
  int knum_;         // the number of anchor/separator keys
  int plen_;         // the length of prefix, embed if possible
  union {
    Extent* extent_; // contiguous memory block storing anchors/prefix
    String* huge_;   // prefix can't be embedded, points to first anchor
  };
  void* next_;       // sibling or last child (right-most node)

  char features_[kFeatureSize][kNodeSize];
  char tiny_[kEmbedPrSize];    // prefix that can be embedded
  String* anchors_[kNodeSize]; // anchor keys, just pointers
  void* children_[kNodeSize];  // child nodes

 private:
  uint64_t compare_equal(void* p, char c) {
    if(kNodeSize == 64)
      return compare_equal_64(p, c);
    else if(kNodeSize == 32)
      return compare_equal_32(p, c);
    else if(kNodeSize == 16)
      return compare_equal_16(p, c);
  }

  uint64_t compare_less(void* p, char c) {
    if(kNodeSize == 64)
      return compare_less_64(p, c);
    else if(kNodeSize == 32)
      return compare_less_32(p, c);
    else if(kNodeSize == 16)
      return compare_less_16(p, c);
  }

  uint64_t bitmap() {
    CONDITION_ERROR(knum_ < 0 || knum_ > kNodeSize, "error knum");
    if(kNodeSize == 64) {
      if(knum_ == kNodeSize) { return 0x00ul - 1; }
      else { return (0x01ul << knum_) - 1; }
    } else { return (0x01ul << knum_) - 1; }
  }

  /* 0: equal, minus value: key less than node prefix */
  int prefix_compare(String& key) {
    int plen = plen_, pcmp;
    int cmps = std::min(key.len, plen);

    if(cmps <= kEmbedPrSize) { // embedded prefix
      prefetcht0(tiny_); // prefetch a cache line
      pcmp = memcmp(key.str, tiny_, cmps);
    } else if(!kExtentOpt && huge_->len >= cmps) {
      pcmp = memcmp(key.str, huge_->str, cmps);
    } else if(kExtentOpt && extent_->huge()->len >= cmps) {
      pcmp = memcmp(key.str, extent_->huge()->str, cmps);
    } // current node has modified by others, retry

    // key.len < plen: can't be equal
    if(!pcmp && cmps < plen) return -1;
    return pcmp;
  }

  int to_next_phase1(String& key, void*& next, bool& to_sibling) {
    int pcmp = prefix_compare(key);

    if(pcmp < 0) {
      // key is less than node prefix
      next = children_[0];
    } else if(pcmp > 0) {
      // key is greater than node prefix
      next = next_;
      if(control_.has_sibling()) {
        to_sibling = true;
      }
    }

    return pcmp;
  }

  int index_phase1(String& key, void*& next, int& index, bool& to_sibling) {
    int pcmp = prefix_compare(key);

    if(pcmp < 0) {
      // key is less than node prefix
      index = 0;
    } else if(pcmp > 0) {
      /* key is greater than prefix, meanwhile
       * current node is the right most node */
      if(!control_.has_sibling()) index = knum_;
      else next = next_, to_sibling = true;
    }

    return pcmp;
  }

  int suffix_bs(String& key, int cmps, int lid, int hid) {
    // suffix binary search, cmps: compared size
    assert(key.len >= cmps && hid > lid);
    char* kstr = key.str + cmps, * sep;
    int ks = key.len - cmps, seps;
    int mid, cmp;

    while(lid < hid) {
      mid = (lid + hid) / 2;
      sep = anchors_[mid]->str + cmps;
      seps = anchors_[mid]->len - cmps;

      if(seps < 0) return mid;
      // current node has been modified, retry

      cmp = compare(kstr, ks, sep, seps);
      if(cmp < 0) { hid = mid; }
      else if(cmp == 0) return mid;
      else { lid = mid + 1; }
    }

    assert(lid == hid);
    return hid;
  }

  void memmove64(void* src, void* dst, int n, bool forward) {
    CONDITION_ERROR(n < 0 || n > kNodeSize, "memmove64 error");
    assert(aligned(src, 8) && aligned(dst, 8));
    if(forward) {
      for(int idx = 0; idx < n; idx++)
        ((uint64_t*) dst)[idx] = ((uint64_t*) src)[idx];
    } else {
      for(int idx = n - 1; idx > -1; idx--)
        ((uint64_t*) dst)[idx] = ((uint64_t*) src)[idx];
    }
  }

  /* adjust the size and arrangement of extent, required memory length */
  void extent_resize(Epoch* epoch, int rlen) {
    if(extent_->left() < rlen) {
      int size = extent_->used() + rlen;
      size = roundup(size, kExtentSize);
      Extent* ext = (Extent*) malloc(size);
      ext->init(size); // copy anchors to ext
      for(int kid = 0; kid < knum_; kid++) {
        anchors_[kid] = ext->make_anchor(anchors_[kid]);
        assert(anchors_[kid] != nullptr);
      }
      if(knum_ > 0) ext->huge(anchors_[0]);
      epoch->retire(extent_);
      extent_ = ext;
    }
  }

  /* allocate a memory block from extent to construct an anchor,
   * if the extent is not enough to allocate, reallocate an extent,
   * copy anchors to the new extent and then construct the anchor */
  String* make_anchor(Epoch* epoch, String* key) {
    String* ret = extent_->make_anchor(key);
    if(ret == nullptr) { // no more space
      extent_resize(epoch, key->len + sizeof(String));
      ret = extent_->make_anchor(key);
    }
    assert(ret != nullptr);
    return ret;
  }

  /* ruin anchor without resizing extent (resized/released by insert/merge)*/
  void ruin_anchor(Epoch* epoch, String* key) {
    extent_->ruin_anchor(key);
  }

  void content_rebuild() {
    /* all keys are sorted, the node prefix is equal to the longest
     * common prefix of the first and the last key; new node: prefix
     * init, index == 0; normal node (whose next_ pointer points to
     * sibling): prefix adjust, index == 0 (index == knum - 1 can never
     * happen); right most node: prefix adjust index == 0 | knum - 1 */
    int fs = anchors_[0]->len, ls = anchors_[knum_ - 1]->len;
    char* fk = anchors_[0]->str, * lk = anchors_[knum_ - 1]->str;
    plen_ = common_prefix(fk, fs, lk, ls);
    if(!kExtentOpt) huge_ = anchors_[0];
    else extent_->huge(anchors_[0]);
    if(plen_ <= kEmbedPrSize) { memcpy(tiny_, fk, plen_); }

    for(int kid = 0; kid < knum_; kid++) {
      for(int fid = 0; fid < kFeatureSize; fid++) {
        char ft = (plen_ + fid) < anchors_[kid]->len ?
                  anchors_[kid]->str[plen_ + fid] : 0;
        features_[fid][kid] = ft + 128;// byte encoding conversion
      }
    }
  }

  void* split(String*& key, void* lchild, void* rchild, int index, Epoch* epoch) {
    void* src, * dst;
    InnerNode* rnode = (InnerNode*) malloc(sizeof(InnerNode));
    new(rnode) InnerNode();
    rnode->next_ = next_, next_ = rnode;
    /* set corresponding variables before setting flag */
    if(!control_.has_sibling()) control_.set_sibling();
    else rnode->control_.set_sibling();

    if(index == kNodeSize) { // index == kNodeSize can only exist in the rightmost node
      // the rightmost node without sibling and key is greater than all keys
      if(kExtentOpt) key = rnode->make_anchor(epoch, key);
      rnode->anchors_[0] = key;
      rnode->children_[0] = lchild;
      rnode->next_ = rchild;
      rnode->knum_ = 1;

      rnode->content_rebuild();
      key = anchors_[kNodeSize - 1];
    } else if(index < kNodeSize / 2) {
      // move right half separators and children to right node
      if(kExtentOpt) {
        // allocate a large enough memory block to prevent resize
        rnode->extent_resize(epoch, extent_->used());
        for(int kid = kNodeSize / 2; kid < kNodeSize; kid++) {
          String* k = rnode->make_anchor(epoch, anchors_[kid]);
          ruin_anchor(epoch, anchors_[kid]);
          rnode->anchors_[kid - kNodeSize / 2] = k;
        }
      } else {
        src = anchors_ + kNodeSize / 2, dst = rnode->anchors_;
        memmove64(src, dst, kNodeSize / 2, true);
      }
      src = children_ + kNodeSize / 2, dst = rnode->children_;
      memmove64(src, dst, kNodeSize / 2, true);

      //insert lhigh to left inner node
      if(kExtentOpt) { knum_ = kNodeSize / 2, key = make_anchor(epoch, key); }
      src = anchors_ + index, dst = anchors_ + index + 1;
      memmove64(src, dst, kNodeSize / 2 - index, false);
      src = children_ + index, dst = children_ + index + 1;
      memmove64(src, dst, kNodeSize / 2 - index, false);
      anchors_[index] = key;
      children_[index + 1] = rchild;

      knum_ = kNodeSize / 2 + 1;
      rnode->knum_ = kNodeSize / 2;

      content_rebuild();
      rnode->content_rebuild();

      key = anchors_[kNodeSize / 2];
    } else { // kNodeSize / 2 <= index < kNodeSize
      int ncp = index - kNodeSize / 2;
      if(kExtentOpt) {
        for(int kid = kNodeSize / 2; kid < kNodeSize; kid++) {
          if(kid == index) {
            key = rnode->make_anchor(epoch, key);
            rnode->anchors_[index - kNodeSize / 2] = key;
            rnode->knum_ += 1;
          }
          String* k = rnode->make_anchor(epoch, anchors_[kid]);
          ruin_anchor(epoch, anchors_[kid]);
          if(kid < index) rnode->anchors_[kid - kNodeSize / 2] = k;
          else rnode->anchors_[kid - kNodeSize / 2 + 1] = k;
          rnode->knum_ += 1;
        }
      } else {
        src = anchors_ + kNodeSize / 2, dst = rnode->anchors_;
        memmove64(src, dst, ncp, true);
        src = anchors_ + index, dst = rnode->anchors_ + ncp + 1;
        memmove64(src, dst, kNodeSize - index, true);
        rnode->anchors_[index - kNodeSize / 2] = key;
      }

      src = children_ + kNodeSize / 2, dst = rnode->children_;
      memmove64(src, dst, ncp + 1, true);
      src = children_ + index + 1, dst = rnode->children_ + ncp + 2;
      memmove64(src, dst, kNodeSize - index - 1, true);
      rnode->children_[index - kNodeSize / 2 + 1] = rchild;

      knum_ = kNodeSize / 2;
      rnode->knum_ = kNodeSize / 2 + 1;

      content_rebuild();
      rnode->content_rebuild();

      key = anchors_[kNodeSize / 2 - 1];
    }

    return rnode;
  }

  void* merge(String*& key, Epoch* epoch) {
    // only merge with the right sibling node
    void* merged = nullptr;
    if(control_.has_sibling()) {
      InnerNode* rnode = (InnerNode*) next_;
      int rnkey = rnode->knum_;
      CONDITION_ERROR(knum_ < 1, "knum equals 1 at least");
      if(knum_ + rnkey <= kMergeSize || rnkey == 0) { // try to merge
        rnode->control_.latch_exclusive();
        rnkey = rnode->knum_;
        // if rnkey = 0 (rightmost inner), merge immediately
        // ensure need to merge with right node
        if(knum_ + rnkey <= kMergeSize || rnkey == 0) {
          merged = rnode, key = anchors_[knum_ - 1];

          // move separators in rnode to current node
          if(kExtentOpt) {
            extent_resize(epoch, rnode->extent_->used());
            for(int kid = 0; kid < rnkey; kid++) {
              anchors_[knum_++] = make_anchor(epoch, rnode->anchors_[kid]);
              rnode->ruin_anchor(epoch, rnode->anchors_[kid]);
            }
            void* src = rnode->children_;
            void* dst = children_ + knum_ - rnkey;
            memmove64(src, dst, rnkey, true);
            rnode->knum_ = 0, epoch->retire(rnode->extent_);
          } else {
            void* src = rnode->anchors_;
            void* dst = anchors_ + knum_;
            memmove64(src, dst, rnkey, true);
            src = rnode->children_;
            dst = children_ + knum_;
            memmove64(src, dst, rnkey, true);
            knum_ += rnkey, rnode->knum_ = 0;
          }

          content_rebuild();
          next_ = rnode->next_, rnode->next_ = this;
          if(!rnode->control_.has_sibling()) {
            control_.clear_sibling();
          }
          rnode->control_.set_delete();
          rnode->control_.update_version();
        }
        rnode->control_.unlatch_exclusive();
      }
    }

    return merged;
  }

  void* bound_remove(String*& key, bool& up, int index, Epoch* epoch) {
    CONDITION_ERROR(up != false, "up is uninitialized");
    void* merged = nullptr;
    if(!control_.has_sibling()) {
      // current node is the right-most node
      next_ = children_[index];
      knum_ -= 1;
      // if no anchors in current node(root, right-most node), set plen
      // to zero, guaranteeing lookup operation can be performed correctly
      if(knum_ == 0) plen_ = 0;
      else content_rebuild();
    } else {
      InnerNode* rnode = (InnerNode*) next_;
      rnode->control_.latch_exclusive();
      rnode->control_.update_version();
      int rnkey = rnode->knum_;

      // if current has no key after border remove, or if sibling has no key (rightmost
      // node) or has only one key, or if the number of keys in current and sibling after
      // border remove is less than or equal to kMergeSize, merge the two node
      if(index + rnkey <= kMergeSize || index == 0
         || rnkey == 0 || rnkey == 1) {
        merged = rnode; // border remove, so do not need to reset mid.
        // move right node key to current node
        if(kExtentOpt) {
          knum_ -= 1, extent_resize(epoch, rnode->extent_->used());
          for(int kid = 0; kid < rnkey; kid++) {
            anchors_[knum_++] = make_anchor(epoch, rnode->anchors_[kid]);
            rnode->ruin_anchor(epoch, rnode->anchors_[kid]);
          }
          void* src = rnode->children_ + 1;
          void* dst = children_ + knum_ - rnkey + 1;
          memmove64(src, dst, rnkey ? rnkey - 1 : 0, true);
          rnode->knum_ = 0, epoch->retire(rnode->extent_);
        } else {
          void* src = rnode->anchors_;
          void* dst = anchors_ + index;
          memmove64(src, dst, rnkey, true);
          src = rnode->children_ + 1;
          dst = children_ + knum_;
          memmove64(src, dst, rnkey ? rnkey - 1 : 0, true);
          knum_ += rnkey - 1, rnode->knum_ = 0;
        }
        content_rebuild();

        // set meta information
        if(rnkey != 0) next_ = rnode->next_;
        else next_ = children_[index]; // rightmost node has no key
        rnode->next_ = this;
        if(!rnode->control_.has_sibling())
          control_.clear_sibling();
        rnode->control_.set_delete();
      } else {
        // to ensure other threads execute correctly when border remove happens
        // move the last one child (the child after merge) to sibling
        up = true, key = anchors_[index - 1];
        rnode->children_[0] = children_[index];
        knum_ -= 1;
        content_rebuild();
      }

      rnode->control_.unlatch_exclusive();
    }

    return merged;
  }

 public:
  InnerNode() : control_(false), knum_(0), plen_(0), next_(nullptr) {
    if(kExtentOpt) {
      extent_ = (Extent*) malloc(Config::kExtentSize);
      extent_->init(Config::kExtentSize);
    }
  }

  ~InnerNode() { if(kExtentOpt) free(extent_); }

  void* sibling() {
    if(control_.has_sibling()) { return next_; }
    return nullptr;
  }

  void statistic(std::map<std::string, double>& stat) {
    stat["index size"] += sizeof(InnerNode);
    if(kExtentOpt) stat["index size"] += extent_->size();
    stat["inner num"] += 1;
  }

  func_used void exhibit() {
    std::vector<std::string> keys;
    for(int kid = 0; kid < knum_; kid++) {
      String& key = *anchors_[kid];
      keys.push_back(std::string(key.str, key.len));
    }
    std::cout << "inner node " << this << ", prefix len: " << plen_
              << ", prefix: " << keys[0].substr(0, plen_) << std::endl;

    for(int kid = 0; kid < knum_; kid++) {
      std::cout << "  " << kid << ": " << GRAPH_FONT_RED << keys[kid].substr(0, plen_)
                << GRAPH_FONT_YELLOW << keys[kid].substr(plen_) << GRAPH_ATTR_NONE << std::endl;
    }
  }

  bool to_next(String& key, void*& next, uint64_t& version) {
    // if next points to a child, return false, else next points to sibling, return true
    bool to_sibling;

    while(true) { // make sure the procedure of to_next is atomic
      to_sibling = false; // make sure to_sibling is initialized with false even retry
      version = control_.begin_read();

      if(control_.deleted()) { // current node has been deleted, jump to its left node
        to_sibling = true, next = next_;
        CONDITION_ERROR(next == nullptr, "next can't be null");
        break;
      }

      // phase 1: prefix comparison
      int pcmp = to_next_phase1(key, next, to_sibling);

      if(!pcmp) { // prefix of key is equal to node prefix
        int idx, rid, plen = plen_; // rid: row index, from higher byte to lower byte
        if(key.len < plen) continue; // current node has modified by other threads

        uint64_t mask, eqmask = bitmap();
        int cmps = std::min(kFeatureSize, key.len - plen); // feature compare bound

        for(rid = 0; rid < cmps; rid++) { // equal comparison
          mask = compare_equal(features_[rid], key.str[plen + rid] + 128);
          mask = mask & eqmask;
          if(mask == 0) break;
          eqmask = mask;
        }

        if(rid < cmps) { // less comparison
          mask = compare_less(features_[rid], key.str[plen + rid] + 128);
          mask = mask & eqmask;

          // less than features corresponding to eqmask
          if(mask == 0) {
            if(eqmask == 0) { idx = 0; }// the right most node, all separators
              // have been deleted, but hasn't been merged to its left sibling
            else { idx = index_least1(eqmask); }
          } else { idx = kBitCnt - countl_zero(mask); }
        } else {
          // can't determine jump to which child just using features
          assert(eqmask != 0);
          int hid = kBitCnt - countl_zero(eqmask);
          int lid = index_least1(eqmask);
          idx = suffix_bs(key, plen + cmps, lid, hid);
        }

        if(idx == knum_) {
          next = next_; // key is greater than all separators
          if(control_.has_sibling()) to_sibling = true;
          assert(next != nullptr);
        } else {
          next = children_[idx];
          assert(next != nullptr);
        }
      }

      if(control_.end_read(version)) break;
    }

    return to_sibling;
  }

  bool index_or_sibling(String& key, void*& next, int& index) {
    // move to sibling, return false; index key, return true;
    bool to_sibling = false;

    if(control_.deleted()) { // current node has been deleted, jump to its left node
      next = next_;
      CONDITION_ERROR(next == nullptr, "next can't be null");
      return true;
    }

    // phase 1: prefix comparison
    int pcmp = index_phase1(key, next, index, to_sibling);

    if(!pcmp) { // prefix of key is equal to node prefix
      int rid, cmps, plen = plen_;
      uint64_t mask, eqmask = bitmap();
      cmps = std::min(kFeatureSize, key.len - plen_);
      CONDITION_ERROR(key.len - plen_ < 0, "unknown error!");

      for(rid = 0; rid < cmps; rid++) {
        mask = compare_equal(features_[rid], key.str[plen + rid] + 128);
        mask = mask & eqmask;
        if(mask == 0) break;
        eqmask = mask;
      }

      if(rid < cmps) { // less comparison
        mask = compare_less(features_[rid], key.str[plen + rid] + 128);
        mask = mask & eqmask;

        // less than features corresponding to eqmask
        if(mask == 0) {
          if(eqmask == 0) { index = 0; }//new node,or the right most node, all separators
            //have been deleted, but hasn't been merged to its left sibling
          else { index = index_least1(eqmask); }
        } else { index = kBitCnt - countl_zero(mask); }
      } else {
        // can't determine jump to which child just using features
        assert(eqmask != 0);
        int hid = kBitCnt - countl_zero(eqmask);
        int lid = index_least1(eqmask);
        index = suffix_bs(key, plen + cmps, lid, hid);
      }

      if(index == knum_ && control_.has_sibling()) {
        // greater than all keys, meanwhile current node is not the rightmost node
        next = next_, to_sibling = true;
        assert(next != nullptr);
      }
    }

    return to_sibling;
  }

  void* insert(String*& key, void* lchild, void* rchild, int index, Epoch* epoch) {
    CONDITION_ERROR(key == nullptr, "key can't be nullptr");
    CONDITION_ERROR(lchild == nullptr, "lchild can't be nullptr");
    CONDITION_ERROR(rchild == nullptr, "rchild can't be nullptr");
    CONDITION_ERROR(index < 0 || index > knum_, "invalid index");

    control_.update_version();

    if(knum_ < kNodeSize) {  // safe, insert key into current node
      if(kExtentOpt) key = make_anchor(epoch, key);
      void* src = anchors_ + index;
      void* dst = anchors_ + index + 1;
      memmove64(src, dst, knum_ - index, false);
      anchors_[index] = key;

      if(index != knum_) { // new node or rightmost node
        src = children_ + index + 1;
        dst = children_ + index + 2;
        memmove64(src, dst, knum_ - index - 1, false);
        children_[index + 1] = rchild;
      } else { children_[index] = lchild, next_ = rchild; }

      knum_ += 1;
      if(index == 0 || index == knum_ - 1) {
        content_rebuild();
      } else {
        for(int rid = 0; rid < kFeatureSize; rid++) {
          src = features_[rid] + index;
          dst = features_[rid] + index + 1;
          memmove(dst, src, knum_ - index - 1);
          char ft = (plen_ + rid) < key->len ?
                    key->str[plen_ + rid] : 0;
          features_[rid][index] = ft + 128; // byte encoding conversion
        }
      }

      return nullptr;
    } else { // unsafe, split the node
      return split(key, lchild, rchild, index, epoch);
    }
  }

  void* remove(String*& key, bool& up, int index, Epoch* epoch) {
    CONDITION_ERROR(key == nullptr, "key can't be null");
    CONDITION_ERROR(index < 0 || index >= knum_, "invalid index");
    control_.update_version(), up = false;

    if(kExtentOpt) ruin_anchor(epoch, anchors_[index]);
    if(index < knum_ - 1) {
      // knum is 2 at least, the merged node is in current node
      CONDITION_ERROR(knum_ < 2, "knum equals 2 at least");
      void* src = anchors_ + index + 1;
      void* dst = anchors_ + index;
      memmove64(src, dst, knum_ - index - 1, true);
      if(index != 0) { // normal remove without the need to re-extract prefix
        for(int rid = 0; rid < kFeatureSize; rid++) {
          src = features_[rid] + index + 1;
          dst = features_[rid] + index;
          memmove(dst, src, knum_ - index - 1);
        }
      }

      src = children_ + index + 2;
      dst = children_ + index + 1;
      memmove64(src, dst, knum_ - index - 2, true);

      knum_ -= 1; // knum >= 1
      if(index == 0) content_rebuild();

      return merge(key, epoch);
    }

    // index == knum -1
    CONDITION_ERROR(index != knum_ - 1, "remove error");
    return bound_remove(key, up, index, epoch);
  }

  bool anchor_update(String* key, int index, Epoch* epoch) {
    CONDITION_ERROR(index < 0 || index >= knum_, "anchor update error");
    control_.update_version();

    if(kExtentOpt) {
      key = make_anchor(epoch, key);
      ruin_anchor(epoch, anchors_[index]);
    }
    anchors_[index] = key;
    if(index == 0 || index == knum_ - 1) {
      content_rebuild();
    } else {
      for(int rid = 0; rid < kFeatureSize; rid++) {
        char ft = (plen_ + rid) < key->len ?
                  key->str[plen_ + rid] : 0;
        features_[rid][index] = ft + 128;
      }
    }

    return control_.has_sibling() && (knum_ - 1) == index;
  }

  void* root_remove(Epoch* epoch) {
    if(knum_ == 0) {
      if(kExtentOpt) epoch->retire(extent_);
      control_.set_delete();
      return next_; // the new root
    }
    return nullptr;
  }
};

}

#endif //INDEXRESEARCH_INODE_H
