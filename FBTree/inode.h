#ifndef INDEXRESEARCH_INODE_H
#define INDEXRESEARCH_INODE_H

#include <iostream>
#include <cassert>
#include <cstring>
#include <vector>
#include "config.h"
#include "constant.h"
#include "compare.h"
#include "control.h"
#include "type.h"
#include "macro.h"
#include "log.h"

namespace FeatureBTree {

template<typename K>
class alignas(Config::kAlignSize) InnerNode {
  static constexpr int kNodeSize = Constant<K>::kNodeSize;
  static constexpr int kMergeSize = Constant<K>::kMergeSize;
  static constexpr int kFeatureSize = Constant<K>::kFeatureSize;
  static constexpr int kBitCnt = 64; // bits number of bitmap

  Control control_;
  int knum_;       // the number of keys
  int plen_;       // the length of prefix
  char prefix_[8];
  void* next_;     // sibling or last child
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
      if(knum_ == kNodeSize) {
        return 0xFFFF'FFFF'FFFF'FFFFul;
      } else {
        return (0x01ul << knum_) - 1;
      }
    } else {
      return (0x01ul << knum_) - 1;
    }
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
      if(knum_ + rnkey <= kMergeSize) { // try to merge
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

  void border_extension() { // prefix extension
    CONDITION_ERROR(knum_ < 1, "border extension error");
    int pid = 0;
    for(; pid < kFeatureSize - plen_; pid++) {
      if(features_[pid][0] != features_[pid][knum_ - 1]) break;
      prefix_[plen_ + pid] = features_[pid][0];
    }
    if(pid > 0) {
      void* src = features_[pid], * dst = features_[0];
      CONDITION_ERROR(kFeatureSize - plen_ - pid < 0, " border extension error");
      memmove(dst, src, (kFeatureSize - plen_ - pid) * kNodeSize);
      plen_ += pid;
    }
  }

  void* border_remove(K& mid, bool& up, int index) {
    CONDITION_ERROR(up != false, "up is uninitialized");
    void* merged = nullptr;
    if(!control_.has_sibling()) {
      // current node is the right-most node
      next_ = children_[index];
      knum_ -= 1;
      if(knum_ == 0) plen_ = 0;
      else border_extension();
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
        border_extension();
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
            if(eqmask == 0) {
              idx = 0; // the right most node, all separators have been
              // deleted, but hasn't been merged to its left sibling
            } else idx = index_least1(eqmask);
          } else idx = kBitCnt - countl_zero(mask);
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
    return border_remove(mid, up, index);
  }

  bool border_update(K mid, int index) {
    CONDITION_ERROR(index < 0 || index >= knum_, "border update error");
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

}

#endif //INDEXRESEARCH_INODE_H
