/*
 * Copyright (c) 2022, Chen Yuan <yuan.chen@whu.edu.cn>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#ifndef B_LINK_TREE_B_LINK_NODE_H
#define B_LINK_TREE_B_LINK_NODE_H

#include <iostream>
#include <string.h>

#include "latch.h"

template<typename key_t, typename value_t, typename latch_t>
class BLinkNode {
 private:
  bool is_leaf_;  // true means this node is a leaf node, otherwise it's a inner node
  int node_size_;  // the maximum number of keys in node
  int item_cnt_;  // the number of keys in node
  BLinkNode* sibling_ptr_;  /// sibling pointer, significant, take care
  BLinkNode* upper_level_;  /// point to the leftmost node on the upper level, significant, take care

  key_t* keys_;   // key array
  union {
    value_t* values_;  // value array
    BLinkNode** children_;  // child node pointer
  };

  latch_t latch_;

 public:
  BLinkNode(int node_size, bool is_leaf) :
    node_size_(node_size), is_leaf_(is_leaf), item_cnt_(0),
    sibling_ptr_(nullptr), upper_level_(nullptr) {
    keys_ = new key_t[node_size_]{};
    if(is_leaf_)
      values_ = new value_t[node_size_]{};
    else
      children_ = new BLinkNode* [node_size_ + 1]{};
  }

  BLinkNode(const BLinkNode& other) {
    is_leaf_ = other.is_leaf_;
    node_size_ = other.node_size_;
    item_cnt_ = other.item_cnt_;
    sibling_ptr_ = other.sibling_ptr_;
    upper_level_ = other.upper_level_;

    keys_ = new key_t[node_size_];
    for(int i = 0; i < item_cnt_; i++)
      keys_[i] = other.keys_[i];

    if(is_leaf_) {
      values_ = new value_t[node_size_];
      for(int i = 0; i < item_cnt_; i++)
        values_[i] = other.values_[i];
    } else {
      children_ = new BLinkNode* [node_size_ + 1];
      for(int i = 0; i < item_cnt_ + 1; i++)
        children_[i] = other.children_[i];
    }
    *latch_ = other.latch_;
  }

  ~BLinkNode() {
    delete[] keys_;
    if(is_leaf_)  // a leaf node
      delete[] values_;
    else {  // inner node
      delete[] children_;
    }
  }

  BLinkNode& operator=(const BLinkNode& other) {
    assert(is_leaf_ == other.is_leaf_);

    node_size_ = other.node_size_;
    item_cnt_ = other.item_cnt_;
    sibling_ptr_ = other.sibling_ptr_;
    upper_level_ = other.upper_level_;
    for(int i = 0; i < item_cnt_; i++)
      keys_[i] = other.keys_[i];

    if(is_leaf_)
      for(int i = 0; i < item_cnt_; i++)
        values_[i] = other.values_[i];
    else
      for(int i = 0; i < item_cnt_ + 1; i++)
        children_[i] = other.children_[i];
    *latch_ = other.latch_;

    return *this;
  }

  uint64_t load_version() {
    return latch_.load_version();
  }

  void latch_shared() {
    latch_.latch_shared();
  }

  void unlatch_shared() {
    latch_.unlatch_shared();
  }

  void latch_exclusive() {
    latch_.latch_exclusive();
  }

  void unlatch_exclusive() {
    latch_.unlatch_exclusive();
  }

  bool is_leaf() {
    return is_leaf_;
  }

  int size() { return item_cnt_; }

  BLinkNode* sibling() { return sibling_ptr_; }

  // return reference of the leftmost node on the upper level
  BLinkNode*& upper_level() { return upper_level_; }

  key_t high_key() {
    return keys_[item_cnt_ - 1];
  }

  key_t key(int index) {
    assert(index >= 0 && index < item_cnt_);
    return keys_[index];
  }

  value_t& value(int index) {
    assert(index >= 0 && index < item_cnt_);
    return values_[index];
  }

  BLinkNode* child(int index) {
    assert(!is_leaf_);
    // to inner node but not the rightmost inner node, index must be less than item_cnt_
    assert(index >= 0 && (index < item_cnt_ || (index == item_cnt_ && sibling_ptr_ == nullptr)));
    return children_[index];
  }


  // used by inner node scan and leaf node scan,
  // to inner node, return the index of child which the key may be in or inserted to;
  // to leaf node, return the index of key if key is in the node,
  // else return the index where the key should be inserted
  int NodeScan(const key_t& key) {
    int mid, low = 0, high = item_cnt_ - 1;

    // if key is beyond the upper bound, or there isn't any key in the node
    if(high == -1 || key > keys_[high])
      return high + 1;

    while(low < high) {
      mid = (low + high) / 2;
      // not high = mid - 1, because key may be in the children_[mid] node,
      // or key may be inserted to mid,
      if(key < keys_[mid]) high = mid;
      else if(key == keys_[mid]) return mid;
      else low = mid + 1;
    }

    assert(low == high);
    return high;
  }


  BLinkNode* InnerInsert(const key_t& left_key, const BLinkNode* left_node,
                         const BLinkNode* right_node, double inner_split_threshold) {
    assert(!is_leaf_);
    int index = NodeScan(left_key);
    assert(index >= 0 && (index < item_cnt_ || (index == item_cnt_ && sibling_ptr_ == nullptr)));

    if(double(item_cnt_ + 1) / node_size_ < inner_split_threshold) { //safe
      for(int i = item_cnt_; i > index; i--) {
        keys_[i] = keys_[i - 1];
        children_[i + 1] = children_[i];
        // to the rightmost inner node on the same level, children_[item_cnt] is valid,
        // but to others, children_[item_cnt] is senseless,
      }

      if(item_cnt_ == 0) // new node as root node
        children_[index] = const_cast<BLinkNode*>(left_node);

      keys_[index] = left_key;
      children_[index + 1] = const_cast<BLinkNode*>(right_node);

      item_cnt_++;
      return nullptr;
    } else { // unsafe, split the node
      BLinkNode* new_node = new BLinkNode(node_size_, false);
      new_node->sibling_ptr_ = sibling_ptr_;
      new_node->upper_level_ = upper_level_;

      if(index <= item_cnt_ / 2) {
        int mid = item_cnt_ / 2;
        // move these bigger key-values to its sibling
        for(int i = 0; i < item_cnt_ - mid; i++) {
          new_node->keys_[i] = keys_[mid + i];
          new_node->children_[i] = children_[mid + i];
        }
        new_node->children_[item_cnt_ - mid] = children_[item_cnt_];
        new_node->item_cnt_ = item_cnt_ - mid;
        assert(new_node->item_cnt_ > 0);

        // insert the left key and pointer to the old node
        for(int i = mid; i > index; i--) {
          keys_[i] = keys_[i - 1];
          children_[i + 1] = children_[i];
        }

        keys_[index] = left_key;
        if(index < mid)
          children_[index + 1] = const_cast<BLinkNode*>(right_node);
        else
          new_node->children_[0] = const_cast<BLinkNode*>(right_node);

        item_cnt_ = mid + 1;
        assert(item_cnt_ > 0);
      } else { // index > item_cnt_ / 2
        int i = 0, mid = item_cnt_ / 2 + 1;
        for(; i < index - mid; i++) {
          new_node->keys_[i] = keys_[mid + i];
          new_node->children_[i] = children_[mid + i];
        }
        new_node->children_[i] = children_[mid + i];
        new_node->keys_[i] = left_key;
        i++;
        new_node->children_[i] = const_cast<BLinkNode*>(right_node);

        for(; i < item_cnt_ - mid + 1; i++) {
          new_node->keys_[i] = keys_[mid + i - 1];
          new_node->children_[i + 1] = children_[mid + i];
        }

        new_node->item_cnt_ = item_cnt_ - mid + 1;
        assert(new_node->item_cnt_ > 0);
        item_cnt_ = mid;
        assert(item_cnt_ > 0);
      }

      sibling_ptr_ = new_node;

      return new_node;
    }
  }


  BLinkNode* LeafInsert(const key_t& key, const value_t& value,
                        int index, double leaf_split_threshold) {
    // if the node is safe (do not reach the split limit), insert the key, return nullptr
    // else split the node, and insert the key, return the new node's pointer
    assert(is_leaf_);
    assert(index == 0 || keys_[index - 1] < key);
    if(double(item_cnt_ + 1) / node_size_ < leaf_split_threshold) { // safe
      // move the greater key-values to their next slot
      for(int i = item_cnt_; i > index; i--) {
        keys_[i] = keys_[i - 1];
        values_[i] = values_[i - 1];
      }

      keys_[index] = key;
      values_[index] = value;

      item_cnt_++;
      return nullptr;
    } else {  // unsafe, split the node
      BLinkNode* new_node = new BLinkNode(node_size_, true);
      new_node->sibling_ptr_ = sibling_ptr_;
      new_node->upper_level_ = upper_level_;

      if(index <= item_cnt_ / 2) {
        int mid = item_cnt_ / 2;
        // move these greater key-values to its sibling
        for(int i = 0; i < item_cnt_ - mid; i++) {
          new_node->keys_[i] = keys_[mid + i];
          new_node->values_[i] = values_[mid + i];
        }
        new_node->item_cnt_ = item_cnt_ - mid;
        assert(new_node->item_cnt_ > 0);

        //insert the key-value to the old node
        for(int i = mid; i > index; i--) {
          keys_[i] = keys_[i - 1];
          values_[i] = values_[i - 1];
        }
        keys_[index] = key;
        values_[index] = value;
        item_cnt_ = mid + 1;
        assert(item_cnt_ > 0);
      } else {  // index > mid
        int i = 0, mid = item_cnt_ / 2 + 1;
        for(; i < index - mid; i++) {
          new_node->keys_[i] = keys_[mid + i];
          new_node->values_[i] = values_[mid + i];
        }
        new_node->keys_[i] = key;
        new_node->values_[i] = value;
        i++;
        for(; i < item_cnt_ - mid + 1; i++) {
          new_node->keys_[i] = keys_[mid + i - 1];
          new_node->values_[i] = values_[mid + i - 1];
        }
        new_node->item_cnt_ = item_cnt_ - mid + 1;
        assert(new_node->item_cnt_ > 0);

        item_cnt_ = mid;
        assert(item_cnt_ > 0);
      }

      sibling_ptr_ = new_node;

      return new_node;
    }
  }


  void NodeExhibition() {
    if(is_leaf_) {
      std::cout << "---- leaf node: " << uint64_t(this) << " item cnt: " << item_cnt_ << " sibling: "
                << uint64_t(sibling_ptr_) << " use_high_key: " << (sibling() != nullptr) << " high key: " << high_key()
                << std::endl << "key-value: ";
      for(int i = 0; i < item_cnt_; i++) {
        std::cout << keys_[i] << "-" << values_[i] << " | ";
      }
      std::cout << std::endl;
    } else {
      std::cout << "---- inner node: " << uint64_t(this) << " item cnt: " << item_cnt_ << " sibling: "
                << uint64_t(sibling_ptr_) << " use_high_key: " << (sibling() != nullptr) << " high key: " << high_key()
                << std::endl << "child-key: ";
      for(int i = 0; i < item_cnt_; i++) {
        std::cout << uint64_t(children_[i]) << "-" << keys_[i] << " | ";
      }
      std::cout << uint64_t(children_[item_cnt_]) << std::endl;
    }
  }
};

#endif //B_LINK_TREE_B_LINK_NODE_H
