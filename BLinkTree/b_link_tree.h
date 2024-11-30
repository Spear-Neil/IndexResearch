/*
 * Copyright (c) 2022, Chen Yuan <yuan.chen@whu.edu.cn>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#ifndef B_LINK_TREE_B_LINK_TREE_H
#define B_LINK_TREE_B_LINK_TREE_H

#include <vector>
#include <memory>
#include <algorithm>
#include <xmmintrin.h>
#include <immintrin.h>

#include "b_tree.h"
#include "b_link_node.h"


template<typename key_t, typename value_t, typename latch_t = StandardSharedMutex>
class BLinkTree : public BTree<key_t, value_t> {
 private:
  using Node = BLinkNode<key_t, value_t, latch_t>;
  Node* root_;   // root node
  Node* leaves_;  // points to leaf nodes

  int tree_depth_;

 private:
  void TraverseInner(Node*& work, Node*& current, const key_t& key) {
    while(!current->is_leaf()) {
      work = current;

      work->latch_shared();
      if(current->sibling() != nullptr && current->high_key() < key) {
        // if the node is splitting, move to its sibling node
        assert(current->sibling() != nullptr);
        current = current->sibling();
      } else {
        int index = current->NodeScan(key);
        current = current->child(index);
      }
      work->unlatch_shared();
    }
  }

 public:
  BLinkTree() : BTree<key_t, value_t>() {
    root_ = new Node(this->leaf_node_size_, true);
    leaves_ = root_;
    tree_depth_ = 1;
  }

  BLinkTree(int inner_node_size, int leaf_node_size, double inner_split_threshold,
            double inner_merge_threshold, double leaf_split_threshold, double leaf_merge_threshold) :
    BTree<key_t, value_t>(inner_node_size, leaf_node_size, inner_split_threshold, inner_merge_threshold,
                          leaf_split_threshold, leaf_merge_threshold) {
    root_ = new Node(this->leaf_node_size_, true);
    leaves_ = root_;
    tree_depth_ = 1;
  }

  ~BLinkTree() {
    Node* left = root_, * current = nullptr, * next = nullptr;

    while(true) {
      current = left->sibling();
      while(current != nullptr) {  // destruction of B-link-tree's every level
        next = current->sibling();
        delete current;
        current = next;
      }

      current = left;
      if(!current->is_leaf()) { // move to next level
        left = left->child(0);
        delete current;
      } else {
        delete current;
        break;
      }
    }
  }

  bool Insert(const key_t& key, const value_t& value) override {
    std::vector<Node*> path_stack;
    path_stack.reserve(tree_depth_);

    // traverse the b-link-tree to leaf node, save path to path_stack
    Node* work, * current = root_;
    while(!current->is_leaf()) {
      work = current;

      work->latch_shared();
      bool is_sibling = false;
      if(work->sibling() != nullptr && work->high_key() < key) {
        // if the node is splitting, move to its sibling node
        assert(work->sibling() != nullptr);
        current = work->sibling();
        is_sibling = true;
      } else {
        int index = work->NodeScan(key);
        if(index < work->size()      // make sure not to access key[item_cnt_] ( which is invalid )
           && work->key(index) == key) {
          work->unlatch_shared();
          return false;
        }   // if key is already in the node, return false

        current = work->child(index);
      }
      work->unlatch_shared();

      if(!is_sibling)
        path_stack.push_back(work);
    }

    // reach leaf node
    current->latch_exclusive();
    while(current->sibling() != nullptr && current->high_key() < key) {
      // if the node is splitting, move to its sibling node
      work = current->sibling();
      assert(work != nullptr);
      work->latch_exclusive();
      current->unlatch_exclusive();
      current = work;
    }

    // if key is already in the node, return false
    int index = current->NodeScan(key);
    if(index < current->size()      // make sure not to access key[item_cnt_] ( which is invalid )
       && current->key(index) == key) {   // if key is already in the node, return false
      current->unlatch_exclusive();
      return false;
    }

    // insert key to leaf node, return new node or null pointer
    Node* new_node = current->LeafInsert(key, value, index, this->leaf_split_threshold_);

    work = current;
    while(new_node != nullptr) { // split
      key_t right_max_key = new_node->key(new_node->size() - 1);
      key_t left_max_key = current->key(current->size() - 1);

      if(current == root_) {
        // root node need splitting
        work = new Node(this->inner_node_size_, false);
        current->upper_level() = work;
        new_node->upper_level() = work;
        this->tree_depth_++;
      } else if(!path_stack.empty()) {
        work = path_stack.back();
        path_stack.pop_back();
      } else {  // track to the new root
        work = current->upper_level();
        assert(work != nullptr);
      }

      work->latch_exclusive();
      if(current == root_) // set root after it has been latched
        root_ = work;

      while(work->sibling() != nullptr && work->high_key() < right_max_key) {
        // if the node is splitting, move to its sibling node
        Node* sibling_node = work->sibling();
        assert(sibling_node != nullptr);
        sibling_node->latch_exclusive();
        work->unlatch_exclusive();
        work = sibling_node;
      }
      current->unlatch_exclusive();
      // inner node insertion
      new_node = work->InnerInsert(left_max_key, current, new_node, this->inner_split_threshold_);
      current = work;
    }
    work->unlatch_exclusive();

    return true;
  }

  bool Update(const key_t& key, const value_t& value) override {
    Node* work, * current = root_;
    TraverseInner(work, current, key);

    // reach leaf node
    current->latch_exclusive();
    while(current->sibling() != nullptr && current->high_key() < key) {
      // if the node is splitting, move to its sibling node
      work = current->sibling();
      assert(work != nullptr);
      current->unlatch_exclusive();

      current = work;
      current->latch_exclusive();
    }

    int index = current->NodeScan(key);
    if(index == current->size() ||  // the rightmost leaf node, upper bound
       current->key(index) != key) {     // didn't find the key
      current->unlatch_exclusive();
      return false;
    }

    assert(current->key(index) == key);
    current->value(index) = value;
    current->unlatch_exclusive();
    return true;
  }

  bool Search(const key_t& key, value_t& value) override {    // traverse the b-link-tree to leaf node
    Node* work, * current = root_;
    TraverseInner(work, current, key);

    // reach leaf node
    current->latch_shared();
    while(current->sibling() != nullptr && current->high_key() < key) {
      // if the node is splitting, move to its sibling node
      work = current->sibling();
      assert(work != nullptr);
      current->unlatch_shared();

      current = work;
      current->latch_shared();
    }

    int index = current->NodeScan(key);
    if(index == current->size() ||  // the rightmost leaf node, upper bound
       current->key(index) != key) {     // didn't find the key
      current->unlatch_shared();
      return false;
    }

    assert(current->key(index) == key);
    value = current->value(index);
    current->unlatch_shared();
    return true;
  }

  bool SearchUnsafe(const key_t& key, value_t& value) override {   // used it only when there are not any inserts;
    Node* current = root_;
    while(!current->is_leaf()) {
      if(current->sibling() != nullptr && current->high_key() < key) {
        // if the node is splitting, move to its sibling node
        assert(false);  // never happen
        assert(current->sibling() != nullptr);
        current = current->sibling();
      } else {
        int index = current->NodeScan(key);
        current = current->child(index);
      }
    }

    // reach leaf node
    while(current->sibling() != nullptr && current->high_key() < key) {
      // if the node is splitting, move to its sibling node
      assert(false);  // never happen
      current = current->sibling();
      assert(current != nullptr);
    }

    int index = current->NodeScan(key);
    if(index == current->size() ||  // the rightmost leaf node, upper bound
       current->key(index) != key) {     // didn't find the key
      return false;
    }

    assert(current->key(index) == key);
    value = current->value(index);
    return true;
  }

  bool Delete(const key_t& key) override {  /// todo: delete operation implementation
    return false;
  }

  virtual bool ScanFixed(const key_t& left_key, const int scan_sz,  // the result includes the left key
                         std::vector<std::pair<key_t, value_t>>& kv_pairs) override {
    Node* work, * current = root_;
    TraverseInner(work, current, left_key);

    // reach leaf node
    current->latch_shared();
    while(current->sibling() != nullptr && current->high_key() < left_key) {
      // if the node is splitting, move to its sibling node
      work = current->sibling();
      assert(work != nullptr);
      current->unlatch_shared();

      current = work;
      current->latch_shared();
    }

    std::vector<Node*> scan_stack;
    scan_stack.push_back(current);

    int index = current->NodeScan(left_key);
    if(index == current->size()) { // the rightmost leaf node, upper bound
      current->unlatch_shared();
      return false;  // no keys that are not less than left_key
    }

    // scan the values whose key is not less than left_key
    int scan_index = 0;
    while(scan_index < scan_sz) {
      while(index < current->size()) {
        kv_pairs.push_back(std::pair<key_t, value_t>(
          current->key(index), current->value(index)));
        index++, scan_index++;

        if(scan_index >= scan_sz)
          goto release_latch1;
      }

      current = current->sibling();
      if(current == nullptr)
        goto release_latch1;

      index = 0;
      current->latch_shared();
      scan_stack.push_back(current);
    }

    release_latch1:
    // release shared latch_exclusive
    while(!scan_stack.empty()) {
      scan_stack.back()->unlatch_shared();
      scan_stack.pop_back();
    }

    return true;
  }

  virtual bool ScanRange(const key_t& left_key, const key_t& right_key,  // [ ),
                         std::vector<std::pair<key_t, value_t>>& kv_pairs) override {
    Node* work, * current = root_;
    TraverseInner(work, current, left_key);

    // reach leaf node
    current->latch_shared();
    while(current->sibling() != nullptr && current->high_key() < left_key) {
      // if the node is splitting, move to its sibling node
      work = current->sibling();
      assert(work != nullptr);
      current->unlatch_shared();

      current = work;
      current->latch_shared();
    }

    std::vector<Node*> scan_stack;
    scan_stack.push_back(current);

    int index = current->NodeScan(left_key);
    if(index == current->size()) { // the rightmost leaf node, upper bound
      current->unlatch_shared();
      return false;  // no key that are not less than left_key
    }

    while(true) {
      while(index < current->size()) {
        if(current->key(index) >= right_key)
          goto release_latch2;

        kv_pairs.push_back(std::pair<key_t, value_t>(
          current->key(index), current->value(index)));
        index++;
      }

      current = current->sibling();
      if(current == nullptr)
        goto release_latch2;

      index = 0;
      current->latch_shared();
      scan_stack.push_back(current);
    }

    release_latch2:
    // release shared latch_exclusive
    while(!scan_stack.empty()) {
      scan_stack.back()->unlatch_shared();
      scan_stack.pop_back();
    }

    // scan the values whose key is not less than left_key, but less than right_key
    return true;
  }

  void ScanLeaf(std::vector<std::pair<key_t, value_t>>& kv_pairs) override {
    Node* work = leaves_;
    while(work != nullptr) {
      for(int i = 0; i < work->size(); i++) {
        kv_pairs.push_back(std::pair<key_t, value_t>(work->key(i), work->value(i)));
      }
      work = work->sibling();
    }
  }

  int GetTreeDepth() override {
    return tree_depth_;
  }

  void BTreeExhibition() override {
    Node* level_left = root_;
    int level = 1;
    while(level_left != nullptr) {
      std::cout << "=================================  tree level: " << level
                << "  =================================" << std::endl;
      Node* work = level_left;
      while(work != nullptr) {
        work->NodeExhibition();
        work = work->sibling();
      }

      if(level_left->is_leaf())
        break;
      level_left = level_left->child(0);
      level++;
    }
  }
};


#endif //B_LINK_TREE_B_LINK_TREE_H
