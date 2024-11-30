/*
 * Copyright (c) 2022, Chen Yuan <yuan.chen@whu.edu.cn>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#ifndef B_LINK_TREE_B_TREE_H
#define B_LINK_TREE_B_TREE_H

#include <vector>

template<typename key_t, typename value_t>
class BTree {
 protected:
  int inner_node_size_;   // the maximum number of keys in inner node
  int leaf_node_size_;    // the maximum number of keys in leaf node

  double inner_split_threshold_;
  double inner_merge_threshold_;

  double leaf_split_threshold_;
  double leaf_merge_threshold_;

 public:
  BTree(int inner_node_size = 4096, int leaf_node_size = 4096, double inner_split_threshold = 0.75,
        double inner_merge_threshold = 0.25, double leaf_split_threshold = 0.75, double leaf_merge_threshold = 0.25) :
    inner_node_size_(inner_node_size), leaf_node_size_(leaf_node_size),
    inner_split_threshold_(inner_split_threshold), inner_merge_threshold_(inner_merge_threshold),
    leaf_split_threshold_(leaf_split_threshold), leaf_merge_threshold_(leaf_merge_threshold) {}

  virtual ~BTree() {}

  virtual bool Insert(const key_t& key, const value_t& value) = 0;

  virtual bool Update(const key_t& key, const value_t& value) = 0;

  virtual bool Search(const key_t& key, value_t& value) = 0;

  virtual bool SearchUnsafe(const key_t& key, value_t& value) = 0; // search operation without concurrent protocol

  virtual bool Delete(const key_t& key) = 0;

  virtual bool ScanFixed(const key_t& left_key, const int scan_sz,  // the result includes the left key
                         std::vector<std::pair<key_t, value_t>>& kv_pairs) = 0;

  virtual bool ScanRange(const key_t& left_key, const key_t& right_key,  // [ ),
                         std::vector<std::pair<key_t, value_t>>& kv_pairs) = 0;

  virtual void ScanLeaf(std::vector<std::pair<key_t, value_t>>& kv_pairs) = 0;  // multithreading unsafe

  virtual int GetTreeDepth() = 0;

  virtual void BTreeExhibition() = 0;
};


#endif //B_LINK_TREE_B_TREE_H
