#include <iostream>
#include <vector>
#include "BTreeOLC_child_layout.h"

using Tree_t = btreeolc::BTree<int, int>;

int main() {
  Tree_t tree;
  int nkey = 1000;
  std::vector<int> keys;
  for(int i = 0; i < nkey; i++)
    keys.push_back(i);

  for(auto& key : keys) {
    tree.insert(key, key);
  }

  for(auto& key : keys) {
    int value;
    bool find = tree.lookup(key, value);
    if(!find)
      std::cout << "error not found: " << key << std::endl;
    else if(value != key)
      std::cout << "error error value: " << key << " " << value << std::endl;
  }

  return 0;
}