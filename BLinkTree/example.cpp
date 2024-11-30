#include <iostream>
#include "b_link_tree.h"

int main() {
  BLinkTree<uint64_t, uint64_t> tree;
  int nkey = 10000;
  for(int i = 0; i < nkey; i++) {
    if(!tree.Insert(i, i)) {
      std::cerr << "-- Error: failed to insert " << i << std::endl;
    }
  }

  for(int i = 0; i < nkey; i++) {
    uint64_t value;
    bool find = tree.Search(i, value);
    if(!find || i != value) {
      std::cerr << "-- Error: failed to find " << i << std::endl;
    }
  }

  return 0;
}