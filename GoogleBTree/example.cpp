#include <iostream>
#include "btree_map.h"

int main() {
  btree::btree_map<std::string, int> tree;

  int nkey = 1000;
  std::vector<std::string> keys;
  for(int i = 0; i < nkey; i++)
    keys.push_back(std::to_string(i));

  for(auto& key : keys) {
    tree.insert(std::make_pair(key, std::stoi(key)));
  }

  for(auto& key : keys) {
    auto it = tree.find(key);
    if(it->first != key)
      std::cout << "error not found: " << key << std::endl;
    else if(it->second != std::stoi(key))
      std::cout << "error error value: " << key << " " << it->second << std::endl;
  }

  return 0;
}