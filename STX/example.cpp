#include <iostream>
#include "tlx/tlx/container.hpp"

using Tree_t = tlx::btree_map<std::string, int>;

int main() {
  Tree_t tree;
  int nkey = 1000;
  std::vector<std::string> keys;
  for(int i = 0; i < nkey; i++)
    keys.push_back(std::to_string(i));

  for(auto& key : keys) {
    tree.insert(std::make_pair(key, std::stoi(key)));
  }

  for(auto& key : keys) {
    Tree_t::iterator it = tree.find(key);
    if(it == tree.end())
      std::cout << "error not found: " << key << std::endl;
    else if(it->second != std::stoi(key))
      std::cout << "error error value: " << key << " " << it->second << std::endl;
  }

  return 0;
}