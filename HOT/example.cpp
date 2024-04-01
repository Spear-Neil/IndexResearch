#include <iostream>
#include <vector>
#include <string>
#include "hot.h"

int main() {
  HotTree<std::string, int> tree;
  int nkey = 1000;
  std::vector<std::string> keys;
  for(int i = 0; i < nkey; i++)
    keys.push_back(std::to_string(i));

  for(auto& key : keys) {
    tree.upsert(key, std::stoi(key));
  }

  for(auto& key : keys) {
    int value;
    if(!tree.search(key, value))
      std::cout << "error not found: " << key << std::endl;
    else if(value != std::stoi(key))
      std::cout << "error error value: " << key << " " << value << std::endl;
  }

  return 0;
}