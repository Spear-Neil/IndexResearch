#include <iostream>
#include <vector>
#include "index.h"

int main() {
  auto index = IndexFactory<std::string, uint64_t>::get_index(WORMHOLE);

  std::vector<uint64_t> data;
  size_t nkey = 200;

  for(uint64_t i = 0; i < nkey; i++) {
    std::string key = std::to_string(i);
    index->insert(key, i);
  }

  for(uint64_t i = 0; i < nkey; i++) {
    std::string key = std::to_string(i);
    index->update(key, i + 2);
  }

  for(uint64_t i = 0; i < nkey; i++) {
    std::string key = std::to_string(i);
    uint64_t value;
    bool find = index->lookup(key, value);
    if(!find || value != i + 2) {
      std::cout << "not found: " << i << std::endl;
      exit(1);
    }
  }

  for(uint64_t i = 0; i < nkey; i++) {
    std::string key = std::to_string(i);
    int count = index->scan(key, 10);
    if(count < 0) exit(2);
  }

  for(uint64_t i = 0; i < nkey; i++) {
    std::string key = std::to_string(i);
    index->remove(key);
  }
}
