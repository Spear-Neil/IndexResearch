#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include "fast64.h"

int main() {
  size_t nkey = 100;
  std::vector<uint64_t> keys, values;

  std::mt19937_64 mt(std::random_device{}());
  for(size_t i = 0; i < nkey; i++) {
    keys.push_back(i), values.push_back(mt());
  }

  Fast64* tree = create_fast64(keys.data(), keys.size(), values.data(), values.size());

  for(size_t i = 0; i < nkey; i++) {
    uint64_t out1, out2;
    lookup_fast64(tree, keys[i], &out1, &out2);
    printf("%i: %i, %i - %i, %i\n", i, keys[i], values[i], out1, out2);
  }

  return 0;
}
