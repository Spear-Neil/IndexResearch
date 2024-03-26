#include <iostream>
#include <vector>
#include <set>
#include <algorithm>
#include "FBTree/fbtree.h"
#include "tlx/container.hpp"
#include "GoogleBTree/btree_map.h"
#include "util.h"

using namespace util;

void fb_driver(std::vector<uint64_t>& keys, bool wi_query) {
  using namespace FeatureBTree;
  FBTree<uint64_t, uint64_t> tree;
  Timer<> timer;
  double itpt = 0, qtpt = 0;

  tree.get_epoch().startop();
  timer.start();
  for(auto k : keys) {
    tree.upsert(k, k);
  }
  long drt = timer.duration_us();
  itpt = double(keys.size()) / drt;

  if(wi_query) {
    timer.start();
    for(auto k : keys) {
      auto pair = tree.lookup(k);
      if(pair == nullptr) {
        std::cout << k << " not found" << std::endl;
        exit(-1);
      }
    }
    drt = timer.duration_us();
    qtpt = double(keys.size()) / drt;
  }
  tree.get_epoch().endop();

  std::cout << "-- insert tpt: " << itpt << std::endl;
  std::cout << "-- query tpt: " << qtpt << std::endl;
}

void stx_driver(std::vector<uint64_t>& keys, bool wi_query) {
  tlx::btree_map<uint64_t, uint64_t> tree;
  Timer<> timer;
  double itpt = 0, qtpt = 0;

  timer.start();
  for(auto k : keys) {
    tree.insert(std::make_pair(k, k));
  }
  long drt = timer.duration_us();
  itpt = double(keys.size()) / drt;

  if(wi_query) {
    timer.start();
    for(auto k : keys) {
      auto it = tree.find(k);
      if(it == tree.end()) {
        std::cout << k << " not found" << std::endl;
        exit(-1);
      }
    }
    drt = timer.duration_us();
    qtpt = double(keys.size()) / drt;
  }

  std::cout << "-- insert tpt: " << itpt << std::endl;
  std::cout << "-- query tpt: " << qtpt << std::endl;
}

int main(int argc, char* argv[]) {
  if(argc < 6) {
    std::cout << "-- nkey, random key, shuffle, tree_type, wi_query" << std::endl;
    exit(-1);
  }

  size_t nkey = std::stoul(argv[1]);
  int random = std::stoi(argv[2]);
  int shuffle = std::stoi(argv[3]);
  int tree_type = std::stoi(argv[4]);
  int wi_query = std::stoi(argv[5]);

  std::cout << "-- nkey: " << nkey << ", random key: " << random << ", shuffle: " << shuffle << std::endl;

  std::vector<uint64_t> keys;
  keys.reserve(nkey);

  std::cout << "-- key generation ... " << std::flush;
  if(random) {
    std::set<uint64_t> uniq;
    size_t i = 0;
    while(uniq.size() < nkey) {
      uniq.insert(hash(i++));
    }
    for(auto k : uniq) {
      keys.push_back(k);
    }
  } else {
    for(size_t i = 0; i < nkey; i++) {
      keys.push_back(i);
    }
  }
  std::cout << "end" << std::endl;

  if(shuffle) {
    std::cout << "-- shuffle ... " << std::flush;
    std::random_shuffle(keys.begin(), keys.end());
    std::cout << "end" << std::endl;
  }

  if(tree_type == 0) {
    std::cout << "-- tree_type: FBTree, with query: " << wi_query << std::endl;
    fb_driver(keys, wi_query);
  } else if(tree_type == 1) {
    std::cout << "-- tree_type: StxBTre, with query: " << wi_query << std::endl;
    stx_driver(keys, wi_query);
  }  else {
    std::cout << "-- error tree type" << std::endl;
  }

  return 0;
}
