#include <iostream>
#include <vector>
#include <unordered_set>
#include <algorithm>
#include "../FBTree/fbtree.h"
#include "../FAST/fast64.h"
#include "tlx/container.hpp"
#include "util.h"

using namespace util;

class Index {
 public:
  virtual ~Index() = default;

  virtual void bulk_load(std::vector<uint64_t>& keys) = 0;

  virtual bool lookup(uint64_t key, bool real) = 0;
};

class IndexFBTree : public Index {
  FeatureBTree::FBTree<uint64_t, uint64_t> tree;

 public:
  void bulk_load(std::vector<uint64_t>& keys) override {
    for(auto key : keys) tree.upsert(key, key);
  }

  bool lookup(uint64_t key, bool real) override {
    if(!real) return true;
    auto pair = tree.lookup(key);
    if(pair == nullptr || pair->value != key)
      return false;
    return true;
  }
};

class IndexSTX : public Index {
  tlx::btree_map<uint64_t, uint64_t> tree;

 public:
  void bulk_load(std::vector<uint64_t>& keys) override {
    for(auto key : keys) tree.insert({key, key});
  }

  bool lookup(uint64_t key, bool real) override {
    if(!real) return true;
    auto it = tree.find(key);
    if(it == tree.end() || it->second != key)
      return false;
    return true;
  }
};

class IndexFast : public Index {
  Fast64* tree = nullptr;

 public:
  void bulk_load(std::vector<uint64_t>& keys) override {
    assert(tree == nullptr);
    tree = create_fast64(keys.data(), keys.size(), keys.data(), keys.size());
  }

  bool lookup(uint64_t key, bool real) override {
    if(!real) return true;
    assert(tree != nullptr);
    uint64_t out1, out2;
    lookup_fast64(tree, key, &out1, &out2);
    if(key != out1) return false;
    return true;
  }
};


/** Test Procedure for Cache Misses/Branch Misses of B-trees (STX B+-tree, FB+-tree, FAST),
 *  Phase 1: bulk_load a set of ordered uint64 keys (because FAST only supports bulk_load)
 *  Phase 2: perform a set of lookup operations following uniform or zipfian distribution
 *
 *  Due to a little optimization, the lookup performance of FB+-tree in such case (ordered
 *  insertions) may be higher than that when dynamically inserting these keys in random order.
 * */

double run_driver(Index* tree, std::vector<uint64_t>& reqs, bool wi_query) {
  Timer timer;
  timer.start();
  for(auto key : reqs) {
    // passing parameters into virtual function to prevent excessive optimizations
    if(!tree->lookup(key, wi_query)) {
      std::cerr << key << " not found" << std::endl;
      exit(-1);
    }
  }
  long drt = timer.duration_us();
  return double(reqs.size()) / drt;
}

int main(int argc, char* argv[]) {
  if(argc < 6) {
    std::cerr << "-- nkey, key_type (0-dense, 1-sparse), req_type(0-unif, 1-zipf), tree_type, wi_query" << std::endl;
    exit(-1);
  }

  size_t nkey = std::stoul(argv[1]);
  int key_type = std::stoi(argv[2]);
  int req_type = std::stoi(argv[3]);
  int tree_type = std::stoi(argv[4]);
  int wi_query = std::stoi(argv[5]);

  PinningMap pin;
  pin.pinning_thread(0, 0, pthread_self());

  if(key_type != 0 && key_type != 1) {
    std::cerr << "-- no such key type" << std::endl;
    exit(-1);
  }

  if(req_type != 0 && key_type != 1) {
    std::cerr << "-- no such req type" << std::endl;
    exit(-1);
  }

  Index* tree = nullptr;

  std::cout << "-- nkey: " << nkey << ", key_type: " << (key_type ? "sparse" : "dense")
            << ", req_tye: " << (req_type ? "zipf" : "unif") << ", tree_type: ";
  if(tree_type == 0) {
    std::cout << "FBTree" << std::endl;
    tree = new IndexFBTree();
  } else if(tree_type == 1) {
    std::cout << "STX BTree" << std::endl;
    tree = new IndexSTX();
  } else if(tree_type == 2) {
    std::cout << "FAST" << std::endl;
    tree = new IndexFast();
  } else {
    std::cout << std::endl;
    std::cerr << "-- no such tree type" << std::endl;
    exit(-1);
  }

  size_t nreq = nkey;
  double skew = 0.99;

  std::vector<uint64_t> keys, reqs;
  keys.reserve(nkey), reqs.reserve(nreq);

  std::cout << "-- loads generation ... " << std::flush;
  if(key_type) { // sparse keys
    std::unordered_set<uint64_t> uniq;
    uniq.reserve(nkey);
    RandomEngine gen;
    while(uniq.size() < nkey) {
      uniq.insert(gen());
    }
    for(auto k : uniq) {
      keys.push_back(k);
    }
    std::sort(keys.begin(), keys.end());
  } else { // dense keys
    for(size_t i = 0; i < nkey; i++) {
      keys.push_back(i);
    }
  }
  std::cout << "end" << std::endl;

  std::cout << "-- requests generation ... " << std::flush;
  if(req_type) { // zipf distribution
    ZipfGenerator<uint64_t> gen(0, nkey, skew);
    for(size_t i = 0; i < nreq; i++) {
      reqs.push_back(keys[gen()]);
    }
  } else { // unif distribution
    UnifGenerator<uint64_t> gen(0, nkey);
    for(size_t i = 0; i < nreq; i++) {
      reqs.push_back(keys[gen()]);
    }
  }
  std::cout << "end" << std::endl;

  std::cout << "-- bulk load ... " << std::flush;
  tree->bulk_load(keys);
  std::cout << "end" << std::endl;

  double tpt = 0;
  std::cout << "-- perform requests ... " << std::flush;
  tpt = run_driver(tree, reqs, wi_query);
  std::cout << "end, throughput: " << tpt << ", wi_query: " << wi_query << std::endl;

  return 0;
}
