#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_set>
#include "util.h"

using namespace util;

constexpr size_t key_space = 1'000'000'000ul;

int main(int argc, char* argv[]) {
  if(argc < 2) {
    std::cerr << "-- ycsb workloads path, [keys path, ycsb keys by default]" << std::endl;
    exit(-1);
  }
  std::string ycsb_path(argv[1]);
  std::string key_path;
  if(argc > 2) key_path = std::string(argv[2]);

  std::vector<std::string> keys;
  std::ifstream ycsb_fin, key_fin;
  ycsb_fin.open(ycsb_path, std::ios::in);
  if(!ycsb_fin.good()) {
    std::cerr << "-- failed to open ycsb workloads file" << std::endl;
    exit(-1);
  }
  if(!key_path.empty()) {
    key_fin.open(key_path, std::ios::in);
    if(!key_fin.good()) {
      std::cerr << "-- failed to open keys file" << std::endl;
      exit(-1);
    }
    keys.reserve(key_space);
    std::string line;
    while(std::getline(key_fin, line)) {
      if(keys.size() >= key_space) break;
      keys.push_back(std::move(line));
    }
  }

  std::unordered_set<std::string> ops{"INSERT", "UPDATE", "READ", "SCAN"};
  std::string request;
  while(std::getline(ycsb_fin, request)) {
    auto&& req_fields = string_split(std::move(request), ' ');
    std::string& type = req_fields[0];
    if(ops.find(type) != ops.end()) {
      std::string& key = req_fields[2];
      if(!key_path.empty()) {
        key = keys[hash(key.data(), key.length()) % keys.size()];
      }
      if(type == "SCAN") {
        std::cout << type << " " << key << " " << req_fields[3] << "\n";
      } else {
        std::cout << type << " " << key << "\n";
      }
    }
  }
  std::cout.flush();

  return 0;
}