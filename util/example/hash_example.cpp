#include <iostream>
#include <map>
#include <fstream>
#include <string>
#include <vector>
#include <set>
#include "hash.h"
#include "timer.h"

typedef size_t (* shash_t)(void*, size_t);

typedef size_t(* ihash_t)(uint64_t);

using namespace util;

using util_hash::standard_hash;
using util_hash::jenkins_hash;
using util_hash::murmur_hash32;
using util_hash::murmur_hash64;
using util_hash::faster_hash;
using util_hash::city_hash;

int main(int argc, char* argv[]) {
  std::cout << "-- hash example" << std::endl;

  size_t sndata = 10000000;
  size_t indata = 10000000;
  const int nhash = 6;

  std::string file = "/home/sn/twitter_load";

  Timer<> timer;
  long duration;

  std::map<int, std::string> hash_name;
  hash_name[0] = "standard_hash";
  hash_name[1] = "jenkins_hash";
  hash_name[2] = "murmur_hash32";
  hash_name[3] = "murmur_hash64";
  hash_name[4] = "faster_hash";
  hash_name[5] = "city_hash";

  shash_t shash[nhash] = {standard_hash, jenkins_hash, murmur_hash32, murmur_hash64, faster_hash, city_hash};
  ihash_t ihash[nhash] = {standard_hash, jenkins_hash, murmur_hash32, murmur_hash64, faster_hash, city_hash};

  std::ifstream fin(file);
  if(!fin.is_open()) {
    std::cout << "file open error" << std::endl;
    exit(-1);
  }
  std::vector<std::string> skeys;
  skeys.reserve(sndata);
  std::cout << "-- string hash throughput test" << std::endl;
  std::cout << "  -- data prepare ..." << std::flush;
  std::string line;
  double avgs = 0;
  while(std::getline(fin, line)) {
    avgs += line.size();
    skeys.push_back(line);

    if(skeys.size() >= sndata)
      break;
  }
  sndata = skeys.size();
  avgs = avgs / sndata;
  std::cout << " end! \n" << "  -- data num: " << sndata << ", avg len: " << avgs << std::endl;

  for(int i = 0; i < nhash; i++) {
    timer.start();
    for(auto& key : skeys) {
      shash[i](key.data(), key.size());
    }
    duration = timer.duration_us();

    std::set<uint64_t> htoken;
    for(auto& key : skeys) {
      htoken.insert(shash[i](key.data(), key.size()));
    }
    std::cout << "  -- " << hash_name[i] << " collision num: " << sndata - htoken.size()
              << " tpt: " << double(sndata) / duration << std::endl;
  }

  std::cout << "-- int hash throughput test" << std::endl;
  std::vector<uint64_t> ikeys;
  ikeys.reserve(indata);
  std::cout << "  -- data prepare ..." << std::flush;
  for(size_t i = 0; i < indata; i++) {
    ikeys.push_back(i);
  }
  std::cout << " end!" << std::endl;

  for(int i = 0; i < nhash; i++) {
    timer.start();
    for(auto key : ikeys) {
      ihash[i](key);
    }
    duration = timer.duration_us();

    std::set<uint64_t> htoken;
    for(auto key : ikeys) {
      htoken.insert(ihash[i](key));
    }
    std::cout << "  -- " << hash_name[i] << " collision num: " << indata - htoken.size()
              << " tpt: " << double(indata) / duration << std::endl;
  }

  return 0;
}