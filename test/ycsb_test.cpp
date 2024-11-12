#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <thread>
#include <unordered_map>
#include <mutex>
#include "index.h"
#include "util.h"

using namespace util;

typedef Index<uint64_t, uint64_t> IntIndex;
typedef Index<String, uint64_t> StrIndex;
enum ReqType { INSERT, UPDATE, READ, SCAN };
constexpr size_t kGigaByte = 1024ul * 1024 * 1024;
constexpr size_t kReserveSize = 100'000'000ul;

std::unordered_map<std::string, ReqType> ops{{"INSERT", INSERT},
                                             {"UPDATE", UPDATE},
                                             {"READ",   READ},
                                             {"SCAN",   SCAN}};

bool skip_insert = false; // ARTOLC may have some bugs in scan

template<typename K>
struct Request {
  typedef typename Index<K, uint64_t>::KVType KVType;

  ReqType type;
  int rng_len;
  KVType* kv;
};

template<typename K>
double load_driver(Index<K, uint64_t>& index, std::vector<Request<K>>& loads, int nthd) {
  PinningMap pin;
  pin.pinning_thread(0, 0, pthread_self());
  double load_tpt = 0;
  std::vector<std::thread> workers;
  std::vector<double> throughput(nthd);
  std::atomic<int> ready{0};

  size_t warm_up_size = loads.size() / 100;
  for(size_t i = 0; i < warm_up_size; i++) {
    assert(loads[i].type == INSERT);
    index.insert(loads[i].kv);
  }

  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pin.pinning_thread_continuous(pthread_self());
      size_t load_size = loads.size() - warm_up_size;
      size_t begin = tid * load_size / nthd + warm_up_size;
      size_t end = (tid + 1) * load_size / nthd + warm_up_size;
      ready.fetch_add(1);
      while(ready.load() != nthd);

      Timer timer;
      timer.start();
      for(size_t i = begin; i < end; i++) {
        assert(loads[i].type == INSERT);
        index.insert(loads[i].kv);
      }
      long drt = timer.duration_us();
      throughput[tid] = double(end - begin) / drt;
    }, tid));
  }

  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    load_tpt += throughput[tid];
  }

  return load_tpt;
}


/** Notes: we disabled all indexes' Epoch Reclaimer in Workload C, since some implementations are semi-finished with almost zero performance overhead */
template<typename K>
double run_driver(Index<K, uint64_t>& index, std::vector<Request<K>>& runs, int nthd, int time) {
  PinningMap pin;
  pin.pinning_thread(0, 0, pthread_self());
  double run_tpt = 0;
  std::vector<std::thread> workers;
  std::vector<double> throughput(nthd);
  std::atomic<int> ready{0};

  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pin.pinning_thread_continuous(pthread_self());
      size_t begin = tid * runs.size() / nthd;
      size_t size = (tid + 1) * runs.size() / nthd - begin;
      ready.fetch_add(1);
      while(ready.load() != nthd);
      uint64_t req_cnt = 0, value = 0;

      Timer timer;
      timer.start();
      while(true) {
        Request<K>& req = runs[req_cnt % size + begin];
        if(req.type == INSERT && !skip_insert) index.insert(req.kv);
        else if(req.type == UPDATE) index.update(req.kv);
        else if(req.type == READ) index.lookup(req.kv->key, value);
        else if(req.type == SCAN) index.scan(req.kv->key, req.rng_len);

        if(req_cnt++ % 100000 == 0 && timer.duration_s() >= time) break;
      }
      long drt = timer.duration_us();
      throughput[tid] = double(req_cnt) / drt;
    }, tid));
  }

  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    run_tpt += throughput[tid];
  }

  return run_tpt;
}


int main(int argc, char* argv[]) {
  if(argc < 6) {
    std::cerr << "-- load workloads path, run workloads path, index type, thread number,"
                 " run time(second), [int key type(0/1), 0 by default]" << std::endl;
    std::cerr << "-- index type: ";
    for(int t = ARTOLC; t <= BLink; t++) {
      std::cerr << t << "-" << IndexFactory<uint64_t, uint64_t>::get_index(INDEX_TYPE(t))->index_type() << ", ";
    }
    std::cerr << std::endl;
    exit(-1);
  }
  std::string load_path(argv[1]);
  std::string run_path(argv[2]);
  int index_type = std::stoi(argv[3]);
  int thread_num = std::stoi(argv[4]);
  int run_time = std::stoi(argv[5]);
  int int_key = 0;
  if(argc > 6) int_key = std::stoi(argv[6]);

  PinningMap pin;
  pin.pinning_thread(0, 0, pthread_self());

  void* tree = nullptr;
  if(int_key) tree = IndexFactory<uint64_t, uint64_t>::get_index((INDEX_TYPE) index_type);
  else tree = IndexFactory<String, uint64_t>::get_index((INDEX_TYPE) index_type);
  if(tree == nullptr) {
    std::cerr << "-- invalid index type" << std::endl;
    exit(-1);
  }
  std::string tree_type = int_key ? ((IntIndex*) tree)->index_type() : ((StrIndex*) tree)->index_type();
  std::cout << "-- index type: " << tree_type << ", thread number: " << thread_num
            << ", run time: " << run_time << ", int key: " << int_key << std::endl;

  std::ifstream fload(load_path), frun(run_path);
  if(!fload.good() || !frun.good()) {
    std::cerr << "-- failed to open load/run workloads" << std::endl;
    exit(-1);
  }

  auto acquire_memory_usage = [index_type]() {
    MemStats stats;
    if(index_type != MASSTREE && index_type != WORMHOLE) {
      return stats.allocated();
    }

    // Masstree and wormhole sometimes allocate memory directly via mmap
    std::ifstream stat_fin("/proc/self/statm");
    if(!stat_fin.good()) {
      std::cerr << "-- failed to open statm" << std::endl;
      exit(-1);
    }
    size_t vps, rps; // virtual page num, resident page num
    stat_fin >> vps >> rps;
    rps *= 4 * 1024;
    return rps;
  };

  double load_tpt = 0, run_tpt = 0, avg_len = 0;
  size_t load_size = 0, run_size = 0;
  size_t load_usage = 0, init_usage = 0, index_usage = 0;
  std::vector<Request<uint64_t>> int_loads, int_runs;
  std::vector<Request<String>> str_loads, str_runs;
  if(int_key) int_loads.reserve(kReserveSize), int_runs.reserve(kReserveSize);
  else str_loads.reserve(kReserveSize), str_runs.reserve(kReserveSize);

  std::vector<std::thread> workers;
  std::unordered_map<ReqType, size_t> req_count;
  std::cout << "-- read load & run workloads ... " << std::flush;
  workers.push_back(std::thread([&]() {
    pin.pinning_thread_continuous(pthread_self());
    std::string raw_req;
    while(std::getline(fload, raw_req)) {
      auto&& req_elem = string_split(std::move(raw_req), ' ');
      std::string& req_type = req_elem[0], & req_key = req_elem[1];
      if(req_key.size() > 255) req_key = req_key.substr(0, 255);
      if(req_type != "INSERT") {
        std::cerr << "-- invalid load workloads" << std::endl;
        exit(-1);
      }
      uint64_t value = hash(req_key.data(), req_key.size());

      if(int_key) {
        using KVType = typename Request<uint64_t>::KVType;
        KVType* kv = (KVType*) malloc(sizeof(KVType));
        load_usage += sizeof(KVType);
        kv->key = value, kv->value = kv->value;
        Request<uint64_t> req{.type=INSERT, .kv=kv};
        int_loads.push_back(req), avg_len += sizeof(uint64_t);
      } else {
        using KVType = typename Request<String>::KVType;
        KVType* kv = (KVType*) malloc(sizeof(KVType) + req_key.size() + 1);
        load_usage += sizeof(KVType) + req_key.size() + 1;
        memcpy(kv->key.str, req_key.data(), req_key.size());
        kv->value = value, kv->key.len = req_key.size(), kv->key.str[req_key.size()] = '\0';
        Request<String> req{.type = INSERT, .kv = kv};
        str_loads.push_back(req), avg_len += req_key.size();
      }
    }
  }));
  workers.push_back(std::thread([&]() {
    pin.pinning_thread_continuous(pthread_self());
    std::string raw_req;
    while(std::getline(frun, raw_req)) {
      auto&& req_elem = string_split(std::move(raw_req), ' ');
      std::string& req_type = req_elem[0], & req_key = req_elem[1];
      if(req_key.size() > 255) req_key = req_key.substr(0, 255);
      uint64_t value = hash(req_key.data(), req_key.size());

      req_count[ops[req_type]]++;
      if(int_key) {
        using KVType = typename Request<uint64_t>::KVType;
        KVType* kv = (KVType*) malloc(sizeof(KVType));
        kv->key = value, kv->value = kv->value;
        Request<uint64_t> req{.type=ops[req_type], .kv=kv};
        if(req.type == SCAN) req.rng_len = std::stoi(req_elem[2]);
        int_runs.push_back(req);
      } else {
        using KVType = typename Request<String>::KVType;
        KVType* kv = (KVType*) malloc(sizeof(KVType) + req_key.size() + 1);
        memcpy(kv->key.str, req_key.data(), req_key.size());
        kv->value = value, kv->key.len = req_key.size(), kv->key.str[req_key.size()] = '\0';
        Request<String> req{.type =ops[req_type], .kv = kv};
        if(req.type == SCAN) req.rng_len = std::stoi(req_elem[2]);
        str_runs.push_back(req);
      }
    }
  }));
  for(auto& worker : workers) worker.join();
  load_size = int_key ? int_loads.size() : str_loads.size();
  avg_len = avg_len / load_size;

  // endless loop in ART, skip insert operation in workload e
  if(req_count[SCAN] > 0 && (index_type == ARTOLC || index_type == ARTOptiQL)) skip_insert = true;
  run_size = int_key ? int_runs.size() : str_runs.size();
  int insert_ratio, update_ratio, read_ratio, scan_ratio;
  insert_ratio = std::round((double) req_count[INSERT] * 100 / run_size);
  update_ratio = std::round((double) req_count[UPDATE] * 100 / run_size);
  read_ratio = std::round((double) req_count[READ] * 100 / run_size);
  scan_ratio = std::round((double) req_count[SCAN] * 100 / run_size);
  std::cout << "end\n-- avg key len: " << avg_len << ", Insert/Update/Read/Scan: " << insert_ratio
            << "/" << update_ratio << "/" << read_ratio << "/" << scan_ratio << std::endl;

  init_usage = acquire_memory_usage();
  std::cout << "-- load phase ... " << std::flush;
  if(int_key) load_tpt = load_driver<uint64_t>(*(IntIndex*) tree, int_loads, thread_num);
  else load_tpt = load_driver<String>(*(StrIndex*) tree, str_loads, thread_num);
  std::cout << "end, throughput: " << load_tpt << std::endl;
  index_usage = acquire_memory_usage();

  std::cout << "-- run phase ... " << std::flush;
  if(int_key) run_tpt = run_driver<uint64_t>(*(IntIndex*) tree, int_runs, thread_num, run_time);
  else run_tpt = run_driver<String>(*(StrIndex*) tree, str_runs, thread_num, run_time);
  std::cout << "end, throughput: " << run_tpt << std::endl;

  index_usage = index_usage - init_usage;
  if(index_type != MASSTREE && index_type != WORMHOLE) index_usage += load_usage;
  std::cout << "-- memory usage, index with loads: " << double(index_usage) / kGigaByte
            << ", only loads: " << double(load_usage) / kGigaByte << std::endl;

  return 0;
}