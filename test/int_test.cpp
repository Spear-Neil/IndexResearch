#include <iostream>
#include <vector>
#include <algorithm>
#include <mutex>
#include <thread>
#include "index.h"
#include "hash.h"
#include "pinning.h"
#include "timer.h"

using namespace util;

void data_prepare(std::vector<uint64_t>& warmup, std::vector<uint64_t>& runs,
                  size_t warmup_size, size_t run_size, int run_type) {
  std::cout << "-- data prepare ... " << std::flush;
  uint64_t gen_count = 0;
  while(true) {
    warmup.push_back(hash(gen_count++));
    if(warmup.size() >= warmup_size) break;
  }

  while(true) {
    uint64_t key = gen_count++;  // monotonically increasing integer
    if(run_type == 0) key = hash(key); // random integer keys
    runs.push_back(key);
    if(runs.size() >= run_size) break;
  }

  if(run_size != runs.size() || warmup.size() != warmup.size()) {
    std::cout << "\n-- data prepare error" << std::endl;
    exit(-1);
  }
  std::cout << "end" << std::endl;
}

int main(int argc, char* argv[]) {
  if(argc < 6) {
    std::cout << "-- arg 0: warmup load size\n"
              << "-- arg 1: run load size\n"
              << "-- arg 2: run time (lookup/scan)\n"
              << "-- arg 3: thread number\n"
              << "-- arg 4: index type\n"
              << "-- arg 5: scan size (10 by default)\n"
              << "-- arg 6: run load random(0)/mono(1) integer (random by default)" << std::endl;
    exit(-1);
  }

  size_t warmup_size = std::stoul(argv[1]);
  size_t run_size = std::stoul(argv[2]);
  int run_time = std::stoi(argv[3]);
  int nthd = std::stoi(argv[4]);
  int index_type = std::stoi(argv[5]);
  int scan_size = 10;
  if(argc > 6) scan_size = std::stoi(argv[6]);
  int run_type = 0;
  if(argc > 7) run_type = std::stoi(argv[7]);
  if(run_type != 0 && run_type != 1) {
    std::cout << "-- error run type" << std::endl;
    exit(-1);
  }

  Index<uint64_t, uint64_t>* tree = IndexFactory<uint64_t, uint64_t>::get_index((INDEX_TYPE) index_type);
  if(tree == nullptr) {
    std::cout << "-- no such index" << std::endl;
    exit(-1);
  }

  std::cout << "-- warmup size: " << warmup_size << ", run size: " << run_size
            << ", run time: " << run_time << ", thread num: " << nthd << ", index type: " << tree->index_type()
            << ", scan size: " << scan_size << ", run type: " << (run_type ? "mono" : "random") << std::endl;

  PinningMap pinning;
  pinning.pinning_thread(0, 0, pthread_self());

  Timer<> timer;
  double warmup_tpt = 0, insert_tpt = 0, lookup_tpt = 0, scan_tpt = 0;

  std::vector<uint64_t> warmup, runs;
  warmup.reserve(warmup_size), runs.reserve(run_size);
  data_prepare(warmup, runs, warmup_size, run_size, run_type);

  std::cout << "-- warmup ... " << std::flush;
  timer.start();
  for(uint64_t key : warmup) {
    tree->insert(key, key);
  }
  long drt = timer.duration_us();
  warmup_tpt += double(warmup_size) / drt;
  std::cout << "end" << std::endl;

  std::vector<std::thread> workers;
  std::mutex lock;
  std::atomic<bool> terminate;
  std::atomic<int> ready;
  std::vector<double> tpt;

  auto preparation = [&]() {
    pinning.reset_pinning_counter(0, 0);
    workers.clear(), tpt.clear();
    terminate.store(false), ready.store(0);
  };

  auto termination = [&]() {
    while(ready.load() != nthd);
    timer.start();
    while(timer.duration_s() < run_time) {
      using namespace std::chrono_literals;
      std::this_thread::sleep_for(1ms);
    }
    terminate.store(true);
  };

  std::cout << "-- random shuffle ... " << std::flush;
  std::random_shuffle(runs.begin(), runs.end());
  std::cout << "insert ... " << std::flush;
  preparation();
  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pinning.pinning_thread_continuous(pthread_self());
      size_t begin = tid * run_size / nthd;
      size_t end = (tid + 1) * run_size / nthd;

      std::vector<uint64_t> local_runs;
      local_runs.reserve(end - begin);
      for(size_t i = begin; i < end; i++) {
        local_runs.push_back(runs[i]);
      }
      ready.fetch_add(1);
      while(ready.load() != nthd);

      Timer<> timer;
      timer.start();
      for(uint64_t key : local_runs) {
        tree->insert(key, key);
      }
      long drt = timer.duration_us();
      std::lock_guard<std::mutex> guard(lock);
      tpt.push_back(double(local_runs.size()) / drt);
    }, tid));
  }
  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    insert_tpt += tpt[tid];
  }
  std::cout << "end" << std::endl;

  std::cout << "-- random shuffle ... " << std::flush;
  std::random_shuffle(runs.begin(), runs.end());
  std::cout << "lookup ... " << std::flush;
  preparation();
  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pinning.pinning_thread_continuous(pthread_self());
      size_t begin = tid * run_size / nthd;
      size_t end = (tid + 1) * run_size / nthd;

      std::vector<uint64_t> local_runs;
      local_runs.reserve(end - begin);
      for(size_t i = begin; i < end; i++) {
        local_runs.push_back(runs[i]);
      }
      ready.fetch_add(1);
      while(ready.load() != nthd);

      Timer<> timer;
      timer.start();

      uint64_t value, opcnt = 0, size = local_runs.size();
      while(true) {
        uint64_t key = local_runs[opcnt % size];
        bool find = tree->lookup(key, value);
        if(!find || key != value) {
          std::cout << "\n-- tid: " << tid << ", lookup error" << std::endl;
          exit(-1);
        }
        if(opcnt++ % 10000 == 0 && terminate.load()) break;
      }
      long drt = timer.duration_us();
      std::lock_guard<std::mutex> guard(lock);
      tpt.push_back(double(opcnt) / drt);
    }, tid));
  }

  termination();
  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    lookup_tpt += tpt[tid];
  }
  std::cout << "end" << std::endl;

  std::cout << "-- random shuffle ... " << std::flush;
  std::random_shuffle(runs.begin(), runs.end());
  std::cout << "scan ... " << std::flush;
  preparation();
  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pinning.pinning_thread_continuous(pthread_self());
      size_t begin = tid * run_size / nthd;
      size_t end = (tid + 1) * run_size / nthd;

      std::vector<uint64_t> local_runs;
      local_runs.reserve(end - begin);
      for(size_t i = begin; i < end; i++) {
        local_runs.push_back(runs[i]);
      }
      ready.fetch_add(1);
      while(ready.load() != nthd);

      Timer<> timer;
      timer.start();

      uint64_t opcnt = 0, size = local_runs.size();
      while(true) {
        uint64_t key = local_runs[opcnt % size];
        int count = tree->scan(key, scan_size);
        if(count < 0) {
          std::cout << "\n-- tid: " << tid << ", scan error" << std::endl;
          exit(-1);
        }
        if(opcnt++ % 10000 == 0 && terminate.load()) break;
      }
      long drt = timer.duration_us();
      std::lock_guard<std::mutex> guard(lock);
      tpt.push_back(double(opcnt) / drt);
    }, tid));
  }

  termination();
  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    scan_tpt += tpt[tid];
  }
  std::cout << "end" << std::endl;

  std::cout << "-- warmup tpt: " << warmup_tpt << std::endl;
  std::cout << "-- insert tpt: " << insert_tpt << std::endl;
  std::cout << "-- lookup tpt: " << lookup_tpt << std::endl;
  std::cout << "-- scan tpt: " << scan_tpt << std::endl;
}
