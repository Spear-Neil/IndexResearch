#include <iostream>
#include <mutex>
#include "fbtree.h"
#include "util.h"

using namespace FeatureBTree;
using util::PinningMap;
using util::Timer;

void simple_test(size_t nkey, int nthd, bool shuffle) {
  PinningMap pinning;
  pinning.numa_set_localalloc();
  pinning.pinning_thread(0, 0, pthread_self());

  FBTree<std::string, uint64_t> tree;
  std::vector<std::string> data;
  data.reserve(nkey);
  std::vector<std::thread> workers;
  std::vector<double> tpts;
  std::mutex lock;
  double itpt = 0, utpt = 0, stpt = 0, rtpt = 0;
  double scan_tpt = 0;
  tree.node_parameter();

  std::cout << "-- data prepare ... " << std::flush;
  for(size_t i = 0; i < nkey; i++)
    data.push_back(std::string("key") + std::to_string(i));
  std::cout << "end" << std::endl;

  auto shuffle_func = [&]() {
    std::cout << "-- random shuffle ... " << std::flush;
    std::random_shuffle(data.begin(), data.end());
    std::cout << "end" << std::endl;
  };

  if(shuffle) shuffle_func();
  pinning.reset_pinning_counter(0, 0);
  std::cout << "-- insert ... " << std::flush;
  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pinning.pinning_thread_continuous(pthread_self());
      Timer timer;
      timer.start();
      size_t begin = nkey * tid / nthd;
      size_t end = nkey * (tid + 1) / nthd;
      for(size_t i = begin; i < end; i++) {
        EpochGuard epoch_guard(tree.get_epoch());
        void* old = tree.upsert(data[i], i);
        if(old != nullptr) {
          std::cout << "insert error: " << data[i] << std::endl;
          exit(-1);
        }
      }
      long drt = timer.duration_us();
      std::lock_guard<std::mutex> guard(lock);
      tpts.push_back(double(end - begin) / drt);
    }, tid));
  }
  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    itpt += tpts[tid];
  }
  std::cout << "end" << std::endl;

  if(shuffle) shuffle_func();
  workers.clear();
  tpts.clear();
  pinning.reset_pinning_counter(0, 0);
  std::cout << "-- update ... " << std::flush;
  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pinning.pinning_thread_continuous(pthread_self());
      Timer timer;
      timer.start();
      size_t begin = nkey * tid / nthd;
      size_t end = nkey * (tid + 1) / nthd;
      for(size_t i = begin; i < end; i++) {
        EpochGuard epoch_guard(tree.get_epoch(), 1);
        void* old = tree.update(data[i], i);
        if(old == nullptr) {
          std::cout << "update error: " << data[i] << std::endl;
          exit(-1);
        }
        epoch_guard.retire(old);
      }
      long drt = timer.duration_us();
      std::lock_guard<std::mutex> guard(lock);
      tpts.push_back(double(end - begin) / drt);
    }, tid));
  }
  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    utpt += tpts[tid];
  }
  std::cout << "end" << std::endl;

  if(shuffle) shuffle_func();
  workers.clear();
  tpts.clear();
  pinning.reset_pinning_counter(0, 0);
  std::cout << "-- lookup ... " << std::flush;
  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pinning.pinning_thread_continuous(pthread_self());
      Timer<> timer;
      size_t begin = nkey * tid / nthd;
      size_t end = nkey * (tid + 1) / nthd;
      timer.start();
      for(size_t i = begin; i < end; i++) {
        EpochGuard epoch_guard(tree.get_epoch());
        auto kv = tree.lookup(data[i]);
        if(kv == nullptr) {
          std::cout << "not found: " << data[i] << std::endl;
          exit(-1);
        }
      }
      long drt = timer.duration_us();
      std::lock_guard<std::mutex> guard(lock);
      tpts.push_back(double(end - begin) / drt);
    }, tid));
  }
  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    stpt += tpts[tid];
  }
  std::cout << "end" << std::endl;

  if(shuffle) shuffle_func();
  workers.clear();
  tpts.clear();
  pinning.reset_pinning_counter(0, 0);
  std::cout << "-- scan ... " << std::flush;
  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pinning.pinning_thread_continuous(pthread_self());
      Timer<> timer;
      size_t begin = nkey * tid / nthd;
      size_t end = nkey * (tid + 1) / nthd;
      timer.start();
      for(size_t i = begin; i < end; i++) {
        EpochGuard epoch_guard(tree.get_epoch());
        auto it = tree.lower_bound(data[i]);
        for(int i = 0; i < 10 && !it.end(); i++) {
          it.advance();
        }
      }
      long drt = timer.duration_us();
      std::lock_guard<std::mutex> guard(lock);
      tpts.push_back(double(end - begin) / drt);
    }, tid));
  }
  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    scan_tpt += tpts[tid];
  }
  std::cout << "end" << std::endl;

  if(shuffle) shuffle_func();
  workers.clear();
  tpts.clear();
  pinning.reset_pinning_counter(0, 0);
  std::cout << "-- remove ... " << std::flush;
  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pinning.pinning_thread_continuous(pthread_self());
      Timer<> timer;
      size_t begin = nkey * tid / nthd;
      size_t end = nkey * (tid + 1) / nthd;
      timer.start();
      for(size_t i = begin; i < end; i++) {
        EpochGuard epoch_guard(tree.get_epoch());
        auto kv = tree.remove(data[i]);
        epoch_guard.retire(kv);
      }
      long drt = timer.duration_us();
      std::lock_guard<std::mutex> guard(lock);
      tpts.push_back(double(end - begin) / drt);
    }, tid));
  }
  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    rtpt += tpts[tid];
  }
  std::cout << "end" << std::endl;

  std::cout << "-- insert opus: " << itpt << std::endl;
  std::cout << "-- update opus: " << utpt << std::endl;
  std::cout << "-- lookup opus: " << stpt << std::endl;
  std::cout << "-- scan opus: " << scan_tpt << std::endl;
  std::cout << "-- remove opus: " << rtpt << std::endl;
  tree.statistics();
}

int main(int argc, char* argv[]) {
  if(argc < 4) {
    std::cout << "-- nkey, nthd, shuffle" << std::endl;
    exit(-1);
  }
  size_t nkey = std::stoul(argv[1]);
  int nthd = std::stoi(argv[2]);
  int shuffle = std::stoi(argv[3]);

  std::cout << "-- simple test: " << nkey << ", " << nthd << ", " << shuffle << std::endl;
  simple_test(nkey, nthd, shuffle);
  return 0;
}