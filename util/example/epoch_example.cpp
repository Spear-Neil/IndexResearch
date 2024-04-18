#include <iostream>
#include <vector>
#include <thread>
#include "epoch.h"
#include "timer.h"

using namespace util;

int main(int argc, char* argv[]) {
  int max_thd = 20;
  size_t nop = 1000000;

  std::cout << "-- epoch normal retire test:" << std::endl;
  for(int nthd = 1; nthd <= max_thd; nthd++) {
    std::vector<std::thread> workers;
    Epoch epoch;
    Timer<> timer;
    timer.start();
    for(int tid = 0; tid < nthd; tid++) {
      workers.push_back(std::thread([&](int tid) {
        for(size_t i = 0; i < nop; i++) {
          epoch.startop();
          void* p = malloc(sizeof(size_t));
          epoch.retire(p);
          epoch.endop();
        }
      }, tid));
    }
    for(int tid = 0; tid < nthd; tid++) workers[tid].join();
    long duration = timer.duration_us();
    std::cout << "  -- thd num: " << nthd << ", op num: " << nthd * nop <<
              ", tpt: " << double(nthd * nop) / duration << std::endl;
  }

  std::cout << "-- epoch retire with type test:" << std::endl;
  for(int nthd = 1; nthd <= max_thd; nthd++) {
    std::vector<std::thread> workers;
    Epoch epoch;
    Timer<> timer;
    timer.start();
    for(int tid = 0; tid < nthd; tid++) {
      workers.push_back(std::thread([&](int tid) {
        for(size_t i = 0; i < nop; i++) {
          epoch.startop();
          int* x = new int;
          epoch.retire([x]() { delete x; });
          epoch.endop();
        }
      }, tid));
    }
    for(int tid = 0; tid < nthd; tid++) workers[tid].join();
    long duration = timer.duration_us();
    std::cout << "  -- thd num: " << nthd << ", op num: " << nthd * nop <<
              ", tpt: " << double(nthd * nop) / duration << std::endl;
  }

  return 0;
}