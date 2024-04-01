#include <iostream>
#include <vector>

#include "MassTreeWrapper.h"
#include "timer.h"
#include "pinning.h"

using util::Timer;

volatile uint64_t globalepoch = 1;     // global epoch, updated by main thread regularly
volatile uint64_t active_epoch = 1;

void simple_test(size_t ndata) {
  Timer<> timer;
  MassTreeWrapper<int> tree;

  std::vector<std::string> data;
  data.reserve(ndata);
  for(size_t i = 0; i < ndata; i++)
    data.emplace_back(std::to_string(i));

  std::random_shuffle(data.begin(), data.end());

  timer.start();
  for(size_t i = 0; i < ndata; i++) {
    tree.upsert(data[i], std::stoi(data[i]));
  }
  long insert_duration = timer.duration_us();

  timer.start();
  for(size_t i = 0; i < ndata; i++) {
    tree.upsert(data[i], std::stoi(data[i]) + 1);
  }
  long update_duration = timer.duration_us();

  timer.start();
  for(size_t i = 0; i < ndata; i++) {
    int value;
    MassEpochGuard epoch_guard;
    if(!tree.search(data[i], value))
      std::cout << "error not found: " << data[i] << std::endl;
    else {
      if(value != std::stoi(data[i]) + 1)
        std::cout << "error result: " << data[i] << "   " << value << std::endl;
    }
  }
  long search_duration = timer.duration_us();

  std::cout << "insert opus: " << double(ndata) / insert_duration << std::endl;
  std::cout << "update opus: " << double(ndata) / update_duration << std::endl;
  std::cout << "search opus: " << double(ndata) / search_duration << std::endl;
}

int main() {
  simple_test(1000000);

  return 0;
}