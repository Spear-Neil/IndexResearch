#include <iostream>
#include "hwinfo.h"

using namespace util;

void prefetch_test(size_t memory_size, size_t attempts, size_t step, size_t align = 4 * 1024) {
  char* memory = (char*) aligned_alloc(align, memory_size);
  std::memset(memory, 0, memory_size);

  size_t cycle_sum1 = 0, cycle_sum2 = 0, cycle_empty = 0, idx1, idx2;
  std::mt19937 rand;
  for(size_t i = 0; i < attempts; i++) {
    idx1 = rand() % (memory_size - step);
    idx2 = idx1 + step;
    uint32_t cycle_used1, cycle_used2, edx, t1, t2;
    asm ("mfence\n\t"
         "rdtsc\n\t"
         "mov %%edx, %2\n\t"
         "mov %%eax, %3\n\t"
         "mfence\n\t"
         "mov %5, %%al\n\t"
         "mfence\n\t"
         "rdtsc\n\t"
         "sub %2, %%edx\n\t"
         "sbb %3, %%eax\n\t"
         "mov %%eax, %4\n\t"
         "mfence\n\t"
         "rdtsc\n\t"
         "mov %%edx, %2\n\t"
         "mov %%eax, %3\n\t"
         "mfence\n\t"
         "mov %6, %%al\n\t"
         "mfence\n\t"
         "rdtsc\n\t"
         "sub %2, %%edx\n\t"
         "sbb %3, %%eax\n\t"
      :"=a"(cycle_used2)  // 0
    , "=d"(edx)           // 1
    , "=r"(t1)            // 2
    , "=r"(t2)            // 3
    , "=r"(cycle_used1)   // 4
      :"m"(memory[idx1])  // 5
    , "m"(memory[idx2])   // 6
      );
    cycle_sum1 += cycle_used1;
    cycle_sum2 += cycle_used2;

    asm ("mfence\n\t"
         "rdtsc\n\t"
         "mov %%edx, %2\n\t"
         "mov %%eax, %3\n\t"
         "mfence\n\t"
         "mfence\n\t"
         "rdtsc\n\t"
         "sub %2, %%edx\n\t"
         "sbb %3, %%eax"
      :"=a"(cycle_used1)
    , "=d"(edx)
    , "=r"(t1)
    , "=r"(t2)
      :"m"(memory[idx1])
    , "m"(memory[idx2])
      );
    cycle_empty += cycle_used1;
  }
  std::cout << "-- hardware prefetch test" << std::endl;
  std::cout << "  -- first line access latency: " << (cycle_sum1 - cycle_empty) / attempts << std::endl;
  std::cout << "  -- second line access latency: " << (cycle_sum2 - cycle_empty) / attempts << std::endl;
}

int main(int argc, char* argv[]) {
  int cache_id = 0;
  int step = 64;
  while(true) {
    CacheInfo info;
    info.get_info(cache_id++);
    if(!info.cache_type) break;
    info.show_info();
    if(info.cache_type == 1 || info.cache_type == 3)
      prefetch_test(info.size, info.attempts, step);
    std::cout << std::endl;
  }

  int mem_latency = access_latency(1024 * 1024 * 1024, 1024 * 1024 * 10);
  std::cout << "-- memory access latency: " << mem_latency << std::endl;
  prefetch_test(1024 * 1024 * 1024, 1024 * 1024 * 10, step);
}