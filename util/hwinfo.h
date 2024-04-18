#ifndef UTIL_HWINFO_H
#define UTIL_HWINFO_H

#include <iostream>
#include <cstring>
#include <random>

namespace util {

int access_latency(size_t memory_size, size_t attempts, size_t align = 4 * 1024) {
  char* memory = (char*) aligned_alloc(align, memory_size);
  std::memset(memory, 0, memory_size);

  size_t cycle_sum = 0, cycle_empty = 0, idx;
  std::mt19937 rand;
  for(size_t i = 0; i < attempts; i++) {
    idx = rand() % memory_size;
    uint32_t cycle_used, edx, t1, t2;
    asm ("mfence\n\t"
         "rdtsc\n\t"
         "mov %%edx, %2\n\t"
         "mov %%eax, %3\n\t"
         "mfence\n\t"
         "mov %4, %%al\n\t"
         "mfence\n\t"
         "rdtsc\n\t"
         "sub %2, %%edx\n\t"
         "sbb %3, %%eax"
      :"=a"(cycle_used)
    , "=d"(edx)
    , "=r"(t1)
    , "=r"(t2)
      :"m"(memory[idx])
      );
    cycle_sum += cycle_used;

    asm ("mfence\n\t"
         "rdtsc\n\t"
         "mov %%edx, %2\n\t"
         "mov %%eax, %3\n\t"
         "mfence\n\t"
         "mfence\n\t"
         "rdtsc\n\t"
         "sub %2, %%edx\n\t"
         "sbb %3, %%eax"
      :"=a"(cycle_used)
    , "=d"(edx)
    , "=r"(t1)
    , "=r"(t2)
      :"m"(memory[idx])
      );
    cycle_empty += cycle_used;
  }

  return (cycle_sum - cycle_empty) / attempts;
}

struct CacheInfo {
  /* -- cache type --
   * 0: no more caches,
   * 1: Data Cache,
   * 2: Instruction Cache,
   * 3: unified Cache,
   * other: unknown */
  int cache_id;  // index of using cpuid instruction to get cache info, zero-based
  int cache_type;
  int cache_level;
  int self_init;
  int full_associative;
  int shared_logical;  // the number of logical processors sharing the cache
  // maybe greater than the number of logical processors you can see in your system
  int line_size;  // cache line size, also coherency line size
  int partitions; // physical line partitions
  int ways; // the number of ways of association
  int sets; // number of sets
  int size; // cache size
  int latency; // access latency (hardware cycles)
  int attempts = 1024 * 1024 * 10; // repeat count for latency test

  void get_info(int cache_idx) {
    uint32_t eax = 0x04, ebx, ecx = cache_idx, edx;
    asm("cpuid":"+a"(eax), "=b"(ebx), "+c"(ecx), "=d"(edx));

    cache_id = cache_idx;
    cache_type = eax & 0x1F;
    cache_level = (eax & 0xE0) >> 5;
    self_init = (eax & 0x100) ? 1 : 0;
    full_associative = (eax & 0x200) ? 1 : 0;
    shared_logical = ((eax & 0x03FF'C000) >> 14) + 1;
    line_size = (ebx & 0x0FFF) + 1;
    partitions = ((ebx & 0x002F'F000) >> 12) + 1;
    ways = ((ebx & 0xFFC0'0000) >> 22) + 1;
    sets = ecx + 1;
    size = ways * partitions * line_size * sets;
    if(cache_type == 1 || cache_type == 3)
      latency = access_latency(size, attempts);
  }

  void show_info() {
    if(cache_type) {
      std::cout << "-- Cache ID: " << cache_id << std::endl;
      std::cout << "  -- Cache type: ";
      switch(cache_type) {
        case 1:
          std::cout << "Data Cache" << std::endl;
          break;
        case 2:
          std::cout << "Instruction Cache" << std::endl;
          break;
        case 3:
          std::cout << "Unified Cache" << std::endl;
          break;
      }
      std::cout << "  -- Cache Level: " << cache_level << std::endl;
      std::cout << "  -- Self Initializing: " << self_init << std::endl;
      std::cout << "  -- Fully Associative: " << full_associative << std::endl;
      std::cout << "  -- shared by " << shared_logical << " logical processors" << std::endl;
      std::cout << "  -- Coherency Line Size: " << line_size << std::endl;
      std::cout << "  -- Physical Line partitions: " << partitions << std::endl;
      std::cout << "  -- Ways of associativity: " << ways << std::endl;
      std::cout << "  -- Number of Sets: " << sets << std::endl;
      std::cout << "  -- Cache Size: " << size << " bytes (" << size / 1024 << " kB)" << std::endl;
      if(cache_type == 1 || cache_type == 3)
        std::cout << "  -- Access Latency: " << latency << " cycles" << std::endl;
    }
  }
};

}

#endif //UTIL_HWINFO_H
