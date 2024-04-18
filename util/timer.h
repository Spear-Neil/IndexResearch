#ifndef UTIL_TIMER_H
#define UTIL_TIMER_H

#include <chrono>
#include <inttypes.h>
#include <x86intrin.h>
#include "common.h"

namespace util {

template<typename Clock = std::chrono::system_clock>
class Timer {
 public:
  void start() {
    begin_ = Clock::now();
  }

  long duration_ns() {
    auto end = Clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin_).count();
  }

  long duration_us() {
    auto end = Clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - begin_).count();
  }

  long duration_ms() {
    auto end = Clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - begin_).count();
  }

  long duration_s() {
    auto end = Clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(end - begin_).count();
  }

 private:
  std::chrono::time_point<Clock> begin_;
};

inline uint64_t rdtsc() {  //access cpu cycle count
  return _rdtsc();
}

inline void delay(uint64_t duration, double freq = 3.0) { //ns, GHz
  uint64_t tick = rdtsc();
  uint64_t cycle = duration * freq;
  while(rdtsc() - tick < cycle) nop();
}

}

#endif //UTIL_TIMER_H
