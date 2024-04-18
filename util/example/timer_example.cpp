#include <iostream>
#include "timer.h"

using namespace util;

int main(int argc, char* argv[]) {
  std::cout << "-- timer example" << std::endl;
  Timer timer;
  timer.start();
  std::cout << timer.duration_s() << " "
            << timer.duration_ms() << " "
            << timer.duration_us() << " "
            << timer.duration_ns() << std::endl;

  return 0;
}