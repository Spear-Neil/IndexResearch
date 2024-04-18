#include <iostream>
#include "log.h"

int main(int argc, char* argv[]) {
  std::cout << "-- log example" << std::endl;
  LOG("normal log: %i", 0);
  CONDITION_LOG(argc == 1, "conditional log: %i", 1);
  CONDITION_ERROR(false, "conditional error: %i", 2);
  ERROR("normal error: %i", 3);
}