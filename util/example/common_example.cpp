#include <iostream>
#include "common.h"

using namespace util;

int main(int argc, char* argv[]) {
  int num = 0xFF;
  std::cout << "-- common example" << std::endl;

  std::cout << "-- popcount: " << popcount(num) << std::endl;
  std::cout << "-- countl_zero: " << countl_zero(num) << std::endl;
  std::cout << "-- countl_one: " << countl_one(num) << std::endl;
  std::cout << "-- countr_zero: " << countr_zero(num) << std::endl;
  std::cout << "-- countr_one: " << countr_one(num) << std::endl;
  std::cout << "-- index_least1: " << index_least1(num) << std::endl;
  std::cout << "-- index_least0: " << index_least0(num) << std::endl;
  std::cout << "-- index_most1: " << index_most1(num) << std::endl;
  std::cout << "-- index_most0: " << index_most0(num) << std::endl;
  std::cout << "-- byte_swap: " << byte_swap(num) << std::endl;
  std::cout << "-- parity: " << parity(num) << std::endl;

  std::cout << "-- roundup: " << roundup(num, 256) << std::endl;
  std::cout << "-- rounddown: " << rounddown(num, 256) << std::endl;
  std::cout << "-- roundup_exp2: " << roundup_exp2(num, 256) << std::endl;
  std::cout << "-- rounddown_exp2: " << rounddown_exp2(num, 256) << std::endl;

  std::cout << "-- logical cpu num: " << ncpus_online() << std::endl;

  return 0;
}