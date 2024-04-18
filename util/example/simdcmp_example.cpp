#include <iostream>
#include "simd.h"

using namespace util;

int main(int argc, char* argv[]) {
  std::cout << "-- simd example" << std::endl;

  int mlen = 128, align = 32;
  void* v1 = aligned_alloc(align, mlen);
  void* v2 = aligned_alloc(align, mlen);
  for(int i = 0; i < mlen; i++) {
    ((char*) v1)[i] = i;
    ((char*) v2)[i] = i;
  }

  char c = 10;
  std::cout << std::hex;

#ifdef SSE2_ENABLE
  std::cout << "-- simd128 equal to" << std::endl;
  std::cout << cmpeq_int8_simd128((char*) v1 + 1, c) << " ";
  std::cout << cmpeq_uint8_simd128((char*) v1 + 1, c) << " ";
  std::cout << cmpeq_int8_simd128((char*) v1 + 1, (char*) v2 + 1) << " ";
  std::cout << cmpeq_uint8_simd128((char*) v1 + 1, (char*) v2 + 1) << std::endl;

  std::cout << "-- simd128 less than" << std::endl;
  std::cout << cmplt_int8_simd128((char*) v1, c) << " ";
  std::cout << cmplt_uint8_simd128((char*) v1, c) << " ";
  std::cout << cmplt_int8_simd128((char*) v1, (char*) v2 + 1) << " ";
  std::cout << cmplt_uint8_simd128((char*) v1, (char*) v2 + 1) << std::endl;

  std::cout << "-- simd128 greater than" << std::endl;
  std::cout << cmpgt_int8_simd128((char*) v1, c) << " ";
  std::cout << cmpgt_uint8_simd128((char*) v1, c) << " ";
  std::cout << cmpgt_int8_simd128((char*) v1 + 1, v2) << " ";
  std::cout << cmpgt_uint8_simd128((char*) v1 + 1, v2) << std::endl;
#endif

#ifdef AVX2_ENABLE
  std::cout << "-- simd256 equal to" << std::endl;
  std::cout << cmpeq_int8_simd256((char*) v1 + 1, c) << " ";
  std::cout << cmpeq_uint8_simd256((char*) v1 + 1, c) << " ";
  std::cout << cmpeq_int8_simd256((char*) v1 + 1, (char*) v2 + 1) << " ";
  std::cout << cmpeq_uint8_simd256((char*) v1 + 1, (char*) v2 + 1) << std::endl;

  std::cout << "-- simd256 less than" << std::endl;
  std::cout << cmplt_int8_simd256((char*) v1, c) << " ";
  std::cout << cmplt_uint8_simd256((char*) v1, c) << " ";
  std::cout << cmplt_int8_simd256((char*) v1, (char*) v2 + 1) << " ";
  std::cout << cmplt_uint8_simd256((char*) v1, (char*) v2 + 1) << std::endl;

  std::cout << "-- simd256 greater than" << std::endl;
  std::cout << cmpgt_int8_simd256((char*) v1, c) << " ";
  std::cout << cmpgt_uint8_simd256((char*) v1, c) << " ";
  std::cout << cmpgt_int8_simd256((char*) v1 + 1, v2) << " ";
  std::cout << cmpgt_uint8_simd256((char*) v1 + 1, v2) << std::endl;
#endif

#ifdef AVX512BW_ENABLE
  std::cout << "-- simd512 equal to" << std::endl;
  std::cout << cmpeq_int8_simd512((char*) v1 + 1, c) << " ";
  std::cout << cmpeq_uint8_simd512((char*) v1 + 1, c) << " ";
  std::cout << cmpeq_int8_simd512((char*) v1 + 1, (char*) v2 + 1) << " ";
  std::cout << cmpeq_uint8_simd512((char*) v1 + 1, (char*) v2 + 1) << std::endl;

  std::cout << "-- simd512 less than" << std::endl;
  std::cout << cmplt_int8_simd512((char*) v1, c) << " ";
  std::cout << cmplt_uint8_simd512((char*) v1, c) << " ";
  std::cout << cmplt_int8_simd512((char*) v1, (char*) v2 + 1) << " ";
  std::cout << cmplt_uint8_simd512((char*) v1, (char*) v2 + 1) << std::endl;

  std::cout << "-- simd512 greater than" << std::endl;
  std::cout << cmpgt_int8_simd512((char*) v1, c) << " ";
  std::cout << cmpgt_uint8_simd512((char*) v1, c) << " ";
  std::cout << cmpgt_int8_simd512((char*) v1 + 1, v2) << " ";
  std::cout << cmpgt_uint8_simd512((char*) v1 + 1, v2) << std::endl;
#endif

  return 0;
}