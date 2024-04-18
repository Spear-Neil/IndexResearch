#include <iostream>
#include "simd.h"

using namespace util;

int main(int agc, char* argv[]) {
  std::cout << "-- simd example" << std::endl;

  int mlen = 128, align = 64;
  void* dst = aligned_alloc(align, mlen);
  void* src = aligned_alloc(align, mlen);
  for(int i = 0; i < mlen; i++) ((char*) src)[i] = i;

  auto present = [&]() {
    for(int i = 0; i < mlen; i++)
      std::cout << int(((char*) dst)[i]) << " ";
    std::cout << std::endl;
  };

#ifdef SSE2_ENABLE
  store_simd128((char*) dst + 1, (char*) src + 1);
  present();
  ntstore_simd128((char*) dst + 1, int(10));
  present();
  ntstore_simd128((char*) dst + 2, int64_t(10));
  present();
  ntstore_simd128(dst, (char*) src + 1);
  present();
#endif

#ifdef AVX_ENABLE
  store_simd256((char*) dst + 1, (char*) src + 1);
  present();
  ntstore_simd256(dst, (char*) src + 1);
  present();
#endif

#ifdef AVX512F_ENABLE
  store_simd512((char*) dst + 1, (char*) src + 1);
  present();
  ntstore_simd512(dst, (char*) src + 1);
  present();
#endif

  return 0;
}