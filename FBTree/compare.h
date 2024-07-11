/*
 * Copyright (c) 2022, Chen Yuan <yuan.chen@whu.edu.cn>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#ifndef INDEXRESEARCH_COMPARE_H
#define INDEXRESEARCH_COMPARE_H

#include "config.h"
#include "simd.h"

using util::cmpeq_int8_simd128;
using util::cmpeq_int8_simd256;
using util::cmpeq_int8_simd512;

using util::cmplt_int8_simd128;
using util::cmplt_int8_simd256;
using util::cmplt_int8_simd512;

namespace FeatureBTree {

inline uint64_t compare_equal_16(void* p, char c) {
  return cmpeq_int8_simd128(p, c);
}

inline uint64_t compare_equal_32(void* p, char c) {
  if(Config::kCmpMode == SIMD256) {
    return cmpeq_int8_simd256(p, c);
  } else {
    uint64_t m1 = compare_equal_16(p, c);
    uint64_t m2 = compare_equal_16((char*) p + 16, c);
    return (m2 << 16) | m1;
  }
}

inline uint64_t compare_equal_64(void* p, char c) {
  if(Config::kCmpMode == SIMD512) {
    return cmpeq_int8_simd512(p, c);
  } else {
    uint64_t m1 = compare_equal_32(p, c);
    uint64_t m2 = compare_equal_32((char*) p + 32, c);
    return (m2 << 32) | m1;
  }
}

inline uint64_t compare_equal_16(void* p1, void* p2) {
  return cmpeq_int8_simd128(p1, p2);
}

inline uint64_t compare_equal_32(void* p1, void* p2) {
  if(Config::kCmpMode == SIMD256) {
    return cmpeq_int8_simd256(p1, p2);
  } else {
    uint64_t m1 = compare_equal_16(p1, p2);
    uint64_t m2 = compare_equal_16((char*) p1 + 16, (char*) p2 + 16);
    return (m2 << 16) | m1;
  }
}

inline uint64_t compare_equal_64(void* p1, void* p2) {
  if(Config::kCmpMode == SIMD512) {
    return cmpeq_int8_simd512(p1, p2);
  } else {
    uint64_t m1 = compare_equal_32(p1, p2);
    uint64_t m2 = compare_equal_32((char*) p1 + 32, (char*) p2 + 32);
    return (m2 << 32) | m1;
  }
}

inline uint64_t compare_less_16(void* p, char c) {
  return cmplt_int8_simd128(p, c);
}

inline uint64_t compare_less_32(void* p, char c) {
  if(Config::kCmpMode == SIMD256) {
    return cmplt_int8_simd256(p, c);
  } else {
    uint64_t m1 = compare_less_16(p, c);
    uint64_t m2 = compare_less_16((char*) p + 16, c);
    return (m2 << 16) | m1;
  }
}

inline uint64_t compare_less_64(void* p, char c) {
  if(Config::kCmpMode == SIMD512) {
    return cmplt_int8_simd512(p, c);
  } else {
    uint64_t m1 = compare_less_32(p, c);
    uint64_t m2 = compare_less_32((char*) p + 32, c);
    return (m2 << 32) | m1;
  }
}

inline uint64_t compare_less_16(void* p1, void* p2) {
  return cmplt_int8_simd128(p1, p2);
}

inline uint64_t compare_less_32(void* p1, void* p2) {
  if(Config::kCmpMode == SIMD256) {
    return cmplt_int8_simd256(p1, p2);
  } else {
    uint64_t m1 = compare_less_16(p1, p2);
    uint64_t m2 = compare_less_16((char*) p1 + 16, (char*) p2 + 16);
    return (m2 << 16) | m1;
  }
}

inline uint64_t compare_less_64(void* p1, void* p2) {
  if(Config::kCmpMode == SIMD512) {
    return cmplt_int8_simd512(p1, p2);
  } else {
    uint64_t m1 = compare_less_32(p1, p2);
    uint64_t m2 = compare_less_32((char*) p1 + 32, (char*) p2 + 32);
    return (m2 << 32) | m1;
  }
}

}

#endif //INDEXRESEARCH_COMPARE_H
