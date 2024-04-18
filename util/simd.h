#ifndef UTIL_SIMD_H
#define UTIL_SIMD_H

#include <cstdint>
#include <x86intrin.h>
#include <cassert>
#include "common.h"

namespace util {

/** common-used simd compare instructions */

/**
 * Compare 16/32/64 bytes 'p' with 'c' one by one to check if they are equal to
 * the character 'c', using simd instruction, if equal the corresponding bit is
 * set to 1, else to zero, instance: if p[0] == c, set bits[0] = 1
 */

/*======================= SIMD 128 =======================*/
inline uint64_t cmpeq_int8_simd128(void* p, int8_t c) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p);
  __m128i v2 = _mm_set1_epi8(c);
  __m128i t = _mm_cmpeq_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpeq_uint8_simd128(void* p, uint8_t c) {
  return cmpeq_int8_simd128(p, c);
}

inline uint64_t cmpeq_int8_simd128(void* p1, void* p2) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p1);
  __m128i v2 = _mm_loadu_si128((const __m128i_u*) p2);
  __m128i t = _mm_cmpeq_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpeq_uint8_simd128(void* p1, void* p2) {
  return cmpeq_int8_simd128(p1, p2);
}

inline uint64_t cmplt_int8_simd128(void* p, int8_t c) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p);
  __m128i v2 = _mm_set1_epi8(c);
  __m128i t = _mm_cmplt_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmplt_uint8_simd128(void* p, uint8_t c) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p);
  __m128i v2 = _mm_set1_epi8(c + 0x80);
  __m128i t = _mm_set1_epi8(0x80);
  v1 = _mm_add_epi8(v1, t);
  t = _mm_cmplt_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmplt_int8_simd128(void* p1, void* p2) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p1);
  __m128i v2 = _mm_loadu_si128((const __m128i_u*) p2);
  __m128i t = _mm_cmplt_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmplt_uint8_simd128(void* p1, void* p2) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p1);
  __m128i v2 = _mm_loadu_si128((const __m128i_u*) p2);
  __m128i t = _mm_set1_epi8(0x80);
  v1 = _mm_add_epi8(v1, t), v2 = _mm_add_epi8(v2, t);
  t = _mm_cmplt_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpgt_int8_simd128(void* p, int8_t c) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p);
  __m128i v2 = _mm_set1_epi8(c);
  __m128i t = _mm_cmpgt_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpgt_uint8_simd128(void* p, uint8_t c) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p);
  __m128i v2 = _mm_set1_epi8(c + 0x80);
  __m128i t = _mm_set1_epi8(0x80);
  v1 = _mm_add_epi8(v1, t);
  t = _mm_cmpgt_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpgt_int8_simd128(void* p1, void* p2) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p1);
  __m128i v2 = _mm_loadu_si128((const __m128i_u*) p2);
  __m128i t = _mm_cmpgt_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpgt_uint8_simd128(void* p1, void* p2) {
  __m128i v1 = _mm_loadu_si128((const __m128i_u*) p1);
  __m128i v2 = _mm_loadu_si128((const __m128i_u*) p2);
  __m128i t = _mm_set1_epi8(0x80);
  v1 = _mm_add_epi8(v1, t), v2 = _mm_add_epi8(v2, t);
  t = _mm_cmpgt_epi8(v1, v2);
  unsigned int mask = _mm_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

/*======================= SIMD 256 =======================*/
inline uint64_t cmpeq_int8_simd256(void* p, int8_t c) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p);
  __m256i v2 = _mm256_set1_epi8(c);
  __m256i t = _mm256_cmpeq_epi8(v1, v2);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpeq_uint8_simd256(void* p, uint8_t c) {
  return cmpeq_int8_simd256(p, c);
}

inline uint64_t cmpeq_int8_simd256(void* p1, void* p2) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p1);
  __m256i v2 = _mm256_loadu_si256((const __m256i_u*) p2);
  __m256i t = _mm256_cmpeq_epi8(v1, v2);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpeq_uint8_simd256(void* p1, void* p2) {
  return cmpeq_int8_simd256(p1, p2);
}

inline uint64_t cmplt_int8_simd256(void* p, int8_t c) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p);
  __m256i v2 = _mm256_set1_epi8(c);
  __m256i t = _mm256_cmpgt_epi8(v2, v1);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmplt_uint8_simd256(void* p, uint8_t c) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p);
  __m256i v2 = _mm256_set1_epi8(c + 0x80);
  __m256i t = _mm256_set1_epi8(0x80);
  v1 = _mm256_add_epi8(v1, t);
  t = _mm256_cmpgt_epi8(v2, v1);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmplt_int8_simd256(void* p1, void* p2) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p1);
  __m256i v2 = _mm256_loadu_si256((const __m256i_u*) p2);
  __m256i t = _mm256_cmpgt_epi8(v2, v1);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmplt_uint8_simd256(void* p1, void* p2) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p1);
  __m256i v2 = _mm256_loadu_si256((const __m256i_u*) p2);
  __m256i t = _mm256_set1_epi8(0x80);
  v1 = _mm256_add_epi8(v1, t), v2 = _mm256_add_epi8(v2, t);
  t = _mm256_cmpgt_epi8(v2, v1);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpgt_int8_simd256(void* p, int8_t c) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p);
  __m256i v2 = _mm256_set1_epi8(c);
  __m256i t = _mm256_cmpgt_epi8(v1, v2);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpgt_uint8_simd256(void* p, uint8_t c) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p);
  __m256i v2 = _mm256_set1_epi8(c + 0x80);
  __m256i t = _mm256_set1_epi8(0x80);
  v1 = _mm256_add_epi8(v1, t);
  t = _mm256_cmpgt_epi8(v1, v2);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpgt_int8_simd256(void* p1, void* p2) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p1);
  __m256i v2 = _mm256_loadu_si256((const __m256i_u*) p2);
  __m256i t = _mm256_cmpgt_epi8(v1, v2);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

inline uint64_t cmpgt_uint8_simd256(void* p1, void* p2) {
  __m256i v1 = _mm256_loadu_si256((const __m256i_u*) p1);
  __m256i v2 = _mm256_loadu_si256((const __m256i_u*) p2);
  __m256i t = _mm256_set1_epi8(0x80);
  v1 = _mm256_add_epi8(v1, t), v2 = _mm256_add_epi8(v2, t);
  t = _mm256_cmpgt_epi8(v1, v2);
  unsigned int mask = _mm256_movemask_epi8(t);
  return static_cast<uint64_t>(mask);
}

/*======================= SIMD 512 =======================*/
inline uint64_t cmpeq_int8_simd512(void* p, int8_t c) {
  __m512i v1 = _mm512_loadu_si512((const void*) p);
  __m512i v2 = _mm512_set1_epi8(c);
  return _mm512_cmpeq_epi8_mask(v1, v2);
}

inline uint64_t cmpeq_uint8_simd512(void* p, uint8_t c) {
  return cmpeq_int8_simd512(p, c);
}

inline uint64_t cmpeq_int8_simd512(void* p1, void* p2) {
  __m512i v1 = _mm512_loadu_si512((const void*) p1);
  __m512i v2 = _mm512_loadu_si512((const void*) p2);
  return _mm512_cmpeq_epi8_mask(v1, v2);
}

inline uint64_t cmpeq_uint8_simd512(void* p1, void* p2) {
  return cmpeq_int8_simd512(p1, p2);
}

inline uint64_t cmplt_int8_simd512(void* p, int8_t c) {
  __m512i v1 = _mm512_loadu_si512((const void*) p);
  __m512i v2 = _mm512_set1_epi8(c);
  return _mm512_cmplt_epi8_mask(v1, v2);
}

inline uint64_t cmplt_uint8_simd512(void* p, uint8_t c) {
  __m512i v1 = _mm512_loadu_si512((const void*) p);
  __m512i v2 = _mm512_set1_epi8(c + 0x80);
  __m512i t = _mm512_set1_epi8(0x80);
  v1 = _mm512_add_epi8(v1, t);
  return _mm512_cmplt_epi8_mask(v1, v2);
}

inline uint64_t cmplt_int8_simd512(void* p1, void* p2) {
  __m512i v1 = _mm512_loadu_si512((const void*) p1);
  __m512i v2 = _mm512_loadu_si512((const void*) p2);
  return _mm512_cmplt_epi8_mask(v1, v2);
}

inline uint64_t cmplt_uint8_simd512(void* p1, void* p2) {
  __m512i v1 = _mm512_loadu_si512((const void*) p1);
  __m512i v2 = _mm512_loadu_si512((const void*) p2);
  __m512i t = _mm512_set1_epi8(0x80);
  v1 = _mm512_add_epi8(v1, t), v2 = _mm512_add_epi8(v2, t);
  return _mm512_cmplt_epi8_mask(v1, v2);
}

inline uint64_t cmpgt_int8_simd512(void* p, int8_t c) {
  __m512i v1 = _mm512_loadu_si512((const void*) p);
  __m512i v2 = _mm512_set1_epi8(c);
  return _mm512_cmpgt_epi8_mask(v1, v2);
}

inline uint64_t cmpgt_uint8_simd512(void* p, uint8_t c) {
  __m512i v1 = _mm512_loadu_si512((const void*) p);
  __m512i v2 = _mm512_set1_epi8(c + 0x80);
  __m512i t = _mm512_set1_epi8(0x80);
  v1 = _mm512_add_epi8(v1, t);
  return _mm512_cmpgt_epi8_mask(v1, v2);
}

inline uint64_t cmpgt_int8_simd512(void* p1, void* p2) {
  __m512i v1 = _mm512_loadu_si512((const void*) p1);
  __m512i v2 = _mm512_loadu_si512((const void*) p2);
  return _mm512_cmpgt_epi8_mask(v1, v2);
}

inline uint64_t cmpgt_uint8_simd512(void* p1, void* p2) {
  __m512i v1 = _mm512_loadu_si512((const void*) p1);
  __m512i v2 = _mm512_loadu_si512((const void*) p2);
  __m512i t = _mm512_set1_epi8(0x80);
  v1 = _mm512_add_epi8(v1, t), v2 = _mm512_add_epi8(v2, t);
  return _mm512_cmpgt_epi8_mask(v1, v2);
}

/** common-used simd store instructions */

/** store 16-/32-/64-bytes of integer data from src to dst */
inline void store_simd128(void* dst, void* src) {
  __m128i v = _mm_loadu_si128((const __m128i_u*) src);
  _mm_storeu_si128((__m128i_u*) dst, v);
}

inline void store_simd256(void* dst, void* src) {
  __m256i v = _mm256_loadu_si256((const __m256i_u*) src);
  _mm256_storeu_si256((__m256i_u*) dst, v);
}

inline void store_simd512(void* dst, void* src) {
  __m512i v = _mm512_loadu_si512((const void*) src);
  _mm512_storeu_si512(dst, v);
}

/**
 * non-temporal store using simd instruction, memory type: write combining
 * after these operations, an memory fence is needed to make these memory operation
 * visible to other thread/cpu, dst may have to be aligned on 16-/32-/64-byte boundary
 */
inline void ntstore_simd128(void* dst, int n) {
  _mm_stream_si32((int*) dst, n);
}

inline void ntstore_simd128(void* dst, int64_t n) {
  _mm_stream_si64((long long*) dst, n);
}

inline void ntstore_simd128(void* dst, void* src) {
  assert(aligned(dst, 16));
  __m128i v = _mm_loadu_si128((const __m128i_u*) src);
  _mm_stream_si128((__m128i*) dst, v);
}

inline void ntstore_simd256(void* dst, void* src) {
  assert(aligned(dst, 32));
  __m256i v = _mm256_loadu_si256((const __m256i_u*) src);
  _mm256_stream_si256((__m256i*) dst, v);
}

inline void ntstore_simd512(void* dst, void* src) {
  assert(aligned(dst, 64));
  __m512i v = _mm512_loadu_si512((const void*) src);
  _mm512_stream_si512((__m512i*) dst, v);
}

}

#endif //UTIL_SIMD_H
