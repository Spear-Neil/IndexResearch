#ifndef UTIL_COMMON_H
#define UTIL_COMMON_H

#include <cstdint>
#include <cassert>
#include <x86intrin.h>
#include <unistd.h>

namespace util {

/** memory fence to control the act of compilation and execution */
inline void barrier() { asm volatile("":: :"memory"); }       // compile fence,
inline void mfence() { asm volatile("mfence":: :"memory"); }  // memory fence, on x86, flush store buffer
inline void lfence() { asm volatile("lfence":: :"memory"); }  // load fence
inline void sfence() { asm volatile("sfence":: :"memory"); }  // store fence, on x86, flush store buffer

inline void nop() { asm volatile("nop"); }

/**
 * only available on x86/-64, copy from https://doc.rust-lang.org/stable/core/arch/x86_64/fn._mm_prefetch.html
 * reference: Intel® 64 and IA-32 Architectures Optimization Reference Manual
 * prefetch a cache line into cache, the second parameter of _mm_prefetch can be the follows:
 * _MM_HINT_T0: Fetch into all levels of the cache hierarchy.
 * _MM_HINT_T1: Fetch into L2 and higher.
 * _MM_HINT_T2: Fetch into L3 and higher or an implementation-specific choice (e.g., L2 if there is no L3).
 * _MM_HINT_NTA: Fetch data using the non-temporal access (NTA) hint. It may be a place closer than main
 * memory but outside of the cache hierarchy. This is used to reduce access latency without polluting the cache.
 * _MM_HINT_ET0 and _MM_HINT_ET1 are similar to _MM_HINT_T0 and _MM_HINT_T1 but indicate
 * an anticipation to write to the address.(in gcc9.4, ETO is equal to T0, based on my assembly code)
 */
inline void prefetcht0(void* line) { _mm_prefetch(line, _MM_HINT_T0); }

inline void prefetcht1(void* line) { _mm_prefetch(line, _MM_HINT_T1); }

inline void prefetcht2(void* line) { _mm_prefetch(line, _MM_HINT_T2); }

inline void prefetchnta(void* line) { _mm_prefetch(line, _MM_HINT_NTA); }

/**
 * count the number of 1-bits in 'n'
 */
inline int popcount(uint64_t n) { return __builtin_popcountl(n); }

inline int popcount(int64_t n) { return popcount(uint64_t(n)); }

inline int popcount(uint32_t n) { return __builtin_popcount(n); }

inline int popcount(int32_t n) { return popcount(uint32_t(n)); }

/**
 * count the number of continuous leading 0-bits in n, starting at the
 * most significant bit position, if n is 0, the result is undefined
 */
inline int countl_zero(uint64_t n) {
  assert(n != 0);
  return __builtin_clzl(n);
}

inline int countl_zero(int64_t n) {
  return countl_zero(uint64_t(n));
}

inline int countl_zero(uint32_t n) {
  assert(n != 0);
  return __builtin_clz(n);
}

inline int countl_zero(int32_t n) {
  return countl_zero(uint32_t(n));
}

/**
 * count the number of continuous leading 1-bits in n, starting at the
 * most significant bit position, if all bits of n is 1, the result is undefined
 */
inline int countl_one(uint64_t n) { return countl_zero(~n); }

inline int countl_one(int64_t n) { return countl_zero(~n); }

inline int countl_one(uint32_t n) { return countl_zero(~n); }

inline int countl_one(int32_t n) { return countl_zero(~n); }

/**
 * count the number of continuous rear 0-bits in n, starting at the
 * least significant bit position, if n is 0, the result is undefined
 */
inline int countr_zero(uint64_t n) {
  assert(n != 0);
  return __builtin_ctzl(n);
}

inline int countr_zero(int64_t n) {
  return countr_zero(uint64_t(n));
}

inline int countr_zero(uint32_t n) {
  assert(n != 0);
  return __builtin_ctz(n);
}

inline int countr_zero(int32_t n) {
  return countr_zero(uint32_t(n));
}

/**
 * count the number of continuous rear 1-bits in n, starting at the
 * least significant bit position, if all bits of n is 1, the result is undefined
 */
inline int countr_one(uint64_t n) { return countr_zero(~n); }

inline int countr_one(int64_t n) { return countr_zero(~n); }

inline int countr_one(uint32_t n) { return countr_zero(~n); }

inline int countr_one(int32_t n) { return countr_zero(~n); }

/**
 * return the index of the least significant 1-bit of n, or if
 * n is 0, return -1, can also be implemented with countr_zero
 */
inline int index_least1(uint64_t n) { return __builtin_ffsl(n) - 1; }

inline int index_least1(int64_t n) { return index_least1(uint64_t(n)); }

inline int index_least1(uint32_t n) { return __builtin_ffs(n) - 1; }

inline int index_least1(int32_t n) { return index_least1(uint32_t(n)); }

/**
 * return the index of the least significant 0-bit of n,
 * or if all bits of n is 1, return -1
 */
inline int index_least0(uint64_t n) { return index_least1(~n); }

inline int index_least0(int64_t n) { return index_least1(~n); }

inline int index_least0(uint32_t n) { return index_least1(~n); }

inline int index_least0(int32_t n) { return index_least1(~n); }

/**
 * return the index of the most significant 1-bit of n,
 * or if all bits of n is 0, return -1
 */
inline int index_most1(uint64_t n) { return n ? 63 - countl_zero(n) : -1; }

inline int index_most1(int64_t n) { return index_most1(uint64_t(n)); }

inline int index_most1(uint32_t n) { return n ? 31 - countl_zero(n) : -1; }

inline int index_most1(int32_t n) { return index_most1(uint32_t(n)); }

/**
 * return the index of the most significant 0-bit of n,
 * or if all bits of n is 1, return -1
 */
inline int index_most0(uint64_t n) { return index_most1(~n); }

inline int index_most0(int64_t n) { return index_most1(~n); }

inline int index_most0(uint32_t n) { return index_most1(~n); }

inline int index_most0(int32_t n) { return index_most1(~n); }

/**
 * return n with the order of the bytes reversed, example: 0xaabb -> 0xbbaa
 */
inline uint64_t byte_swap(uint64_t n) { return __builtin_bswap64(n); }

inline uint64_t byte_swap(int64_t n) { return byte_swap(uint64_t(n)); }

inline uint32_t byte_swap(uint32_t n) { return __builtin_bswap32(n); }

inline uint32_t byte_swap(int32_t n) { return byte_swap(uint32_t(n)); }

inline uint16_t byte_swap(uint16_t n) { return __builtin_bswap16(n); }

inline uint16_t byte_swap(int16_t n) { return byte_swap(uint16_t(n)); }

/**
 * return the parity of x, i.e. the number of 1-bits in n mod 2
 */
inline int parity(uint64_t n) { return __builtin_parityl(n); }

inline int parity(int64_t n) { return parity(uint64_t(n)); }

inline int parity(uint32_t n) { return __builtin_parity(n); }

inline int parity(int32_t n) { return parity(uint32_t(n)); }

/**
 * cache line write back, flush, flushopt
 * clwb write back a cache line to memory,
 * clflush write back a cache line to memory, meanwhile invalid the cache line
 * clflushopt is similar to clflush with some parallelism, clflush is executed one by one,
 * however clushopt don't wait the instruction's completion
 */
inline void clwb(void* p) { _mm_clwb(p); }

inline void clflush(void* p) { _mm_clflush(p); }

inline void clflushopt(void* p) { _mm_clflushopt(p); }

/** some common-used sundry functions */
inline bool aligned(void* p, int align) { return !(uint64_t(p) % align); }

template<typename T>
inline T roundup(T size, T align) {
  assert(size >= 0 && align > 0);
  return (size + align - 1) / align * align;
}

template<typename T>
inline T rounddown(T size, T align) {
  assert(size >= 0 && align > 0);
  return size / align * align;
}

template<typename T>
inline T roundup_exp2(T size, T align) {
  assert(size >= 0 && align > 0);
  assert(popcount(size_t(align)) == 1);
  return (size_t(size) + align - 1) & ~(size_t(align) - 1);
}

template<typename T>
inline T rounddown_exp2(T size, T align) {
  assert(size >= 0 && align > 0);
  assert(popcount(size_t(align)) == 1);
  return size_t(size) & ~(size_t(align) - 1);
}

inline int ncpus_online() { return sysconf(_SC_NPROCESSORS_ONLN); } /* logical cpu online */

}

#endif //UTIL_COMMON_H
