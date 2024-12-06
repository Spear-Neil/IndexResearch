/*
 * Copyright (c) 2022-Present, Chen Yuan <yuan.chen@whu.edu.cn>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#ifndef INDEXRESEARCH_CONFIG_H
#define INDEXRESEARCH_CONFIG_H

#include <string>

namespace FeatureBTree {

/** For string keys, kFeatureSize = 8, kInnerSize = 32 seems to be a better config, cause it may mitigate the probability
  * of suffix binary search. However, it also makes the tree 1 level deeper than that of kInnerSize = 64, end up with
  * almost the same overhead. A larger kFeatureSize may make sense in concurrent environment (better memory bandwidth
  * utilization). But it may bring additional overhead in single-threaded environment because the latency increases when
  * accessing a larger memory block. Due to hardware prefetch/memory-level parallelism, on our Intel Xeon Gold 6248R CPU
  * and Core 12700KF CPU, read sequential 64/128/256 bytes have almost the same latency. For binary keys, kInnerSize = 64
  * is a better config. We thus config kFeatureSize = 4, kInnerSize = 64 for better single-threaded throughput and space
  * efficiency in our experimental evaluation.
  *
  * All these configs can be configured independently in constant.h for different key types.
  * */

enum CompareMode { SIMD512, SIMD256, SIMD128 };

struct Config {
  /* manipulation mode of feature comparison and fingerprint comparison */
  static constexpr CompareMode kCmpMode = SIMD256;
  /* the size of feature in inner node 0,1,2,3 ... , only valid for
   * string key, the feature size of basic type key is fixed */
  static constexpr int kFeatureSize = 4;
  /* the number of keys in inner/leaf node, 16/32/64 */
  static constexpr int kInnerSize = 64;
  static constexpr int kLeafSize = 64;
  /* the merge threshold, if the number of keys in current node
   * and its sibling node is lss than MERGE_SIZE, merge the two node */
  static constexpr int kInnerMergeSize = kInnerSize / 2;
  static constexpr int kLeafMergeSize = kLeafSize / 2;
  /* the memory alignment requirement of inner node and leaf node */
  static constexpr int kAlignSize = 32;
  /* prefetch inner node and leaf node before access node */
  static constexpr bool kNodePrefetch = true;
  /* node prefetch size, default 4 cache line (for string key) */
  static constexpr int kPrefetchSize = 4;
  /* backoff of CAS, spin n times before backoff, spin kSpinInit times
   * at first, then spin kSpinInit + kSpinInc * times of backoff; spin
   * kSpinInit at first to ensure there is heavy contention, increase
   * kSpinInc times to ensure a thread does not wait too long */
  static constexpr int kSpinInit = 3, kSpinInc = 2;
  /* store anchors in contiguous memory blocks, not scattered */
  static constexpr bool kExtentOpt = false;
  /* the initial extent size, valid if kExtentOpt(true) */
  static constexpr int kExtentSize = 2048;
};

inline std::string compare_mode() {
  switch(Config::kCmpMode) {
    case SIMD512:
      return "simd512";
    case SIMD256:
      return "simd256";
    case SIMD128:
      return "simd128";
  }
}

static_assert(Config::kFeatureSize > 0);

static_assert((Config::kInnerSize == 16 && Config::kCmpMode == SIMD128)
              || (Config::kInnerSize == 32 && Config::kCmpMode != SIMD512)
              || Config::kInnerSize == 64);

static_assert((Config::kLeafSize == 16 && Config::kCmpMode == SIMD128)
              || (Config::kLeafSize == 32 && Config::kCmpMode != SIMD512)
              || Config::kLeafSize == 64);

static_assert(Config::kInnerMergeSize > 0 &&
              Config::kInnerMergeSize < Config::kInnerSize);

static_assert(Config::kLeafMergeSize > 0 &&
              Config::kLeafMergeSize < Config::kLeafSize);

static_assert(Config::kAlignSize == 32 || Config::kAlignSize == 64);

static_assert(Config::kExtentSize % 2048 == 0);

#ifndef AVX512BW_ENABLE
static_assert(Config::kCmpMode != SIMD512);
#endif

#ifndef AVX2_ENABLE
static_assert(Config::kCmpMode != SIMD256);
#endif

#ifndef SSE2_ENABLE
static_assert(Config::kCmpMode != SIMD128);
#endif

}

#endif //INDEXRESEARCH_CONFIG_H
