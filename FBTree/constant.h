/*
 * Copyright (c) 2022, Chen Yuan <yuan.chen@whu.edu.cn>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#ifndef INDEXRESEARCH_CONSTANT_H
#define INDEXRESEARCH_CONSTANT_H

#include <iostream>
#include <cstdint>
#include <limits>
#include "config.h"
#include "type.h"
#include "common.h"

using util::byte_swap;

namespace FeatureBTree {

template<typename T>
struct Constant;

template<>
struct Constant<String> {
  static constexpr int kInnerSize = Config::kInnerSize;
  static constexpr int kLeafSize = Config::kLeafSize;
  static constexpr int kInnerMergeSize = Config::kInnerMergeSize;
  static constexpr int kLeafMergeSize = Config::kLeafMergeSize;
  static constexpr int kFeatureSize = Config::kFeatureSize;

  static void node_parameter();
};

template<>
struct Constant<uint64_t> {
  static constexpr int kInnerSize = Config::kInnerSize;
  static constexpr int kLeafSize = Config::kLeafSize;
  static constexpr int kInnerMergeSize = Config::kInnerMergeSize;
  static constexpr int kLeafMergeSize = Config::kLeafMergeSize;
  static constexpr int kFeatureSize = sizeof(uint64_t);

  static void node_parameter();

  static inline uint64_t convert(uint64_t key) { return key; }
};

template<>
struct Constant<int64_t> {
  static constexpr int kInnerSize = Config::kInnerSize;
  static constexpr int kLeafSize = Config::kLeafSize;
  static constexpr int kInnerMergeSize = Config::kInnerMergeSize;
  static constexpr int kLeafMergeSize = Config::kLeafMergeSize;
  static constexpr int kFeatureSize = sizeof(int64_t);

  static void node_parameter();

  static inline int64_t convert(int64_t key) {
    return std::numeric_limits<int64_t>::max() + 1 + key;
  }
};

template<>
struct Constant<uint32_t> {
  static constexpr int kInnerSize = Config::kInnerSize;
  static constexpr int kLeafSize = Config::kLeafSize;
  static constexpr int kInnerMergeSize = Config::kInnerMergeSize;
  static constexpr int kLeafMergeSize = Config::kLeafMergeSize;
  static constexpr int kFeatureSize = sizeof(uint32_t);

  static void node_parameter();

  static inline uint32_t convert(uint32_t key) { return key; }
};

template<>
struct Constant<int32_t> {
  static constexpr int kInnerSize = Config::kInnerSize;
  static constexpr int kLeafSize = Config::kLeafSize;
  static constexpr int kInnerMergeSize = Config::kInnerMergeSize;
  static constexpr int kLeafMergeSize = Config::kLeafMergeSize;
  static constexpr int kFeatureSize = sizeof(int32_t);

  static void node_parameter();

  static inline int32_t convert(int32_t key) {
    return std::numeric_limits<int32_t>::max() + 1 + key;
  }
};

template<>
struct Constant<float> {
  static constexpr int kInnerSize = Config::kInnerSize;
  static constexpr int kLeafSize = Config::kLeafSize;
  static constexpr int kInnerMergeSize = Config::kInnerMergeSize;
  static constexpr int kLeafMergeSize = Config::kLeafMergeSize;
  static constexpr int kFeatureSize = sizeof(float);

  static_assert(kFeatureSize == 4);

  static void node_parameter();
};

template<>
struct Constant<double> {
  static constexpr int kInnerSize = Config::kInnerSize;
  static constexpr int kLeafSize = Config::kLeafSize;
  static constexpr int kInnerMergeSize = Config::kInnerMergeSize;
  static constexpr int kLeafMergeSize = Config::kLeafMergeSize;
  static constexpr int kFeatureSize = sizeof(double);

  static_assert(kFeatureSize == 8);

  static void node_parameter();
};

inline void Constant<String>::node_parameter() {
  std::cout << "-- node parameter: compare mode:" << compare_mode() << ", inner node size:" << kInnerSize
            << ", leaf node size:" << kLeafSize << ", inner merge size:" << kInnerMergeSize << ", leaf merge size:"
            << kLeafMergeSize << ", feature size:" << kFeatureSize << std::endl;
}

inline void Constant<uint64_t>::node_parameter() {
  std::cout << "-- node parameter: compare mode:" << compare_mode() << ", inner node size:" << kInnerSize
            << ", leaf node size:" << kLeafSize << ", inner merge size:" << kInnerMergeSize << ", leaf merge size:"
            << kLeafMergeSize << ", feature size:" << kFeatureSize << std::endl;
}

inline void Constant<int64_t>::node_parameter() {
  std::cout << "-- node parameter: compare mode:" << compare_mode() << ", inner node size:" << kInnerSize
            << ", leaf node size:" << kLeafSize << ", inner merge size:" << kInnerMergeSize << ", leaf merge size:"
            << kLeafMergeSize << ", feature size:" << kFeatureSize << std::endl;
}

inline void Constant<uint32_t>::node_parameter() {
  std::cout << "-- node parameter: compare mode:" << compare_mode() << ", inner node size:" << kInnerSize
            << ", leaf node size:" << kLeafSize << ", inner merge size:" << kInnerMergeSize << ", leaf merge size:"
            << kLeafMergeSize << ", feature size:" << kFeatureSize << std::endl;
}

inline void Constant<int32_t>::node_parameter() {
  std::cout << "-- node parameter: compare mode:" << compare_mode() << ", inner node size:" << kInnerSize
            << ", leaf node size:" << kLeafSize << ", inner merge size:" << kInnerMergeSize << ", leaf merge size:"
            << kLeafMergeSize << ", feature size:" << kFeatureSize << std::endl;
}

inline void Constant<float>::node_parameter() {
  std::cout << "-- node parameter: compare mode:" << compare_mode() << ", inner node size:" << kInnerSize
            << ", leaf node size:" << kLeafSize << ", inner merge size:" << kInnerMergeSize << ", leaf merge size:"
            << kLeafMergeSize << ", feature size:" << kFeatureSize << std::endl;
}

inline void Constant<double>::node_parameter() {
  std::cout << "-- node parameter: compare mode:" << compare_mode() << ", inner node size:" << kInnerSize
            << ", leaf node size:" << kLeafSize << ", inner merge size:" << kInnerMergeSize << ", leaf merge size:"
            << kLeafMergeSize << ", feature size:" << kFeatureSize << std::endl;
}

/* 1) all base type will be converted to unsigned type, because unsigned type
 * can be compared one byte by one byte while signed type can't, for example
 * when using 16-bits two's complement representation, -1: 0xFFFF, -2: 0xFFFE
 * 0: 0x000, 1: 0x0001, 2: 0x0002; it's easy to find that the two's complement
 * code use the overflow bit to deal with positive and negative number uniformly
 * and the size relationship of numbers are based on the remaining bits except
 * for the signed bit (in other words, the greater half of [0, 65535] is used to
 * represent negative numbers, moreover, the size relationship between positive or
 * negative numbers are same as unsigned type); here we don't care about whether
 * add and sub operations can be performed correctly, we are only concerned about
 * the size relationship of numbers, so we just need to adjust the relationship
 * between negative and positive numbers; hence, adding 2^n / 2 (where n is the bit
 * number of basic type) to signed type numbers can convert signed type to unsigned
 * type meanwhile reserving the size relationship. \n
 * 2) Based on most of the micro-architecture using little endian byte-order, we
 * assume that our code will be executed on these target machine. So in order to
 * uniform the comparison mode between integer and string, we swap the byte-order
 * of integer. \n
 * 3) Unfortunately, only avx512bw supports unsigned byte comparison while sse2 and
 * avx2 not, finally, same to item 1), we need to convert the byte encoding form for
 * compatibility, oh damn, hope everything works well. */
template<typename K>
inline K encode_convert(K key) {
  key = Constant<K>::convert(key); // encoding converting
  key = byte_swap(key); // endian swap

  // byte encoding converting, may be optimized with sse instruction set
  for(int i = 0; i < sizeof(K); i++) {
    ((char*) &key)[i] += 128;
  }

  return key;
}

template<typename K>
inline K encode_reconvert(K key) {
  // byte encoding converting, may be optimized with sse instruction set
  for(int i = 0; i < sizeof(K); i++) {
    ((char*) &key)[i] += 128;
  }

  key = byte_swap(key); // endian swap
  key = Constant<K>::convert(key); // encoding converting
  return key;
}

}

#endif //INDEXRESEARCH_CONSTANT_H
