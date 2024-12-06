/*
 * Copyright (c) 2022-Present, Chen Yuan <yuan.chen@whu.edu.cn>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#ifndef B_LINK_TREE_LATCH_H
#define B_LINK_TREE_LATCH_H

#include <atomic>
#include <shared_mutex>
#include <thread>
#include <chrono>
#include <cassert>
#include <stdlib.h>

class StandardSharedMutex {
 private:
  std::shared_mutex shared_mutex_;

 public:
  ~StandardSharedMutex() {}

  void latch_shared() {
    shared_mutex_.lock_shared();
  }

  void unlatch_shared() {
    shared_mutex_.unlock_shared();
  }

  void latch_exclusive() {
    shared_mutex_.lock();
  }

  void unlatch_exclusive() {
    shared_mutex_.unlock();
  }
};


class alignas(64) RSpinSharedLatch {  // reader-first
 private:
  bool delay_;
  int w_reader_limit_;  //if the number readers_ is more than limit, delay
  int r_spin_limit_;    //if latch_shared spin more than limit times, delay
  std::atomic<uint64_t> flag_;  // the most significant bit denotes w_latch, the other bits denote reader cnt

 private:
  bool w_latched(uint64_t flag) {
    return ((flag >> 63) == 1);
  }

  uint64_t reader_cnt(uint64_t flag) {
    return flag & (~(1ul << 63));
  }

  bool none_latched(uint64_t flag) {
    return flag == 0;
  }

 public:
  RSpinSharedLatch(bool delay = true, int w_limit = 4, int r_limit = 1) :
    delay_(delay), w_reader_limit_(w_limit), r_spin_limit_(r_limit), flag_(0) {}

  ~RSpinSharedLatch() {}

  void latch_shared() {
    static thread_local int spin_count = 0;

    while(true) {
      uint64_t flag = flag_.fetch_add(1);
      if(w_latched(flag)) {
        flag_.fetch_sub(1);
        spin_count++;

        if(delay_ && (spin_count > r_spin_limit_)) {
          using namespace std::chrono_literals;
          std::this_thread::sleep_for(1us);
        }
      } else {
        spin_count = 0;
        break;
      }
    }
  }

  void unlatch_shared() {
    flag_.fetch_sub(1);
  }

  void latch_exclusive() {
    constexpr uint64_t w_latch = (1ul) << 63u;

    while(true) {
      uint64_t flag = flag_.load();

      if(none_latched(flag) && flag_.compare_exchange_strong(flag, w_latch)) {
        break;
      }

      if(delay_ && (w_latched(flag) || reader_cnt(flag) > w_reader_limit_)) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1us);
      }
    }
  }

  void unlatch_exclusive() {
    constexpr uint64_t w_latch = (1ul) << 63u;
    uint64_t flag = flag_.fetch_sub(w_latch);
    assert(w_latched(flag));
  }
};

static_assert(sizeof(RSpinSharedLatch) == 64);

#endif //B_LINK_TREE_LATCH_H
