#ifndef UTIL_EPOCH_H
#define UTIL_EPOCH_H

#include <atomic>
#include <functional>
#include <tbb/enumerable_thread_specific.h>
#include "common.h"

namespace util {

namespace util_epoch {

struct EpochConfig {
  static constexpr uint64_t kEpochIncrement = 2; // least significant bit is used as quiescent token
  static constexpr int kOpThreshold = 10;        /* the number of operations before trying to check
                                                  * whether we need to advance the global epoch */
  static constexpr int kBagSize = 13;            // Limbo Bag size
};

// current thread is quiescent
inline bool is_quiescent(uint64_t epoch) { return epoch & 0x01ul; }

inline uint64_t with_quiescent(uint64_t epoch) { return epoch | 0x01ul; }

inline uint64_t real_epoch(uint64_t epoch) { return epoch & ~0x01ul; }

struct alignas(64) LimboBag {
  uint64_t epoch;  // the max epoch before which the memory blocks in current limbo bag was retired
  int count;       // memory blocks count
  int freed;       // the number of released memory block
  LimboBag* next;  // next limbo bag
  void* pointers[EpochConfig::kBagSize];  // memory blocks need to be deallocated
};

static_assert(sizeof(LimboBag) == 128);


// type erased LimboBag, for customized memory blocks release
struct alignas(64) TypeLimboBag {
  uint64_t epoch;
  int count;
  int finished;
  TypeLimboBag* next;
  std::function<void()> funcs[EpochConfig::kBagSize];
};

static_assert(sizeof(TypeLimboBag) == 448);


// memory allocator abstraction
class Allocator {
 public:
  void* aligned_alloc(size_t align, size_t len) {
    return std::aligned_alloc(align, len);
  }

  void* malloc(size_t len) {
    return std::malloc(len);
  }

  void free(void* p) {
    std::free(p);
  }
};

static_assert(sizeof(Allocator) == 1);


class alignas(64) LimboList {
 public: //place the common accessed var at head
  bool free_token;                    // need to try free opeartion
  bool type_token;
  int opcount;                        // operation count
  std::atomic<uint64_t> thread_epoch; // thread epoch which can be loaded by other thread
  uint64_t local_epoch;               // thread local epoch read by itself

 private:
  LimboBag* retired_head_;  // memory blocks which is retired, need to be released
  LimboBag* retired_tail_;
  LimboBag* empty_bag_;     // empty LimboBag list

  TypeLimboBag* type_head_;
  TypeLimboBag* type_tail_;
  TypeLimboBag* type_bag_;

 public:
  tbb::enumerable_thread_specific<LimboList>::iterator checked_it; // LimboList iterator that has been checked

  using alloc = Allocator; // default memory allocator

 public:
  LimboList() : retired_head_(nullptr), retired_tail_(nullptr), empty_bag_(nullptr), free_token(false),
                type_head_(nullptr), type_tail_(nullptr), type_bag_(nullptr), type_token(false),
                thread_epoch(with_quiescent(0)), local_epoch(with_quiescent(0)), opcount(0) {}

  ~LimboList() {
    // destroy LimboBags
    LimboBag* limbo = retired_head_;
    while(limbo != nullptr) {
      while(limbo->freed < limbo->count) {
        alloc().free(limbo->pointers[limbo->freed++]);
      }
      retired_head_ = limbo->next;
      alloc().free(limbo);
      limbo = retired_head_;
    }

    limbo = empty_bag_;
    while(limbo != nullptr) {
      empty_bag_ = limbo->next;
      alloc().free(limbo);
      limbo = empty_bag_;
    }

    // destroy TypeLimboBags
    TypeLimboBag* type_limbo = type_head_;
    while(type_limbo != nullptr) {
      while(type_limbo->finished < type_limbo->count) {
        type_limbo->funcs[type_limbo->finished++]();
      }
      type_head_ = type_limbo->next;
      type_limbo->~TypeLimboBag();
      alloc().free(type_limbo);
      type_limbo = type_head_;
    }

    type_limbo = type_bag_;
    while(type_limbo != nullptr) {
      type_bag_ = type_limbo->next;
      type_limbo->~TypeLimboBag();
      alloc().free(type_limbo);
      type_limbo = type_bag_;
    }
  }

  void add(void* p) {
    LimboBag* limbo = retired_tail_;
    bool init = (limbo == nullptr);
    if(init || limbo->count == EpochConfig::kBagSize) {
      if(empty_bag_ == nullptr) {
        limbo = (LimboBag*) alloc().malloc(sizeof(LimboBag));
      } else {
        limbo = empty_bag_;
        empty_bag_ = empty_bag_->next;
      }
      limbo->next = nullptr;
      limbo->freed = 0;
      limbo->count = 0;
      if(init) { retired_head_ = limbo; }
      else { retired_tail_->next = limbo; }
      retired_tail_ = limbo;
    }

    limbo->pointers[limbo->count] = p;
    limbo->count++;
    limbo->epoch = local_epoch;
  }

  void add(std::function<void()>&& func) {
    TypeLimboBag* limbo = type_tail_;
    bool init = (limbo == nullptr);
    if(init || limbo->count == EpochConfig::kBagSize) {
      if(type_bag_ == nullptr) {
        limbo = (TypeLimboBag*) alloc().malloc(sizeof(TypeLimboBag));
        new(limbo) TypeLimboBag;
      } else {
        limbo = type_bag_;
        type_bag_ = type_bag_->next;
      }
      limbo->next = nullptr;
      limbo->finished = 0;
      limbo->count = 0;
      if(init) { type_head_ = limbo; }
      else { type_tail_->next = limbo; }
      type_tail_ = limbo;
    }

    limbo->funcs[limbo->count] = std::move(func);
    limbo->count++;
    limbo->epoch = local_epoch;
  }

  void free(uint64_t gepoch, int nfree) { // nfree: the number of memory blocks to be released
    assert(nfree > 0 && nfree <= EpochConfig::kBagSize);
    LimboBag* cur_bag = retired_head_;
    if(free_token && cur_bag != retired_tail_) {  // retired_tail_ is current limbobag
      if(gepoch - cur_bag->epoch >= 2 * EpochConfig::kEpochIncrement) {  // free blocks retired two epochs ago
        int freed = 0;
        while(freed++ < nfree && cur_bag->freed < cur_bag->count) {
          alloc().free(cur_bag->pointers[cur_bag->freed++]);
        }
        if(cur_bag->freed >= cur_bag->count) { // cur_bag is empty
          retired_head_ = cur_bag->next;
          cur_bag->next = empty_bag_;
          empty_bag_ = cur_bag;
        }
      } else { free_token = false; }
    } else { free_token = false; }
  }

  void type_free(uint64_t gepoch, int nfree) {
    assert(nfree > 0 && nfree <= EpochConfig::kBagSize);
    TypeLimboBag* cur_bag = type_head_;
    if(type_token && cur_bag != type_tail_) {
      if(gepoch - cur_bag->epoch >= 2 * EpochConfig::kEpochIncrement) {
        int finished = 0;
        while(finished++ < nfree && cur_bag->finished < cur_bag->count) {
          cur_bag->funcs[cur_bag->finished++]();
        }
        if(cur_bag->finished >= cur_bag->count) {
          type_head_ = cur_bag->next;
          cur_bag->next = type_bag_;
          type_bag_ = cur_bag;
        }
      } else { type_token = false; }
    } else { type_token = false; }
  }
};

static_assert(sizeof(LimboList) == 128);


class alignas(64) Epoch {
  std::atomic<uint64_t> gepoch_;  // global epoch
  uint64_t __padding[7];          // padding preventing false sharing
  tbb::enumerable_thread_specific<LimboList> limbo_lists_;  // thread local limbo list
 public:
  Epoch() : gepoch_(0) {}

  /* batch free may be harmful when releasing memory blocks allocated by other threads;
   * to jemalloc, when the bin of a size_class in tcache is full, some memory blocks need
   * to be purged into arena, which process need lock operation, resulting in contention */
  void startop(int nfree = EpochConfig::kBagSize) { // nfree: the number of memory blocks to be released
    // create limbo list for current thread, and make limbolist static for reducing looking overhead
    static thread_local LimboList& limbolist = limbo_lists_.local();
    uint64_t gepoch = gepoch_.load(std::memory_order_seq_cst);
    uint64_t lepoch = limbolist.local_epoch;
    limbolist.local_epoch = gepoch;

    // if global epoch has been advanced
    if(gepoch != lepoch) {  // also when initialization,
      limbolist.checked_it = limbo_lists_.begin();
      limbolist.free_token = true, limbolist.type_token = true;
    }

    // reclaim any objects retired two epochs ago.
    limbolist.free(gepoch, nfree);
    limbolist.type_free(gepoch, nfree);

    barrier();  // compiler fence, preventing this store operation is compiled reordered before reclaim
    limbolist.thread_epoch.store(gepoch, std::memory_order_relaxed);/*note: atomic store only guarantee
    * atomicity, don't guarantee this store operation can be observed instantly by other threads, it just
    * will be compiled as thread_epoch = gepoch, please refer to cppreference for more details */
    barrier();

    // incrementally scan the announced epochs of all threads
    if(++limbolist.opcount == EpochConfig::kOpThreshold) {
      limbolist.opcount = 0;
      LimboList& olimbo = *limbolist.checked_it;
      uint64_t tepoch = olimbo.thread_epoch.load(std::memory_order_relaxed);
      if(real_epoch(tepoch) == gepoch || is_quiescent(tepoch)) {
        auto it = ++limbolist.checked_it;
        if(it == limbo_lists_.end()) {
          gepoch_.compare_exchange_strong(gepoch, gepoch + EpochConfig::kEpochIncrement);
        }
      }
    }
  }

  // normal retire, only free the memory block, without taking obj destruct into account
  void retire(void* p) {
    static thread_local LimboList& limbolist = limbo_lists_.local();
    if(p != nullptr) limbolist.add(p);
  }

  // type erased retire, for customized memory blocks release
  // Example: retire([p](){delete p});
  void retire(std::function<void()>&& func) {
    static thread_local LimboList& limbolist = limbo_lists_.local();
    limbolist.add(std::move(func));
  }

  bool guarded() {
    static thread_local LimboList& limbolist = limbo_lists_.local();
    return !is_quiescent(limbolist.thread_epoch.load(std::memory_order_relaxed));
  }

  void endop() {
    static thread_local LimboList& limbolist = limbo_lists_.local();
    limbolist.thread_epoch.store(with_quiescent(limbolist.local_epoch),
                                 std::memory_order_relaxed);
  }

  void clear() { // concurrency unsafe
    gepoch_.store(0, std::memory_order_relaxed);
    limbo_lists_.clear();
  }

  void* aligned_alloc(size_t align, size_t len) {
    return LimboList::alloc().aligned_alloc(align, len);
  }

  void* malloc(size_t len) {
    return LimboList::alloc().malloc(len);
  }

  void free(void* p) {
    LimboList::alloc().free(p);
  }
};

class EpochGuard {
  Epoch& epoch_;

 public:
  EpochGuard() = delete;

  EpochGuard(Epoch& epoch, int nfree) : epoch_(epoch) { epoch.startop(nfree); }

  EpochGuard(Epoch& epoch) : EpochGuard(epoch, EpochConfig::kBagSize) {}

  // normal retire, only free the memory block, without taking obj destruct into account
  void retire(void* p) { epoch_.retire(p); }

  // type erased retire, for customized memory blocks release
  // Example: retire([p](){delete p});
  void retire(std::function<void()>&& func) { epoch_.retire(std::move(func)); }

  ~EpochGuard() { epoch_.endop(); }
};

}

using util_epoch::Epoch;
using util_epoch::EpochGuard;

}

#endif //UTIL_EPOCH_H
