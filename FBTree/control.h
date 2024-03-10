#ifndef INDEXRESEARCH_CONTROL_H
#define INDEXRESEARCH_CONTROL_H

#include <atomic>
#include <thread>
#include <cassert>
#include "log.h"

namespace FeatureBTree {

class Control {
  std::atomic<uint64_t> control_;

  static constexpr uint64_t kLeafBit = 0x8;     // current node is a leaf node
  static constexpr uint64_t kSiblingBit = 0x4;  // current node has sibling node
  static constexpr uint64_t kLockBit = 0x2;     // concurrency control
  static constexpr uint64_t kDelBit = 0x1;      // current node has been deleted

  static constexpr uint64_t kSplitMask = 0x0000'0000'000F'FFF0;   // current node is splitting, only used by leaf node
  static constexpr uint64_t kVersionMask = 0xFFFF'FFFF'FFF0'0000; // node version, monotone increasing

  static constexpr uint64_t kSplitOne = 0x0000'0000'0000'0010;
  static constexpr uint64_t kVersionOne = 0x0000'0000'0010'0000;

  static constexpr std::memory_order load_order = std::memory_order_acquire;
  static constexpr std::memory_order store_order = std::memory_order_release;

 public:
  Control() = delete;

  Control(bool is_leaf) : control_(is_leaf ? kLeafBit : 0) {}

  /* current node is a leaf node, also can be implemented
   * by using 1 least significant bit in pointer, such as ART */
  bool is_leaf() { return control_.load(load_order) & kLeafBit; }

  /* current node has been deleted, for inner node, next_ points to
   * its left node, for leaf node sibling_ points to its left node too */
  bool deleted() { return control_.load(load_order) & kDelBit; }

  // for inner node, it is also used to determine whether next_ is a sibling or a child
  bool has_sibling() { return control_.load(load_order) & kSiblingBit; }

  /* current node is splitting, only used by leaf node it means
   * that the btree is in an inconsistent state, we need to check
   * the high key on leaf node to determine whether we need to
   * jump to its sibling leaf node */
  bool is_splitting() { return control_.load(load_order) & kSplitMask; }

  uint64_t load_version() { return control_.load(load_order) & kVersionMask; }

  uint64_t begin_read() {
    // begin atomic reading node, if locked, waiting for other thread
    while(true) {
      uint64_t control = control_.load(load_order);
      if((control & kLockBit) == 0) {
        return control & kVersionMask;
      }

      // waiting for other threads' modification
      using namespace std::chrono_literals;
      std::this_thread::sleep_for(1us);
    }
  }

  bool end_read(uint64_t version) {
    // end atomic reading node, if locked or version has changed, retry
    uint64_t control = control_.load(load_order);
    if((control & kLockBit) != 0 ||
       (control & kVersionMask) != version)
      return false;

    return true;
  }

  void begin_splitting() {
    uint64_t old = control_.fetch_add(kSplitOne);
    CONDITION_ERROR((old & kSplitMask) == kSplitMask, "fatal error, split token overflow!");
  }

  void end_splitting() {
    uint64_t old = control_.fetch_sub(kSplitOne);
    CONDITION_ERROR((old & kSplitMask) == 0, "fatal error, split token under flow!");
  }

  void set_delete() {
    uint64_t old = control_.fetch_add(kDelBit);
    CONDITION_ERROR((old & kDelBit) != 0, "fatal error, delete a node that had been deleted!");
  }

  void set_sibling() {
    uint64_t old = control_.fetch_add(kSiblingBit);
    CONDITION_ERROR((old & kSiblingBit) != 0, "fatal error, current node already has sibling!");
  }

  void clear_sibling() {
    uint64_t old = control_.fetch_sub(kSiblingBit);
    CONDITION_ERROR((old & kSiblingBit) == 0, "fatal error, current node doesn't have sibling!");
  }

  void latch_exclusive() {
    while(true) {
      // using backoff, so reload control before cas
      uint64_t expected = control_.load(load_order);
      uint64_t desired = expected | kLockBit;
      if((expected & kLockBit) == 0 &&
         control_.compare_exchange_strong(expected, desired))
        break;
      using namespace std::chrono_literals;
      std::this_thread::sleep_for(1us);
    }
  }

  void update_version() {
    control_.fetch_add(kVersionOne);
  }

  void unlatch_exclusive() {
    assert(control_.load(load_order) & kLockBit);
    control_.fetch_sub(kLockBit);
  }


};

static_assert(sizeof(Control) == 8);

}

#endif //INDEXRESEARCH_CONTROL_H
