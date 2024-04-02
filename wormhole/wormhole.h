#ifndef TREERESEARCH_WORMHOLE_H
#define TREERESEARCH_WORMHOLE_H

#include "lib.h"
#include "kv.h"
#include "wh.h"
#include "common.h"

inline thread_local wormref* whref;

using util::byte_swap;

class WormHole {
  wormhole* wh;

 public:
  WormHole() { wh = wh_create(); }

  ~WormHole() { wh_destroy(wh); }

  void thread_init() { whref = wh_ref(wh); }

  void thread_end() { wh_unref(whref); }

  bool upsert(char* key, int klen, void* value) {
    return wh_put(whref, key, klen, &value, sizeof(void*));
  }

  bool upsert(uint64_t key, void* value) {
    key = byte_swap(key);
    return wh_put(whref, &key, sizeof(key), &value, sizeof(void*));
  }

  bool upsert(uint32_t key, void* value) {
    key = byte_swap(key);
    return wh_put(whref, &key, sizeof(key), &value, sizeof(void*));
  }

  bool search(char* key, int klen, void*& value) {
    uint32_t vlen;
    return wh_get(whref, key, klen, &value, sizeof(void*), &vlen);
  }

  bool search(uint64_t key, void*& value) {
    key = byte_swap(key);
    uint32_t vlen;
    return wh_get(whref, &key, sizeof(key), &value, sizeof(void*), &vlen);
  }

  bool search(uint32_t key, void*& value) {
    key = byte_swap(key);
    uint32_t vlen;
    return wh_get(whref, &key, sizeof(key), &value, sizeof(void*), &vlen);
  }

  bool remove(char* key, int klen) {
    return wh_del(whref, key, klen);
  }

  bool remove(uint64_t key) {
    key = byte_swap(key);
    return wh_del(whref, &key, sizeof(key));
  }

  bool remove(uint32_t key) {
    key = byte_swap(key);
    return wh_del(whref, &key, sizeof(key));
  }
};

#endif //TREERESEARCH_WORMHOLE_H
