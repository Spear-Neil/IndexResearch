#ifndef TREERESEARCH_MASSTREEWRAPPER_H
#define TREERESEARCH_MASSTREEWRAPPER_H

#include <tbb/enumerable_thread_specific.h>
#include "masstree.hh"
#include "kvthread.hh"
#include "masstree_struct.hh"
#include "masstree_insert.hh"
#include "masstree_get.hh"
#include "masstree_tcursor.hh"
#include "masstree_remove.hh"


inline thread_local threadinfo* mass_thd_info = nullptr;

struct MassEpochGuard {
  MassEpochGuard() {
    mass_thd_info->rcu_start();
  }

  ~MassEpochGuard() {
    mass_thd_info->rcu_stop();
  }
};

struct NormalValueTag {}; // value: basic type, i.e. int32, uint32, int64, uint64
struct PointerValueTag {}; // value type: pointer

template<typename V>
struct ValueTagTraits {
  typedef NormalValueTag vtag;
};

template<typename V>
struct ValueTagTraits<V*> {
  typedef PointerValueTag vtag;
};

/* typename V must be basic type or pointer */
template<typename V>
class MassTreeWrapper {
  struct NodeParam : Masstree::nodeparams<> {
    typedef V value_type;
    typedef threadinfo threadinfo_type;
  };

  typedef Masstree::basic_table<NodeParam> Tree_t;
  typedef typename Masstree::basic_table<NodeParam>::cursor_type locked_cursor_t;
  typedef typename Masstree::basic_table<NodeParam>::unlocked_cursor_type unlocked_cursor_t;
  typedef typename ValueTagTraits<V>::vtag vtag;

  Tree_t tree_;

 private:
  void retire(V value, NormalValueTag) {}

  void retire(V value, PointerValueTag) {
    mass_thd_info->deallocate_rcu(value, 0, memtag_value);
  }

 public:
  MassTreeWrapper() {
    if(!mass_thd_info) {
      mass_thd_info = threadinfo::make(threadinfo::TI_MAIN, -1);
    }
    tree_.initialize(*mass_thd_info);
  }

  void thread_init(int tid) {
    if(!mass_thd_info) {
      mass_thd_info = threadinfo::make(threadinfo::TI_PROCESS, tid);
    }
  }

  /* return false means the key has existed(update corresponding
   * value), otherwise successfully insert the key */
  bool upsert(char* key, int klen, V value) {
    mass_thd_info->rcu_start();
    locked_cursor_t lp(tree_, key, klen);
    bool find = lp.find_insert(*mass_thd_info);
    if(find) {
      retire(lp.value(), vtag());
    }
    lp.value() = value;
    lp.finish(1, *mass_thd_info);
    mass_thd_info->rcu_stop();

    return !find;
  }

  bool upsert(std::string& key, V value) {
    return upsert(key.data(), key.size(), value);
  }

  bool upsert(std::string&& key, V value) {
    return upsert(key.data(), key.size(), value);
  }

  bool search(char* key, int klen, V& value) {
    unlocked_cursor_t lp(tree_, key, klen);
    bool find = lp.find_unlocked(*mass_thd_info);
    if(find) value = lp.value();
    return find;
  }

  bool search(std::string& key, V& value) {
    return search(key.data(), key.size(), value);
  }

  bool search(std::string&& key, V& value) {
    return search(key.data(), key.size(), value);
  }
};

#endif //TREERESEARCH_MASSTREEWRAPPER_H
