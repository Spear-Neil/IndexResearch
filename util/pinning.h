#ifndef UTIL_PINNING_H
#define UTIL_PINNING_H

#include <iostream>
#include <fstream>
#include <numa.h>
#include <pthread.h>
#include <pthread.h>
#include <atomic>
#include <vector>
#include <string>
#include <sstream>
#include <set>

#include "strutil.h"

namespace util {

class PinningMap {
 private:
  int thread_num_; // the number of physical thread supported by the machine
  std::atomic<int> pinning_counter_;
  std::vector<std::vector<int>> thread_map_;
  // logical thread to physical thread map, from lower numa node to higher numa node

 private:
  void arg_check(int socket_id, int tid) {
    bool invalid = false;
    if(socket_id >= thread_map_.size()) {
      std::cerr << "ERROR: socket id must be less than socket num" << std::endl;
      invalid = true;
    }
    if(tid >= thread_map_[socket_id].size()) {
      std::cerr << "ERROR: thread id must be less than thread per socket" << std::endl;
      invalid = true;
    }
    if(invalid) exit(1);
  }

 public:
  PinningMap() {
    std::fstream cpuinfo("/proc/cpuinfo", std::ios_base::in);
    if(!cpuinfo.is_open()) {
      std::cout << "/proc/cpuinfo open error" << std::endl;
      exit(-1);
    }
    std::string line;
    std::vector<int> processor;
    std::vector<int> physical_id;
    std::set<int> physical_set;

    while(getline(cpuinfo, line)) {
      if(line.size() > 0) {
        auto&& substring = string_split(line, ':');
        if(substring[0] == "processor\t")
          processor.push_back(std::stoi(substring[1].substr(1)));
        if(substring[0] == "physical id\t") {
          int pid = std::stoi(substring[1].substr(1));
          physical_id.push_back(pid);
          physical_set.insert(pid);
        }
      }
    }

    if(processor.size() != physical_id.size()) {
      std::cerr << "fatal unknown error, termination!" << std::endl;
      exit(-1);
    }

    thread_num_ = processor.size();
    pinning_counter_.store(0);
    for(int i = 0; i < physical_set.size(); i++)
      thread_map_.push_back(std::vector<int>());

    std::vector<int>::iterator it1 = processor.begin();
    std::vector<int>::iterator it2 = physical_id.begin();
    for(; it1 != processor.end(); it1++, it2++) {
      thread_map_[*it2].push_back(*it1);
    }
  }

  int pinning_thread(int socket_id, int tid, pthread_t pthread) {
    //socket_id:numa node id or the real cpu id; tid: logical thread id mapping to a socket
    arg_check(socket_id, tid);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread_map_[socket_id][tid], &cpuset);
    return pthread_setaffinity_np(pthread, sizeof(cpu_set_t), &cpuset);
  }

  //reset pinning_counter_ as n ( socket_id * thread_per_socket_ + tid ), so function
  //pinning_thread_continuous(pthread_t pthread) starts pinning from logical thread id n
  void reset_pinning_counter(int socket_id = 0, int tid = 0) {
    arg_check(socket_id, tid);
    int thread_count = 0;
    for(int id = 0; id < socket_id; id++)
      thread_count += thread_map_[id].size();
    pinning_counter_ = thread_count + tid;
  }

  int pinning_thread_continuous(pthread_t pthread) {
    //pinning logical threads to hardware threads within one socket if the socket
    //have any other hardware threads haven't been pinned to, otherwise pinning threads to
    //the next socket
    int tid = (pinning_counter_++) % thread_num_;
    int socket_id = 0;
    while(thread_map_[socket_id].size() <= tid) {
      tid -= thread_map_[socket_id].size();
      socket_id++;
    }

    arg_check(socket_id, tid);

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread_map_[socket_id][tid], &cpuset);
    int ret = pthread_setaffinity_np(pthread, sizeof(cpu_set_t), &cpuset);
    return ret;
  }

  // the max hardware thread number of current machine supported
  int concurrency_number() {
    return thread_num_;
  }

  // numa node
  int numa_number() {
    return thread_map_.size();
  }

  void numa_set_localalloc() {
    if(numa_available() >= 0)
      ::numa_set_localalloc();
  }
};

}

#endif //UTIL_PINNING_H
