#include <iostream>
#include <random>
#include <vector>
#include <thread>
#include "util.h"

using namespace util;

typedef KVPair<uint64_t, uint64_t> KVType;
constexpr int kNodeSize = 128;
constexpr int kMaxScanLen = 100;
constexpr bool kReadFiled = true;
static_assert(kMaxScanLen <= kNodeSize);

class NodeBase {
 public:
  virtual int scan(int len) = 0;
};

class InorderNode : public NodeBase {
  KVType* kvs_[kNodeSize];

 public:
  InorderNode() {
    RandomEngine engine(std::random_device{}());
    for(int i = 0; i < kNodeSize; i++) {
      kvs_[i] = new KVType{engine(), engine()};
    }
  }

  int scan(int len) override {
    int sum = 0;
    for(int i = 0; i < len; i++) {
      if(kReadFiled) sum += kvs_[i]->value;
      else sum += (uint64_t) kvs_[i];
    }
    return sum;
  }
};


class IndirectNode : public NodeBase {
  KVType* kvs_[kNodeSize];
  uint8_t seq_[kNodeSize];

 public:
  IndirectNode() {
    RandomEngine engine(std::random_device{}());
    for(int i = 0; i < kNodeSize; i++) {
      kvs_[i] = new KVType{engine(), engine()};
      seq_[i] = i;
    }
    std::shuffle(seq_, seq_ + kNodeSize, std::mt19937(std::random_device()()));
  }

  int scan(int len) override {
    int sum = 0;
    for(int i = 0; i < len; i++) {
      if(kReadFiled) sum += kvs_[seq_[i]]->value;
      else sum += (uint64_t) kvs_[seq_[i]];
    }
    return sum;
  }
};


/** Test Procedure for scan simulation in ordered (FB+-tree) and indirect (wormhole) leaf nodes
 *  Similar to YCSB-E workload: requests follow zipfian distribution,
 *                              scan length follows uniform distribution
 *                              max scan length: 100, (100% scan)
 * */

int main(int argc, char* argv[]) {
  if(argc < 6) {
    std::cerr << GRAPH_FONT_YELLOW << "[PARAMETER]: node count, operation count, node type, thread number"
                                      ", run time (second)" << GRAPH_ATTR_NONE << std::endl;
    exit(-1);
  }
  size_t node_cnt = std::stoul(argv[1]);
  size_t req_cnt = std::stoul(argv[2]);
  int node_type = std::stoi(argv[3]);
  int nthd = std::stoi(argv[4]);
  int run_time = std::stoi(argv[5]);

  std::cout << "[Info]: " << (node_type ? "IndirectNode, " : "InorderNode, ") << node_cnt << " nodes, "
            << req_cnt << " operations, " << run_time << " seconds, " << nthd << " threads" << std::endl;

  PinningMap pin;
  pin.pinning_thread(0,0, pthread_self());
  std::cout << GRAPH_FONT_GREEN << "[Info]: node generation ... " << GRAPH_ATTR_NONE << std::endl;
  NodeBase** nodes = new NodeBase* [node_cnt];
  if(node_type) {
    for(size_t i = 0; i < node_cnt; i++) {
      nodes[i] = new IndirectNode;
    }
  } else {
    for(size_t i = 0; i < node_cnt; i++) {
      nodes[i] = new InorderNode;
    }
  }

  std::cout << GRAPH_FONT_GREEN << "[Info]: request generation ... " << GRAPH_ATTR_NONE << std::endl;
  std::vector<std::pair<size_t, int>> reqs;
  reqs.reserve(req_cnt);
  ZipfGenerator<size_t> req_gen(0, node_cnt);
  UnifGenerator<int> len_gen(1, kMaxScanLen);
  for(size_t i = 0; i < req_cnt; i++) {
    reqs.emplace_back(req_gen(), len_gen());
  }

  std::cout << GRAPH_FONT_GREEN << "[Info]: test start ... " << GRAPH_ATTR_NONE << std::endl;
  std::vector<std::thread> workers;
  double throughput[nthd], total_tpt = 0;
  size_t nosense = 0;
  for(int tid = 0; tid < nthd; tid++) {
    workers.push_back(std::thread([&](int tid) {
      pin.pinning_thread_continuous(pthread_self());
      size_t start = reqs.size() * tid / nthd, sum = 0, cnt = 0;
      int rng = reqs.size() * (tid + 1) / nthd - start;
      Timer timer;
      timer.start();
      while(true) {
        size_t req_id = cnt % rng + start;
        auto [rec_id, scan_len] = reqs[req_id];
        sum += nodes[rec_id]->scan(scan_len);
        if(cnt++ % 10000 == 0 && timer.duration_s() >= run_time) break;
      }
      long dur = timer.duration_us();
      nosense += sum;
      throughput[tid] = double(cnt) / dur;
    }, tid));
  }

  for(int tid = 0; tid < nthd; tid++) {
    workers[tid].join();
    total_tpt += throughput[tid];
  }

  std::cout << "[Info]: no sense: " << nosense << std::endl;
  std::cout << "[Info]: Throughput: " << total_tpt << " Mops" << std::endl;
  return 0;
}