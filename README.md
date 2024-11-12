# FB+-tree
In essence, FB+-tree is identical to a main-memory B+-tree except for the layout of inner node and leaf node.
Similar to employing bit or byte for branch operation in tries, FB+-tree progressively considers several bytes 
following the common prefix on each level of inner nodes, referred to as features. By incorporating features, 
FB+-tree blurs the lines between B+-trees and tries, allowing FB+-tree to benefit from prefix skewness. In the
best case, FB+-tree almost becomes a trie, whereas in the worst case, it continues to function as a B+-tree. 
In most cases, branch operations can be achieved by feature comparison thus enhancing its cache consciousness.
Furthermore, feature comparison is implemented with SIMD instructions in a simple for loop, which mitigates 
dependences between instructions in comparison to binary search, allowing FB+-tree to leverage computational 
and memory-level parallelism, such as, super-scalar, dynamic branch prediction, speculative execution and a 
series of out-of-order execution techniques.

In concurrent environments, with feature comparison, FB+-tree effectively alleviates small and random memory 
access generated by binary search, thus significantly improving the utilization of memory bandwidth and Ultra
Path interconnect (UPI) bandwidth. Eventually, FB+-tree are more multi-core scalable than typical B+-tree even
on read-only workloads. Unlike typical B+-trees that copy anchor keys into inner nodes, FB+-tree stores the actual
contents of anchor keys in leaf nodes (i.e., high_key, the upper bound), while inner nodes only maintain pointers
to high_key, which makes FB+-tree more space-efficient. Since high_key only represents the upper bound of a leaf
node, it can be constructed using discriminative prefixes to improve performance and space consumption.

# Synchronization Protocol
FB+-tree employs a highly optimized optimistic synchronization protocol for concurrent index access.
It highlights:
* latch-free index traversal, without the comparison overhead with high_key in most cases
* **highly scalable latch-free update (subtle atomic operations coordinated with optimistic lock)**
* concurrent linearizable range scan on linked list of leaf nodes, and lazy rearrangement

# Index Structures
There is an example in each index directory. 
* [ARTOLC](https://github.com/wangziqi2016/index-microbench.git)
* BLinkTree: lock-based B-link-tree, implemented based on the paper by YAO. et al. just a demo, may have some bugs
* [B+-treeOLC](https://github.com/wangziqi2016/index-microbench.git)
* [FAST](https://github.com/RyanMarcus/fast64.git): a simple implementation of FAST in Rust, only support bulk_load.
* FB+-tree
* [GoogleBtree](https://code.google.com/archive/p/cpp-btree/)
* [HOT](https://github.com/speedskater/hot.git)
* [Masstree](https://github.com/kohler/masstree-beta.git)
* [ARTOptiQL](https://github.com/sfu-dis/optiql)
* [STX B+-tree](https://github.com/tlx/tlx.git) 
* [Wormhole](https://github.com/wuxb45/wormhole.git)

# Requirements
* x86-64 CPU supporting SSE2 or AVX2 or AVX512 instruction set
* intel Threading Building Blocks (TBB) (`apt install libtbb-dev`)
* jemalloc (`apt install libjemalloc-dev`)
* a C++17 compliant compiler

# API
```
KVPair* lookup(KeyType key)

KVPair* update(KVPair* kv)

KVPair* upsert(KVPair* kv)

KVPair* remove(KeyType key)

iterator begin()

iterator lower_bound(KeyType key)

iterator upper_bound(KeyType key)
```

# Get Started
1. Clone this repository and initialize the submodules
```
git clone <repository name>
cd <repository name>
git submodule init
git submodule update
```
2. Create a new directory *build* `mkdir build && cd build`
3. Build the project `cmake -DCMAKE_BUILD_TYPE=Release .. && make -j`
4. Run the example `./FBTree/FBTreeExample 10000000 1 1`

# Notes
* To evaluate the performance/scalability of concurrent remove, disable `free` interface to mitigate cross-thread 
  memory release overhead (for example, acquire a lock on an arena in jemalloc)