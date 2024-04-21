# FBTree
In essence, FBTree is identical to a main-memory B+Tree except for the layout of inner node and leaf node.
Similar to employing bit or byte for branch operation in Tries, FBTree gradually considers several bytes, referred to as features, following the common prefix on each leaf of inner nodes. 
By incorporating features, FBTree introduces radix concept into B+Tree, so that it blurs the boundary between B+Trees and Tries.
In the best case, FBTree almost becomes a Trie, whereas in the worst case, it continues to function as a B+Tree.

# Synchronization Protocol
FBTree adopts a highly optimized optimistic synchronization protocol for concurrent index access.
It combines the link technique from B-link-Tree with optimistic lock coupling, while the disadvantages of both are eschewed.
It highlights:
* Latch-free index traversal
* Latch-free lookup/update
* No comparison overhead with high_key (B-link-Tree in contrast)
* No restarts from root node (optimistic lock coupling in contrast)

# Index Structures
There is an example in each index directory. 
* [ARTOLC](https://github.com/wangziqi2016/index-microbench.git)
* [BTreeOLC](https://github.com/wangziqi2016/index-microbench.git)
* FBTree
* [GoogleBTree](https://code.google.com/archive/p/cpp-btree/)
* [HOT](https://github.com/speedskater/hot.git)
* [Masstree](https://github.com/kohler/masstree-beta.git)
* [STX BTree](https://github.com/tlx/tlx.git) 
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
* To evaluate the performance/scalability of concurrent remove, disable `free` interface to mitigate cross-thread memory release overhead (for example, acquire a lock on an arena in jemalloc)