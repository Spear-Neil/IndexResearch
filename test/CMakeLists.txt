cmake_minimum_required(VERSION 3.10)
project(ExperimentEvaluation)
set(CMAKE_CXX_STANDARD 17)

include_directories(../HOT/include)

add_compile_options(-march=native)
link_libraries(pthread numa jemalloc tbb)
set(artlib ../ARTOLC/Tree.cpp)
set(optiqllib ../OptiQL/Tree.cpp)
set(masslib ../MassTree/kvthread.cc ../MassTree/compiler.cc ../MassTree/str.cc ../MassTree/string.cc ../MassTree/straccum.cc)
set(whlib ../wormhole/kv.c ../wormhole/lib.c ../wormhole/wh.c)
add_library(indexes SHARED index.cpp ${artlib} ${masslib} ${whlib} ${optiqllib})
# disable OMCS_OFFSET_NUMA_QNODE because it works bad in our machine
target_compile_definitions(indexes PUBLIC OMCS_LOCK IS_CONTEXTFUL OMCS_OP_READ OMCS_OFFSET)# OMCS_OFFSET_NUMA_QNODE)

add_executable(CacheMissTest cache_miss.cpp)
add_dependencies(CacheMissTest build_fast)
target_link_directories(CacheMissTest PRIVATE ${CMAKE_BINARY_DIR}/fast64/target/release)
target_link_libraries(CacheMissTest fast64 dl)

link_libraries(indexes)
add_executable(ycsb_build ycsb_build.cpp)
add_executable(ycsb_test ycsb_test.cpp)