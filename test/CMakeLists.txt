cmake_minimum_required(VERSION 3.10)
project(ExperimentEvaluation)
set(CMAKE_CXX_STANDARD 17)

include_directories(../HOT/include)

add_compile_options(-march=native)
link_libraries(pthread numa jemalloc tbb)
set(artlib ../ARTOLC/Tree.cpp)
set(masslib ../MassTree/kvthread.cc ../MassTree/compiler.cc ../MassTree/str.cc ../MassTree/string.cc ../MassTree/straccum.cc)
set(whlib ../wormhole/kv.c ../wormhole/lib.c ../wormhole/wh.c)
add_library(indexes index.cpp ${artlib} ${masslib} ${whlib})

add_executable(CacheMissTest cache_miss.cpp)

link_libraries(indexes)
add_executable(int_test int_test.cpp)
add_executable(str_test str_test.cpp)