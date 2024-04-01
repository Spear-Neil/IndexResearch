cmake_minimum_required(VERSION 3.10)
project(ExperimentEvaluation)
set(CMAKE_CXX_STANDARD 17)

add_compile_options(-march=native)
link_libraries(pthread numa jemalloc tbb)
add_executable(CacheMissTest cache_miss.cpp)