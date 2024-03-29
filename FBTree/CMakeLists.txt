cmake_minimum_required(VERSION 3.10)
project(FeatureBTree)
set(CMAKE_CXX_STANDARD 17)

add_compile_options(-march=native)
link_libraries(pthread numa jemalloc tbb)

add_executable(FBTreeExample example.cpp)
add_executable(StringFBTreeExample sexample.cpp)
