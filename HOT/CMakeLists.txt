cmake_minimum_required(VERSION 3.10)
project(Hot)
set(CMAKE_CXX_STANDARD 17)

add_compile_options(-march=native)
include_directories(include)
link_libraries(pthread numa jemalloc tbb)
add_executable(HOTExample example.cpp)