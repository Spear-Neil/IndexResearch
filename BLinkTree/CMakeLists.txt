cmake_minimum_required(VERSION 3.10)
project(BLinkTree)
set(CMAKE_CXX_STANDARD 17)

add_compile_options(-march=native)
add_executable(BlinkExample example.cpp)