cmake_minimum_required(VERSION 3.10)
project(MassTree)
set(CMAKE_CXX_STANDARD 17)

add_compile_options(-include "${PROJECT_SOURCE_DIR}/config.h")

link_libraries(pthread numa jemalloc tbb)
add_executable(MassTreeExample example.cpp kvthread.cc compiler.cc)
