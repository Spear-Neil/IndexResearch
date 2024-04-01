cmake_minimum_required(VERSION 3.10)
project(ArtOLC)
set(CMAKE_CXX_STANDARD 17)

link_libraries(pthread tbb)
add_executable(ArtExample example.cpp Tree.cpp)