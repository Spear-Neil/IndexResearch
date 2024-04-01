cmake_minimum_required(VERSION 3.10)
project(WormHole)
set(CMAKE_CXX_STANDARD 17)

set(libs kv.c lib.c wh.c)

add_compile_options(-march=native)
link_libraries(pthread numa jemalloc)
add_executable(wh_demo easydemo.c ${libs})
add_executable(wh_concbench concbench.c ${libs})
add_executable(wh_stresstest stresstest.c ${libs})