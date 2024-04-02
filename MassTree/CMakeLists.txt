cmake_minimum_required(VERSION 3.10)
project(MassTree)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_COMPILER g++)

link_libraries(pthread numa jemalloc tbb)
add_compile_options(-include "${PROJECT_SOURCE_DIR}/config.h")

add_executable(MassTreeExample example.cpp kvthread.cc compiler.cc)

add_executable(Mass_mttest mttest.cc misc.cc checkpoint.cc testrunner.cc kvio.cc json.cc string.cc
        straccum.cc str.cc msgpack.cc clp.c kvrandom.cc compiler.cc memdebug.cc kvthread.cc query_masstree.cc)
