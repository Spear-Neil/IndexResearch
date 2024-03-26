cmake_minimum_required(VERSION 3.10)
project(IndexResearch)
set(CMAKE_CXX_STANDARD 17)

include_directories(util)
include_directories(STX/tlx)

execute_process(COMMAND lscpu COMMAND grep avx512bw OUTPUT_VARIABLE AVX512BW_ENABLE)
execute_process(COMMAND lscpu COMMAND grep avx2 OUTPUT_VARIABLE AVX2_ENABLE)
execute_process(COMMAND lscpu COMMAND grep sse2 OUTPUT_VARIABLE SSE2_ENABLE)

if (AVX512BW_ENABLE)
    message(STATUS "avx512bw enable")
    add_definitions(-DAVX512BW_ENABLE)
endif ()

if (AVX2_ENABLE)
    message(STATUS "avx2 enable")
    add_definitions(-DAVX2_ENABLE)
endif ()

if (SSE2_ENABLE)
    message(STATUS "sse2 enable")
    add_definitions(-DSSE2_ENABLE)
endif ()

if (NOT (AVX512BW_ENABLE OR AVX2_ENABLE OR SSE2_ENABLE))
    message(FATAL_ERROR "not support needed SIMD instruction")
endif ()

add_subdirectory(FBTree)
add_subdirectory(GoogleBTree)
add_subdirectory(STX)

add_compile_options(-march=native)
link_libraries(pthread numa jemalloc tbb)
add_executable(CacheMissTest cache_miss.cpp)
