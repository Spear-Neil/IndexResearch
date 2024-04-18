cmake_minimum_required(VERSION 3.10)
project(util)
set(CMAKE_CXX_STANDARD 17)

execute_process(COMMAND lscpu COMMAND grep avx512f OUTPUT_VARIABLE AVX512F_ENABLE)
execute_process(COMMAND lscpu COMMAND grep avx512bw OUTPUT_VARIABLE AVX512BW_ENABLE)
execute_process(COMMAND lscpu COMMAND grep avx OUTPUT_VARIABLE AVX_ENABLE)
execute_process(COMMAND lscpu COMMAND grep avx2 OUTPUT_VARIABLE AVX2_ENABLE)
execute_process(COMMAND lscpu COMMAND grep sse2 OUTPUT_VARIABLE SSE2_ENABLE)

if (AVX512F_ENABLE)
    message(STATUS "avx512f enable")
    add_definitions(-DAVX512F_ENABLE)
endif ()

if (AVX512BW_ENABLE)
    message(STATUS "avx512bw enable")
    add_definitions(-DAVX512BW_ENABLE)
endif ()

if (AVX_ENABLE)
    message(STATUS "avx enable")
    add_definitions(-DAVX_ENABLE)
endif ()

if (AVX2_ENABLE)
    message(STATUS "avx2 enable")
    add_definitions(-DAVX2_ENABLE)
endif ()

if (SSE2_ENABLE)
    message(STATUS "sse2 enable")
    add_definitions(-DSSE2_ENABLE)
endif ()


include_directories(./)
add_compile_options(-march=native)
link_libraries(pthread numa jemalloc tbb)

add_executable(util_common_example example/common_example.cpp)
add_executable(util_epoch_example example/epoch_example.cpp)
add_executable(util_hash_example example/hash_example.cpp)
add_executable(util_log_example example/log_example.cpp)
add_executable(util_memory_test example/memory_test.cpp)
add_executable(util_simdcmp_example example/simdcmp_example.cpp)
add_executable(util_simdstore_example example/simdstore_example.cpp)
add_executable(util_strutil_example example/strutil_example.cpp)
add_executable(util_timer_example example/timer_example.cpp)
