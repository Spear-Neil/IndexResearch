cmake_minimum_required(VERSION 3.10)
project(fast)
set(CMAKE_CXX_STANDARD 17)

add_custom_target(copy_fast COMMAND cp -rf ${CMAKE_CURRENT_SOURCE_DIR}/fast64 ${CMAKE_BINARY_DIR})
# in clion you need to configure environment in CMake to find cargo, for example PATH=/home/sn/.cargo/bin/:$PATH
add_custom_target(build_fast COMMAND cargo build --release WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/fast64)
add_dependencies(build_fast copy_fast)

add_executable(FastExample example.cpp)
add_dependencies(FastExample build_fast)

target_link_directories(FastExample PRIVATE ${CMAKE_BINARY_DIR}/fast64/target/release)
target_link_libraries(FastExample fast64 pthread numa jemalloc tbb dl)
