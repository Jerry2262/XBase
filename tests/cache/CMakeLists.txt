cmake_minimum_required(VERSION 3.16)
project(kvrocks_cache_test DESCRIPTION "NoSQL which based on rocksdb and compatible with the Redis protocol"
        LANGUAGES CXX)

#g++  -o kvrocks_cache_test test.cc

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#set(CMAKE_CXXFLAGS "${CMAKE_CXXFLAGS} -std=c++17 -pthread")
#set(CMAKE_CXXFLAGS "${CMAKE_CXXFLAGS} -march=arm8.2-a+sve -D__ARM_FEATURE_CRC32=1 -D__ARM_FEATURE_SVE=1")

#so
#add_library(kvrocks_cache_test SHARED kvrocks_cache_test.cc ../../src/storage/cache/kvrocks_cache.cc)

#bin
add_executable(kvrocks_cache_test kvrocks_cache_test.cc ../../src/storage/cache/kvrocks_cache.cc)

include_directories(
    "../../src/storage/cache/"
    "./"
)

target_link_libraries(kvrocks_cache_test PRIVATE Threads::Threads)
