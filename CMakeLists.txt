cmake_minimum_required(VERSION 3.23)
project(KeyMapped)

set(CMAKE_CXX_STANDARD 17)

if (MSVC)
    add_compile_options(/W4 /Za /WX /MP /arch:AVX /favor:AMD64)
endif()

include_directories(KeyMapped PUBLIC include)

set(SRC
        src/KeyMapped.cpp
        src/Logger.cpp
        src/Utils.cpp
        src/index/HashIndex.cpp
        src/index/SlowIndex.cpp
        src/index/SsTableIndex.cpp
        src/index/LsmTreeIndex.cpp)

add_executable(KeyMapped ${SRC} src/main.cpp)
add_executable(KeyMappedTests ${SRC} tests/tests.cpp)
