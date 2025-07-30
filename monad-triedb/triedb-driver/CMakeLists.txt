cmake_minimum_required(VERSION 3.20)

project(triedb-driver)

# triedb_driver

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory("../../monad-cxx/monad-execution" "monad-execution-build"
)# needed for monad-trie

add_library(triedb_driver SHARED "${PROJECT_SOURCE_DIR}/include/triedb.h"
                                 "${PROJECT_SOURCE_DIR}/src/triedb.cpp")

target_include_directories(triedb_driver PUBLIC ${PROJECT_SOURCE_DIR}/include)

target_link_libraries(triedb_driver PUBLIC monad_trie)
target_link_libraries(triedb_driver PUBLIC monad_execution)

monad_compile_options(triedb_driver)
