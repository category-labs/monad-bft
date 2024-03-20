cmake_minimum_required(VERSION 3.20)

project(triedb-driver)

# triedb_driver_mock
add_library(
  triedb_driver_mock SHARED
  "${PROJECT_SOURCE_DIR}/include/triedb.h"
  "${PROJECT_SOURCE_DIR}/src/triedb_mock.cpp")

target_include_directories(
  triedb_driver_mock
  PUBLIC ${PROJECT_SOURCE_DIR}/include)

set_property(TARGET triedb_driver_mock PROPERTY C_STANDARD 17)
set_property(TARGET triedb_driver_mock PROPERTY C_STANDARD_REQUIRED ON)
set_property(TARGET triedb_driver_mock PROPERTY CXX_STANDARD 17)
set_property(TARGET triedb_driver_mock PROPERTY CXX_STANDARD_REQUIRED ON)

if(CMAKE_BUILD_TARGET STREQUAL "triedb_driver")
  # triedb_driver

  set(CMAKE_POSITION_INDEPENDENT_CODE ON)

  add_subdirectory("../../monad-cxx/monad-execution" "monad-execution-build") # needed for monad-trie

  add_library(
    triedb_driver SHARED
    "${PROJECT_SOURCE_DIR}/include/triedb.h"
    "${PROJECT_SOURCE_DIR}/src/triedb.cpp")

  target_include_directories(
    triedb_driver
    PUBLIC ${PROJECT_SOURCE_DIR}/include)

  target_link_libraries(
    triedb_driver
    PUBLIC monad_trie)

  monad_core_compile_options(triedb_driver)
endif()
