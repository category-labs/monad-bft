cmake_minimum_required(VERSION 3.20)

project(monad-cxx-driver)

if(CMAKE_BUILD_TARGET STREQUAL "monad_rpc_test")
  add_subdirectory("monad-execution")
  add_library(
    monad_rpc_test SHARED
    "include/test_db.hpp"
    "src/test_db.cpp")
  target_include_directories(monad_rpc_test PUBLIC ${PROJECT_SOURCE_DIR}/include)

  monad_compile_options(monad_rpc_test)

  target_link_libraries(monad_rpc_test PRIVATE -Wl,--whole-archive monad_rpc -Wl,--no-whole-archive monad_execution)
endif()
