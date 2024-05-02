cmake_minimum_required(VERSION 3.20)

project(monad-cxx-driver)

# mock_eth_call
add_library(
  mock_eth_call SHARED
  "${PROJECT_SOURCE_DIR}/include/eth_call.hpp"
  "${PROJECT_SOURCE_DIR}/include/test_db.hpp"
  "${PROJECT_SOURCE_DIR}/src/mock_eth_call.cpp"
  "${PROJECT_SOURCE_DIR}/src/mock_test_db.cpp")

target_include_directories(mock_eth_call PUBLIC ${PROJECT_SOURCE_DIR}/include)

set_property(TARGET mock_eth_call PROPERTY C_STANDARD 20)
set_property(TARGET mock_eth_call PROPERTY C_STANDARD_REQUIRED ON)
set_property(TARGET mock_eth_call PROPERTY CXX_STANDARD 20)
set_property(TARGET mock_eth_call PROPERTY CXX_STANDARD_REQUIRED ON)

if(CMAKE_BUILD_TARGET STREQUAL "monad_shared")
  add_subdirectory("monad-execution")
  add_library(
    monad_shared SHARED
    # header
    "include/eth_call.hpp" "include/test_db.hpp"
    # src
    "src/eth_call.cpp" "src/test_db.cpp")
  target_include_directories(monad_shared PUBLIC ${PROJECT_SOURCE_DIR}/include)
  target_link_libraries(monad_shared PUBLIC monad_execution)
  monad_compile_options(monad_shared)
endif()
