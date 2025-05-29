# Normally we would set the _INIT variants in a toolchain file, but Rust's cmake
# crate force overrides the toolchain file. Rather than fix their incorrect crate,
# work around it
set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -march=skylake-avx512")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=skylake-avx512")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=skylake-avx512 -DQUILL_ACTIVE_LOG_LEVEL=QUILL_LOG_LEVEL_CRITICAL")

set(CMAKE_C_FLAGS_RELEASE_INIT "-g1")
set(CMAKE_CXX_FLAGS_RELEASE_INIT "-g1")
set(CMAKE_C_STANDARD 23)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(BUILD_SHARED_LIBS ON)
