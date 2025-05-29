use std::{env, path};

fn main() {
    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");
    println!("cargo:rerun-if-env-changed=TRIEDB_TARGET");
    let build_execution_lib = env::var("TRIEDB_TARGET")
        .is_ok_and(|target| target == "monad-triedb-driver" || target == "triedb_driver");
    if build_execution_lib {
        let target = "monad-triedb-driver";
        let toolchain_file = path::absolute("../monad-cxx/toolchain-avx2.cmake").unwrap();
        let dst = cmake::Config::new("../monad-cxx/monad-execution")
            .define("CMAKE_BUILD_TARGET", target)
            .define("CMAKE_TOOLCHAIN_FILE", toolchain_file)
            .build_target(target)
            .build();

        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dst.display());
        println!(
            "cargo:rustc-link-search=native={}/build/libs/triedb-driver",
            dst.display()
        );
        println!("cargo:rustc-link-lib=dylib={}", &target);
    }
}
