// build.rs

use std::path::PathBuf;

fn main() {
    use cmake::Config;
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=include");
    println!("cargo:rerun-if-changed=monad-execution");
    println!("cargo:rerun-if-env-changed=TRIEDB_TARGET");

    let includes = [PathBuf::from("include")];

    // TODO(rene): find a better way of figuring out the vendor-specific standard version string
    let std = "-std=c++23";

    // generate rust bindings for eth_call C++ API
    {
        let mut b = autocxx_build::Builder::new("src/lib.rs", includes.iter())
            .extra_clang_args(&[std])
            .build()
            .expect("autocxx failed");
        b.flag_if_supported(std).compile("monad-cxx");
    }

    // Because we added test files here, the we need to build monad_rpc (for execution) together
    // with the tests, thus we need a new target
    let target = "monad_rpc_test";
    let dst = Config::new(".")
        .define("CMAKE_BUILD_TARGET", target)
        .always_configure(true)
        .define("BUILD_SHARED_LIBS", "ON")
        .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON")
        .build_target(target)
        .very_verbose(true)
        .build();
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dst.display());
    println!("cargo:rustc-link-search=native={}/build", dst.display());
    println!("cargo:rustc-link-lib=dylib={}", target);
}
