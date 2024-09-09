use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=include");
    println!("cargo:rerun-if-changed=monad-execution");
    println!("cargo:rerun-if-env-changed=ETH_CALL_TARGET");
    let target = env::var("ETH_CALL_TARGET").unwrap_or("mock_eth_call".to_owned());
    if target != "mock_eth_call" {
        println!("cargo:rustc-cfg=triedb");
    }
    println!("cargo:warning=target {}", &target);

    // let includes = [PathBuf::from("include")];

    // // TODO(rene): find a better way of figuring out the vendor-specific standard version string
    // let std = "-std=c++23";

    // // generate rust bindings for eth_call C++ API
    // {
    //     let mut b = autocxx_build::Builder::new("src/lib.rs", includes.iter())
    //         .extra_clang_args(&[std])
    //         .build()
    //         .expect("autocxx failed");
    //     b.flag_if_supported(std).compile("monad-cxx");
    // }

    let dst = cmake::Config::new(".")
        .define("CMAKE_BUILD_TARGET", &target)
        .always_configure(true)
        .define("BUILD_SHARED_LIBS", "ON")
        .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON")
        .build_target(&target)
        .build();
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dst.display());
    println!("cargo:rustc-link-search=native={}/build", dst.display());
    println!("cargo:rustc-link-lib=dylib={}", target);

    let bindings = bindgen::Builder::default()
        .header("include/triedb.h")
        // invalidate on header change
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");
}
