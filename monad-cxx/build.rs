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
        .header("include/eth_call.h")
        .header("include/test_db.h")
        // invalidate on header change
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("eth_call.rs"))
        .expect("Couldn't write bindings!");
}
