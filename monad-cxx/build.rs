// build.rs

use std::{env, path::PathBuf};

fn main() {
    use cmake::Config;
    let target = env::var("ETH_CALL_TARGET").unwrap_or("mock_eth_call".to_owned());
    let c_compiler = env::var("CMAKE_C_COMPILER").unwrap_or("gcc-13".to_owned());
    let cxx_compiler = env::var("CMAKE_CXX_COMPILER").unwrap_or("g++-13".to_owned());
    println!("cargo:warning=target {}", &target);
    println!("cargo:warning=c_compiler {}", &c_compiler);
    println!("cargo:warning=cxx_compiler {}", &cxx_compiler);
    let includes = [
        "include",
        "src",
        "monad-execution/include",
        "monad-execution/third_party/intx/include",
        "monad-execution/third_party/evmone/evmc/include",
        "monad-execution/monad-core/c/include",
        "monad-execution/monad-core/include",
    ]
    .into_iter()
    .map(PathBuf::from)
    .collect::<Vec<_>>();

    // generate rust bindings from EVMC C API
    {
        let bindings = bindgen::Builder::default()
            .header("include/evmc.h")
            .generate_comments(true)
            // do not generate an empty enum for EVMC_ABI_VERSION
            .constified_enum("")
            // generate Rust enums for each evmc enum
            .rustified_enum(".*")
            // force deriving the Hash trait on basic types (address, bytes32)
            .derive_hash(true)
            // force deriving the PratialEq trait on basic types (address, bytes32)
            .derive_partialeq(true)
            .blocklist_type("evmc_host_context")
            .allowlist_type("evmc_.*")
            .allowlist_function("evmc_.*")
            .allowlist_var("EVMC_ABI_VERSION")
            // TODO: consider removing this
            .size_t_is_usize(true)
            .generate()
            .expect("Unable to generate bindings");

        let out_path = PathBuf::from(std::env::var("OUT_DIR").unwrap());
        bindings
            .write_to_file(out_path.join("bindings.rs"))
            .expect("Couldn't write bindings!");
    }

    // TODO(rene): find a better way of figuring out the vendor-specific standard version string
    let std = "-std=c++20";

    // generate rust bindings for eth_call C++ API
    {
        let mut b = autocxx_build::Builder::new("src/lib.rs", includes.iter())
            .extra_clang_args(&[std])
            .build()
            .expect("autocxx failed");
        b.flag_if_supported(std).compile("monad-cxx");
    }

    let dst = Config::new(".")
        .define("CMAKE_C_COMPILER", c_compiler)
        .define("CMAKE_CXX_COMPILER", cxx_compiler)
        .define("CMAKE_BUILD_TARGET", &target)
        .always_configure(true)
        .asmflag("-march=haswell")
        .cflag("-march=haswell")
        .cxxflag("-march=haswell")
        .build_target(&target)
        .very_verbose(true)
        .build();
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dst.display());
    println!("cargo:rustc-link-search=native={}/build", dst.display());
    println!("cargo:rustc-link-lib=dylib={}", target);
}
