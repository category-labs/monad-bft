use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");

    let out_path = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_args(["-x", "c", "-std=c23"])
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .wrap_static_fns(true)
        .derive_copy(true)
        .derive_debug(true)
        .derive_hash(true)
        .derive_default(false)
        .derive_eq(true)
        .derive_partialeq(true)
        .no_hash(r"monad_exec_.+")
        .no_partialeq(r"monad_exec_.+")
        .allowlist_recursively(false);

    for (lib_path, lib_files) in [
        (
            "../monad-cxx/monad-execution/libs/core/src/",
            vec!["monad/event/event_metadata.h"],
        ),
        (
            "../monad-cxx/monad-execution/libs/execution/src/",
            vec![
                "monad/core/base_ctypes.h",
                "monad/core/eth_ctypes.h",
                "monad/core/event/exec_event_ctypes.h",
            ],
        ),
        (
            "../monad-cxx/monad-execution/third_party/intx/include/",
            vec!["intx/intx.hpp"],
        ),
    ] {
        builder = builder.clang_arg(format!("-I{lib_path}"));

        for lib_file in lib_files {
            builder = builder.allowlist_file(format!("{lib_path}{lib_file}"));
        }
    }

    let bindings = builder.generate().expect("Unable to generate bindings");

    let bindings_str = bindings
        .to_string()
        .replace(r#"#[doc = "<"#, r#"#[doc = ""#)
        .replace(r#"#[doc = " "#, r#"#[doc = ""#);

    std::fs::write(out_path.join("bindings.rs"), &bindings_str).expect("Couldn't write bindings!");
}
