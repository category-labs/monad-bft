use std::path::PathBuf;

const INCLUDES: &[(&str, &[&str])] = &[
    (
        "../monad-cxx/monad-execution/",
        &["category/core/event/event_metadata.h"],
    ),
    (
        "../monad-cxx/monad-execution/",
        &[
            "category/execution/ethereum/core/base_ctypes.h",
            "category/execution/ethereum/core/eth_ctypes.h",
            "category/execution/ethereum/event/exec_event_ctypes.h",
        ],
    ),
];

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_args(["-x", "c", "-std=c23"])
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .derive_copy(true)
        .derive_debug(true)
        .derive_hash(true)
        .derive_default(false)
        .derive_eq(true)
        .derive_partialeq(true)
        .no_hash(r"monad_exec_.+")
        .no_partialeq(r"monad_exec_.+")
        .prepend_enum_name(false)
        .allowlist_recursively(false);

    for (lib_path, lib_files) in INCLUDES {
        builder = builder.clang_arg(format!("-I{lib_path}"));

        for lib_file in lib_files.iter() {
            builder = builder.allowlist_file(format!("{lib_path}{lib_file}"));
        }
    }

    let bindings = builder.generate().expect("Unable to generate bindings");

    let bindings_str = bindings
        .to_string()
        .replace(r#"#[doc = "<"#, r#"#[doc = ""#)
        .replace(r#"#[doc = " "#, r#"#[doc = ""#);

    std::fs::write(out_dir.join("bindings.rs"), &bindings_str).expect("Couldn't write bindings!");
}
