use std::path::PathBuf;

const INCLUDES: [(&str, &[&str]); 1] = [(
    "../monad-cxx/monad-execution/libs/core/src/",
    &[
        "monad/event/event_iterator_inline.h",
        "monad/event/event_iterator.h",
        "monad/event/event_metadata.h",
        "monad/event/event_ring_util.h",
        "monad/event/event_ring.h",
    ],
)];

const STATIC_FNS_PATH: &str = "monad_event__wrap_static_fns";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");

    let client_target = "monad_event";
    let client_dst = cmake::Config::new("../monad-cxx/monad-execution/libs/event")
        .define("CMAKE_BUILD_TARGET", client_target)
        .build_target(client_target)
        .build();

    println!(
        "cargo:rustc-link-search=native={}/build",
        client_dst.display()
    );
    println!("cargo:rustc-link-lib=static=monad_event");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_args(["-x", "c", "-std=c23"])
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .wrap_static_fns(true)
        .wrap_static_fns_path(out_dir.join(STATIC_FNS_PATH))
        .derive_copy(false)
        .derive_partialeq(true)
        .derive_eq(true)
        .allowlist_recursively(false);

    for (lib_path, lib_files) in INCLUDES {
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

    std::fs::write(out_dir.join("bindings.rs"), bindings_str).expect("Couldn't write bindings!");

    cc::Build::new()
        .std("c2x")
        .file(out_dir.join(format!("{STATIC_FNS_PATH}.c")))
        .includes(
            std::iter::once(PathBuf::from(env!("CARGO_MANIFEST_DIR"))).chain(
                INCLUDES
                    .iter()
                    .map(|(include_path, _)| PathBuf::from(include_path)),
            ),
        )
        .compile(STATIC_FNS_PATH);
}
