fn main() {
    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");

    let target = "monad_event_client";
    let dst = cmake::Config::new("../monad-cxx/monad-execution/libs/event")
        .define("CMAKE_BUILD_TARGET", target)
        .build_target(target)
        .build();

    println!("cargo:rustc-link-search=native={}/build", dst.display());
}
