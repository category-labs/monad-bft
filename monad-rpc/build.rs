fn main() {
    // TODO(ken): this is only needed when we're running tests, to build the
    //   fake event server library to test exec_update_builder.rs; I couldn't
    //   easily figure out how to get conditional compilation in the build
    //   system, but this should be done later
    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");

    let target = "monad_event_test_server";
    let dst = cmake::Config::new("../monad-cxx/monad-execution/libs/event")
        .define("MONAD_EVENT_BUILD_TEST_SERVER", "ON")
        .define("CMAKE_BUILD_TARGET", target)
        .build_target(target)
        .build();

    println!("cargo:rustc-link-search=native={}/build", dst.display());
}
