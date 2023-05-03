mod base;

#[test]
fn two_nodes() {
    tracing_subscriber::fmt::init();

    base::run_nodes(2, 1024);
}
