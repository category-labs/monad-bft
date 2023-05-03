mod base;

#[test]
fn two_nodes() {
    tracing_subscriber::fmt::init();

    base::run_nodes_msg_delays(4, 40);
}
