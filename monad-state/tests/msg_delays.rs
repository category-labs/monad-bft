mod base;

#[test]
fn two_nodes() {
    base::run_nodes_msg_delays(4, 40);
}
