mod base;

#[test]
fn order_test() {
    base::run_one_delayed_node(4, 4, false);
}

#[test]
fn order_test_rev() {
    base::run_one_delayed_node(4, 4, true);
}
