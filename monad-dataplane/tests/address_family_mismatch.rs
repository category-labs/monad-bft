use std::{thread::sleep, time::Duration};

use monad_dataplane::{udp::DEFAULT_SEGMENT_SIZE, BroadcastMsg, DataplaneBuilder};
use tracing::debug;

/// 1_000 = 1 Gbps, 10_000 = 10 Gbps
const UP_BANDWIDTH_MBPS: u64 = 1_000;

const BIND_ADDRS: [&str; 3] = ["0.0.0.0:9100", "127.0.0.1:9101", "[::1]:9102"];

const TX_ADDRS: [&str; 2] = ["127.0.0.1:9200", "[::1]:9201"];

#[test]
fn address_family_mismatch() {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Cause the test to fail if any of the Dataplane threads panic.  Taken from:
    // https://stackoverflow.com/questions/35988775/how-can-i-cause-a-panic-on-a-thread-to-immediately-end-the-main-thread/36031130#36031130
    let orig_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        orig_panic_hook(panic_info);
        std::process::exit(1);
    }));

    for addr in BIND_ADDRS {
        let mut dataplane =
            DataplaneBuilder::new(&addr.parse().unwrap(), UP_BANDWIDTH_MBPS).build();

        // Allow Dataplane thread to set itself up.
        sleep(Duration::from_millis(10));

        for tx_addr in TX_ADDRS {
            debug!("sending to {} from {}", tx_addr, addr);

            dataplane.udp_write_broadcast(BroadcastMsg {
                targets: vec![tx_addr.parse().unwrap(); 1],
                payload: vec![0; DEFAULT_SEGMENT_SIZE.into()].into(),
                stride: DEFAULT_SEGMENT_SIZE,
            });
        }

        // Allow Dataplane thread to catch up.
        sleep(Duration::from_millis(10));
    }
}
