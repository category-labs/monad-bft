use std::{collections::HashMap, sync::Once, thread, time::Duration};

use futures::executor;
use monad_dataplane::{udp::DEFAULT_SEGMENT_SIZE, BroadcastMsg, DataplaneBuilder, RecvUdpMsg};
use ntest::timeout;
use rand::Rng;
use tracing_subscriber::fmt::format::FmtSpan;

const UP_BANDWIDTH_MBPS: u64 = 1_000;
const LEGACY_SOCKET: &str = "legacy";

static ONCE_SETUP: Once = Once::new();

fn once_setup() {
    ONCE_SETUP.call_once(|| {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_span_events(FmtSpan::CLOSE)
            .init();
    });
}

#[test]
#[timeout(10000)]
fn udp_reuseport_with_four_readers() {
    once_setup();

    let rx_addr = "127.0.0.1:9040".parse().unwrap();
    let tx_addr_1 = "127.0.0.1:9041".parse().unwrap();
    let tx_addr_2 = "127.0.0.1:9042".parse().unwrap();
    let tx_addr_3 = "127.0.0.1:9043".parse().unwrap();

    let num_msgs_per_tx = 100;
    let total_msgs = num_msgs_per_tx * 3;

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .enable_so_reuseport(4)
        .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
            socket_addr: rx_addr,
            label: LEGACY_SOCKET.to_string(),
        }])
        .build();

    let mut tx1 = DataplaneBuilder::new(&tx_addr_1, UP_BANDWIDTH_MBPS)
        .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
            socket_addr: tx_addr_1,
            label: LEGACY_SOCKET.to_string(),
        }])
        .build();

    let mut tx2 = DataplaneBuilder::new(&tx_addr_2, UP_BANDWIDTH_MBPS)
        .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
            socket_addr: tx_addr_2,
            label: LEGACY_SOCKET.to_string(),
        }])
        .build();

    let mut tx3 = DataplaneBuilder::new(&tx_addr_3, UP_BANDWIDTH_MBPS)
        .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
            socket_addr: tx_addr_3,
            label: LEGACY_SOCKET.to_string(),
        }])
        .build();

    assert!(rx.block_until_ready(Duration::from_secs(2)));
    assert!(tx1.block_until_ready(Duration::from_secs(1)));
    assert!(tx2.block_until_ready(Duration::from_secs(1)));
    assert!(tx3.block_until_ready(Duration::from_secs(1)));

    let payload1: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    let payload2: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    let payload3: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    let mut rx_socket = rx.take_udp_socket_handle(LEGACY_SOCKET).unwrap();
    let tx1_socket = tx1.take_udp_socket_handle(LEGACY_SOCKET).unwrap();
    let tx2_socket = tx2.take_udp_socket_handle(LEGACY_SOCKET).unwrap();
    let tx3_socket = tx3.take_udp_socket_handle(LEGACY_SOCKET).unwrap();

    tx1_socket.write_broadcast(BroadcastMsg {
        targets: vec![rx_addr; num_msgs_per_tx],
        payload: payload1.clone().into(),
        stride: DEFAULT_SEGMENT_SIZE,
    });

    tx2_socket.write_broadcast(BroadcastMsg {
        targets: vec![rx_addr; num_msgs_per_tx],
        payload: payload2.clone().into(),
        stride: DEFAULT_SEGMENT_SIZE,
    });

    tx3_socket.write_broadcast(BroadcastMsg {
        targets: vec![rx_addr; num_msgs_per_tx],
        payload: payload3.clone().into(),
        stride: DEFAULT_SEGMENT_SIZE,
    });

    let mut received_by_sender: HashMap<std::net::SocketAddr, usize> = HashMap::new();
    received_by_sender.insert(tx_addr_1, 0);
    received_by_sender.insert(tx_addr_2, 0);
    received_by_sender.insert(tx_addr_3, 0);

    for _ in 0..total_msgs {
        let msg: RecvUdpMsg = executor::block_on(rx_socket.recv());

        if msg.src_addr == tx_addr_1 {
            assert_eq!(msg.payload, payload1);
            *received_by_sender.get_mut(&tx_addr_1).unwrap() += 1;
        } else if msg.src_addr == tx_addr_2 {
            assert_eq!(msg.payload, payload2);
            *received_by_sender.get_mut(&tx_addr_2).unwrap() += 1;
        } else if msg.src_addr == tx_addr_3 {
            assert_eq!(msg.payload, payload3);
            *received_by_sender.get_mut(&tx_addr_3).unwrap() += 1;
        } else {
            panic!("unexpected source address: {}", msg.src_addr);
        }
    }

    assert_eq!(
        *received_by_sender.get(&tx_addr_1).unwrap(),
        num_msgs_per_tx
    );
    assert_eq!(
        *received_by_sender.get(&tx_addr_2).unwrap(),
        num_msgs_per_tx
    );
    assert_eq!(
        *received_by_sender.get(&tx_addr_3).unwrap(),
        num_msgs_per_tx
    );
}

#[test]
#[timeout(10000)]
fn udp_reuseport_high_volume() {
    once_setup();

    let rx_addr = "127.0.0.1:9044".parse().unwrap();
    let tx_addr = "127.0.0.1:9045".parse().unwrap();

    let num_msgs = 1000;

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .enable_so_reuseport(4)
        .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
            socket_addr: rx_addr,
            label: LEGACY_SOCKET.to_string(),
        }])
        .build();

    let mut tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS)
        .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
            socket_addr: tx_addr,
            label: LEGACY_SOCKET.to_string(),
        }])
        .build();

    assert!(rx.block_until_ready(Duration::from_secs(2)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE).map(|i| (i % 256) as u8).collect();

    let mut rx_socket = rx.take_udp_socket_handle(LEGACY_SOCKET).unwrap();
    let tx_socket = tx.take_udp_socket_handle(LEGACY_SOCKET).unwrap();

    tx_socket.write_broadcast(BroadcastMsg {
        targets: vec![rx_addr; num_msgs],
        payload: payload.clone().into(),
        stride: DEFAULT_SEGMENT_SIZE,
    });

    for _ in 0..num_msgs {
        let msg: RecvUdpMsg = executor::block_on(rx_socket.recv());
        assert_eq!(msg.src_addr, tx_addr);
        assert_eq!(msg.payload, payload);
    }
}

#[test]
#[timeout(10000)]
fn udp_reuseport_concurrent_senders() {
    once_setup();

    let rx_addr = "127.0.0.1:9046".parse().unwrap();

    let num_senders = 8;
    let num_msgs_per_sender = 50;
    let total_msgs = num_senders * num_msgs_per_sender;

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .enable_so_reuseport(4)
        .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
            socket_addr: rx_addr,
            label: LEGACY_SOCKET.to_string(),
        }])
        .build();

    assert!(rx.block_until_ready(Duration::from_secs(2)));

    let mut tx_threads = Vec::new();
    let mut expected_data: HashMap<std::net::SocketAddr, Vec<u8>> = HashMap::new();

    for i in 0..num_senders {
        let tx_addr = format!("127.0.0.1:{}", 9050 + i).parse().unwrap();

        let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
            .map(|_| rand::thread_rng().gen_range(0..255))
            .collect();

        expected_data.insert(tx_addr, payload.clone());

        let handle = thread::spawn(move || {
            let mut tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS)
                .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
                    socket_addr: tx_addr,
                    label: LEGACY_SOCKET.to_string(),
                }])
                .build();

            assert!(tx.block_until_ready(Duration::from_secs(1)));

            let tx_socket = tx.take_udp_socket_handle(LEGACY_SOCKET).unwrap();
            tx_socket.write_broadcast(BroadcastMsg {
                targets: vec![rx_addr; num_msgs_per_sender],
                payload: payload.into(),
                stride: DEFAULT_SEGMENT_SIZE,
            });
        });

        tx_threads.push(handle);
    }

    let mut rx_socket = rx.take_udp_socket_handle(LEGACY_SOCKET).unwrap();
    let mut received_by_sender: HashMap<std::net::SocketAddr, usize> = HashMap::new();

    for _ in 0..total_msgs {
        let msg: RecvUdpMsg = executor::block_on(rx_socket.recv());

        let expected = expected_data.get(&msg.src_addr).expect("unexpected sender");
        assert_eq!(msg.payload, *expected);

        *received_by_sender.entry(msg.src_addr).or_insert(0) += 1;
    }

    assert_eq!(received_by_sender.len(), num_senders);
    for count in received_by_sender.values() {
        assert_eq!(*count, num_msgs_per_sender);
    }

    for handle in tx_threads {
        handle.join().unwrap();
    }
}

#[test]
#[timeout(10000)]
fn udp_reuseport_with_ringbuf() {
    once_setup();

    let rx_addr = "127.0.0.1:9060".parse().unwrap();
    let tx_addr = "127.0.0.1:9061".parse().unwrap();

    let num_msgs = 500;

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .enable_so_reuseport(4)
        .uring_udp_ringbuf(true)
        .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
            socket_addr: rx_addr,
            label: LEGACY_SOCKET.to_string(),
        }])
        .build();

    let mut tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS)
        .extend_udp_sockets(vec![monad_dataplane::UdpSocketConfig {
            socket_addr: tx_addr,
            label: LEGACY_SOCKET.to_string(),
        }])
        .build();

    assert!(rx.block_until_ready(Duration::from_secs(2)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE).map(|i| (i % 256) as u8).collect();

    let mut rx_socket = rx.take_udp_socket_handle(LEGACY_SOCKET).unwrap();
    let tx_socket = tx.take_udp_socket_handle(LEGACY_SOCKET).unwrap();

    tx_socket.write_broadcast(BroadcastMsg {
        targets: vec![rx_addr; num_msgs],
        payload: payload.clone().into(),
        stride: DEFAULT_SEGMENT_SIZE,
    });

    for _ in 0..num_msgs {
        let msg: RecvUdpMsg = executor::block_on(rx_socket.recv());
        assert_eq!(msg.payload, payload);
    }
}
