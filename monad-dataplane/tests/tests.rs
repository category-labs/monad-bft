use std::{sync::Once, thread::sleep, time::Duration};

use futures::executor;
use monad_dataplane::{
    udp::DEFAULT_SEGMENT_SIZE, BroadcastMsg, Dataplane, RecvMsg, TcpMsg, UnicastMsg,
};
use ntest::timeout;
use rand::Rng;
use tracing_subscriber::fmt::format::FmtSpan;

/// 1_000 = 1 Gbps, 10_000 = 10 Gbps
const UP_BANDWIDTH_MBPS: u64 = 1_000;

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
#[timeout(1000)]
fn udp_broadcast() {
    once_setup();

    let rx_addr = "127.0.0.1:9000".parse().unwrap();
    let tx_addr = "127.0.0.1:9001".parse().unwrap();
    let num_msgs = 10;

    let mut rx = Dataplane::new(&rx_addr, UP_BANDWIDTH_MBPS);
    let mut tx = Dataplane::new(&tx_addr, UP_BANDWIDTH_MBPS);

    // Allow Dataplane threads to set themselves up.
    sleep(Duration::from_millis(10));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    tx.udp_write_broadcast(BroadcastMsg {
        targets: vec![rx_addr; num_msgs],
        payload: payload.clone().into(),
        stride: DEFAULT_SEGMENT_SIZE,
    });

    for _ in 0..num_msgs {
        let msg: RecvMsg = executor::block_on(rx.udp_read());

        assert_eq!(msg.src_addr, tx_addr);
        assert_eq!(msg.payload, payload);
    }
}

#[test]
#[timeout(1000)]
fn udp_unicast() {
    once_setup();

    let rx_addr = "127.0.0.1:9002".parse().unwrap();
    let tx_addr = "127.0.0.1:9003".parse().unwrap();
    let num_msgs = 10;

    let mut rx = Dataplane::new(&rx_addr, UP_BANDWIDTH_MBPS);
    let mut tx = Dataplane::new(&tx_addr, UP_BANDWIDTH_MBPS);

    // Allow Dataplane threads to set themselves up.
    sleep(Duration::from_millis(10));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    tx.udp_write_unicast(UnicastMsg {
        msgs: vec![(rx_addr, payload.clone().into()); num_msgs],
        stride: DEFAULT_SEGMENT_SIZE,
    });

    for _ in 0..num_msgs {
        let msg: RecvMsg = executor::block_on(rx.udp_read());

        assert_eq!(msg.src_addr, tx_addr);
        assert_eq!(msg.payload, payload);
    }
}

#[test]
#[timeout(1000)]
fn tcp_slow() {
    once_setup();

    let rx_addr = "127.0.0.1:9004".parse().unwrap();
    let tx_addr = "127.0.0.1:9005".parse().unwrap();
    let num_msgs = 10;

    let mut rx = Dataplane::new(&rx_addr, UP_BANDWIDTH_MBPS);
    let mut tx = Dataplane::new(&tx_addr, UP_BANDWIDTH_MBPS);

    // Allow Dataplane threads to set themselves up.
    sleep(Duration::from_millis(10));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    for _ in 0..num_msgs {
        tx.tcp_write(
            rx_addr,
            TcpMsg {
                msg: payload.clone().into(),
                completion: None,
            },
        );
        sleep(Duration::from_millis(10));
    }

    for _ in 0..num_msgs {
        let (_src_addr, received_payload) = executor::block_on(rx.tcp_read());

        assert_eq!(received_payload, payload);
    }
}

#[test]
#[timeout(1000)]
fn tcp_rapid() {
    once_setup();

    let rx_addr = "127.0.0.1:9006".parse().unwrap();
    let tx_addr = "127.0.0.1:9007".parse().unwrap();
    let num_msgs = 1024;

    let mut rx = Dataplane::new(&rx_addr, UP_BANDWIDTH_MBPS);
    let mut tx = Dataplane::new(&tx_addr, UP_BANDWIDTH_MBPS);

    // Allow Dataplane threads to set themselves up.
    sleep(Duration::from_millis(10));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    for _ in 0..num_msgs {
        tx.tcp_write(
            rx_addr,
            TcpMsg {
                msg: payload.clone().into(),
                completion: None,
            },
        );
    }

    for _ in 0..num_msgs {
        let (_src_addr, received_payload) = executor::block_on(rx.tcp_read());

        assert_eq!(received_payload, payload);
    }
}

const MINIMUM_SEGMENT_SIZE: u16 = 256;

#[test]
#[timeout(30000)]
fn broadcast_all_strides() {
    once_setup();

    let rx_addr = "127.0.0.1:9008".parse().unwrap();
    let tx_addr = "127.0.0.1:9009".parse().unwrap();

    let mut rx = Dataplane::new(&rx_addr, UP_BANDWIDTH_MBPS);
    let mut tx = Dataplane::new(&tx_addr, UP_BANDWIDTH_MBPS);

    // Allow Dataplane threads to set themselves up.
    sleep(Duration::from_millis(10));

    let total_length: usize = 100000;

    let payload: Vec<u8> = (0..total_length)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    for stride in MINIMUM_SEGMENT_SIZE..=DEFAULT_SEGMENT_SIZE {
        tx.udp_write_broadcast(BroadcastMsg {
            targets: vec![rx_addr],
            payload: payload.clone().into(),
            stride,
        });

        let stride = stride.into();

        let num_msgs = total_length.div_ceil(stride);

        for i in 0..num_msgs {
            let msg: RecvMsg = executor::block_on(rx.udp_read());

            assert_eq!(msg.src_addr, tx_addr);
            assert_eq!(
                &msg.payload[..],
                &payload[i * stride..((i + 1) * stride).min(payload.len())]
            );
        }
    }
}

#[test]
#[timeout(30000)]
fn unicast_all_strides() {
    once_setup();

    let rx_addr = "127.0.0.1:9010".parse().unwrap();
    let tx_addr = "127.0.0.1:9011".parse().unwrap();

    let mut rx = Dataplane::new(&rx_addr, UP_BANDWIDTH_MBPS);
    let mut tx = Dataplane::new(&tx_addr, UP_BANDWIDTH_MBPS);

    // Allow Dataplane threads to set themselves up.
    sleep(Duration::from_millis(10));

    let total_length: usize = 100000;

    let payload: Vec<u8> = (0..total_length)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    for stride in MINIMUM_SEGMENT_SIZE..=DEFAULT_SEGMENT_SIZE {
        tx.udp_write_unicast(UnicastMsg {
            msgs: vec![(rx_addr, payload.clone().into())],
            stride,
        });

        let stride = stride.into();

        let num_msgs = total_length.div_ceil(stride);

        for i in 0..num_msgs {
            let msg: RecvMsg = executor::block_on(rx.udp_read());

            assert_eq!(msg.src_addr, tx_addr);
            assert_eq!(
                &msg.payload[..],
                &payload[i * stride..((i + 1) * stride).min(payload.len())]
            );
        }
    }
}
