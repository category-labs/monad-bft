// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::VecDeque, io::Write, net::TcpStream, sync::Once, thread::sleep, time::Duration,
};

use futures::{channel::oneshot, executor, FutureExt};
use monad_dataplane::{
    tcp::tx::{MSG_WAIT_TIMEOUT, QUEUED_MESSAGE_LIMIT},
    udp::DEFAULT_SEGMENT_SIZE,
    BroadcastMsg, DataplaneBuilder, RecvUdpMsg, TcpMsg, UnicastMsg,
};
use ntest::timeout;
use rand::Rng;
use rstest::*;
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

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    tx.udp_write_broadcast(BroadcastMsg {
        targets: vec![rx_addr; num_msgs],
        payload: payload.clone().into(),
        stride: DEFAULT_SEGMENT_SIZE,
    });

    for _ in 0..num_msgs {
        let msg: RecvUdpMsg = executor::block_on(rx.udp_read());

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

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    tx.udp_write_unicast(UnicastMsg {
        msgs: vec![(rx_addr, payload.clone().into()); num_msgs],
        stride: DEFAULT_SEGMENT_SIZE,
    });

    for _ in 0..num_msgs {
        let msg: RecvUdpMsg = executor::block_on(rx.udp_read());

        assert_eq!(msg.src_addr, tx_addr);
        assert_eq!(msg.payload, payload);
    }
}

#[test]
#[timeout(1000)]
fn udp_direct_socket() {
    once_setup();

    let rx_addr = "127.0.0.1:9030".parse().unwrap();
    let rx_direct_port = 9031;
    let tx_addr = "127.0.0.1:9032".parse().unwrap();
    let tx_direct_port = 9033;
    let num_msgs = 10;

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .with_direct_socket(rx_direct_port)
        .build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS)
        .with_direct_socket(tx_direct_port)
        .build();

    assert!(rx.block_until_ready(Duration::from_secs(2)));
    assert!(tx.block_until_ready(Duration::from_secs(2)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    tx.udp_write_broadcast(BroadcastMsg {
        targets: vec![rx_addr; num_msgs / 2],
        payload: payload.clone().into(),
        stride: DEFAULT_SEGMENT_SIZE,
    });

    let mut rx_direct_addr = rx_addr;
    rx_direct_addr.set_port(rx_direct_port);

    for _ in 0..num_msgs / 2 {
        tx.udp_write_direct(rx_direct_addr, payload.clone().into(), DEFAULT_SEGMENT_SIZE);
    }

    for _ in 0..num_msgs / 2 {
        let msg: RecvUdpMsg = executor::block_on(rx.udp_read());
        assert_eq!(msg.src_addr, tx_addr);
        assert_eq!(msg.payload, payload);
    }

    for _ in 0..num_msgs / 2 {
        let msg: RecvUdpMsg = executor::block_on(rx.udp_direct_read());
        assert_eq!(msg.src_addr.ip(), tx_addr.ip());
        assert_eq!(msg.payload, payload);
    }
}

// This verifies that the TCP transmit task recovers from a peer transmit
// task exiting after a timeout.
#[test]
#[timeout(10000)]
fn tcp_very_slow() {
    once_setup();

    let rx_addr = "127.0.0.1:9004".parse().unwrap();
    let tx_addr = "127.0.0.1:9005".parse().unwrap();
    let num_msgs = 2;

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    for _ in 0..num_msgs {
        let (sender, receiver) = oneshot::channel::<()>();

        tx.tcp_write(
            rx_addr,
            TcpMsg {
                msg: payload.clone().into(),
                completion: Some(sender),
            },
        );

        assert!(executor::block_on(receiver).is_ok());

        sleep(2 * MSG_WAIT_TIMEOUT);
    }

    for _ in 0..num_msgs {
        let recv_msg = executor::block_on(rx.tcp_read());

        assert_eq!(recv_msg.payload, payload);
    }
}

// This should exercise the sleep-for-a-new-message logic in the peer transmit task.
#[test]
#[timeout(1000)]
fn tcp_slow() {
    once_setup();

    let rx_addr = "127.0.0.1:9006".parse().unwrap();
    let tx_addr = "127.0.0.1:9007".parse().unwrap();
    let num_msgs = 10;

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    for _ in 0..num_msgs {
        let (sender, receiver) = oneshot::channel::<()>();

        tx.tcp_write(
            rx_addr,
            TcpMsg {
                msg: payload.clone().into(),
                completion: Some(sender),
            },
        );

        assert!(executor::block_on(receiver).is_ok());
    }

    for _ in 0..num_msgs {
        let recv_msg = executor::block_on(rx.tcp_read());

        assert_eq!(recv_msg.payload, payload);
    }
}

#[test]
#[timeout(1000)]
fn tcp_rapid() {
    once_setup();

    let rx_addr = "127.0.0.1:9008".parse().unwrap();
    let tx_addr = "127.0.0.1:9009".parse().unwrap();
    let num_msgs = 1024;

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    let mut completions = VecDeque::with_capacity(QUEUED_MESSAGE_LIMIT);

    for _ in 0..num_msgs {
        let (sender, receiver) = oneshot::channel::<()>();

        tx.tcp_write(
            rx_addr,
            TcpMsg {
                msg: payload.clone().into(),
                completion: Some(sender),
            },
        );

        completions.push_back(receiver);

        while completions.len() >= QUEUED_MESSAGE_LIMIT {
            assert!(executor::block_on(completions.pop_front().unwrap()).is_ok());
        }
    }

    while !completions.is_empty() {
        assert!(executor::block_on(completions.pop_front().unwrap()).is_ok());
    }

    for _ in 0..num_msgs {
        let recv_msg = executor::block_on(rx.tcp_read());

        assert_eq!(recv_msg.payload, payload);
    }
}

#[test]
#[timeout(1000)]
fn tcp_connect_fail() {
    once_setup();

    let rx_addr = "127.0.0.1:9010".parse().unwrap();
    let tx_addr = "127.0.0.1:9011".parse().unwrap();

    // let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    let (sender, receiver) = oneshot::channel::<()>();

    tx.tcp_write(
        rx_addr,
        TcpMsg {
            msg: payload.into(),
            completion: Some(sender),
        },
    );

    assert!(executor::block_on(receiver).is_err());
}

#[test]
#[timeout(1000)]
fn tcp_exceed_queue_limits() {
    once_setup();

    let rx_addr = "127.0.0.1:9012".parse().unwrap();
    let tx_addr = "127.0.0.1:9013".parse().unwrap();
    let num_msgs = 100 * QUEUED_MESSAGE_LIMIT;

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..DEFAULT_SEGMENT_SIZE)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    let mut completions = Vec::with_capacity(num_msgs);

    for _ in 0..num_msgs {
        let (sender, receiver) = oneshot::channel::<()>();

        tx.tcp_write(
            rx_addr,
            TcpMsg {
                msg: payload.clone().into(),
                completion: Some(sender),
            },
        );

        completions.push(receiver);
    }

    // At least QUEUED_MESSAGE_LIMIT messages should be delivered successfully.
    for _ in 0..QUEUED_MESSAGE_LIMIT {
        let recv_msg = executor::block_on(rx.tcp_read());

        assert_eq!(recv_msg.payload, payload);
    }

    let failures: usize = completions
        .into_iter()
        .map(|receiver| {
            if executor::block_on(receiver).is_err() {
                1
            } else {
                0
            }
        })
        .sum();

    // We should have at least some messages that failed to transmit.
    assert_ne!(failures, 0);
}

const MINIMUM_SEGMENT_SIZE: u16 = 256;

#[test]
#[timeout(1000)]
fn tcp_reject_oversized_message() {
    once_setup();

    let rx_addr = "127.0.0.1:9018".parse().unwrap();
    let tx_addr = "127.0.0.1:9019".parse().unwrap();

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let oversized_payload = vec![0u8; 3 * 1024 * 1024 + 1];

    tx.tcp_write(
        rx_addr,
        TcpMsg {
            msg: oversized_payload.into(),
            completion: None,
        },
    );

    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_millis(100) {
        if rx.tcp_read().now_or_never().is_some() {
            panic!("expected no message but received one");
        }
    }
}

#[test]
#[timeout(5000)]
fn tcp_accept_max_size_message() {
    once_setup();

    let rx_addr = "127.0.0.1:9020".parse().unwrap();
    let tx_addr = "127.0.0.1:9021".parse().unwrap();

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let max_size_payload = vec![0u8; 3 * 1024 * 1024];

    let (sender, receiver) = oneshot::channel::<()>();

    tx.tcp_write(
        rx_addr,
        TcpMsg {
            msg: max_size_payload.clone().into(),
            completion: Some(sender),
        },
    );

    assert!(executor::block_on(receiver).is_ok());

    let recv_msg = executor::block_on(rx.tcp_read());
    assert_eq!(recv_msg.payload, max_size_payload);
}

#[test]
#[timeout(1000)]
fn tcp_rx_reject_oversized_header() {
    once_setup();

    let rx_addr = "127.0.0.1:19022".parse().unwrap();

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    assert!(rx.block_until_ready(Duration::from_secs(1)));

    let mut tcp_stream = TcpStream::connect(rx_addr).unwrap();

    let header_magic: u32 = 0x434e5353;
    let header_version: u32 = 1;
    let oversized_length: u64 = (3 * 1024 * 1024 + 1) as u64;

    tcp_stream.write_all(&header_magic.to_le_bytes()).unwrap();
    tcp_stream.write_all(&header_version.to_le_bytes()).unwrap();
    tcp_stream
        .write_all(&oversized_length.to_le_bytes())
        .unwrap();
    tcp_stream.flush().unwrap();

    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_millis(100) {
        if rx.tcp_read().now_or_never().is_some() {
            panic!("Expected no message but received one");
        }
        sleep(Duration::from_millis(10));
    }
}

#[test]
#[timeout(30000)]
fn broadcast_all_strides() {
    once_setup();

    let rx_addr = "127.0.0.1:9014".parse().unwrap();
    let tx_addr = "127.0.0.1:9015".parse().unwrap();

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .with_udp_buffer_size(400 << 10)
        .build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

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
            let msg: RecvUdpMsg = executor::block_on(rx.udp_read());

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

    let rx_addr = "127.0.0.1:9016".parse().unwrap();
    let tx_addr = "127.0.0.1:9017".parse().unwrap();

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .with_udp_buffer_size(400 << 10)
        .build();
    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();

    // Allow Dataplane threads to set themselves up.
    assert!(rx.block_until_ready(Duration::from_secs(1)));
    assert!(tx.block_until_ready(Duration::from_secs(1)));

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
            let msg: RecvUdpMsg = executor::block_on(rx.udp_read());

            assert_eq!(msg.src_addr, tx_addr);
            assert_eq!(
                &msg.payload[..],
                &payload[i * stride..((i + 1) * stride).min(payload.len())]
            );
        }
    }
}

fn find_unused_address() -> std::net::SocketAddr {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap()
}

#[rstest]
#[case::total_limit_applied(1, 100)]
#[case::per_ip_limit(10000, 1)]
#[monoio::test(timer_enabled = true)]
async fn test_tcp_limits_are_applied(
    #[case] tcp_connection_limit: usize,
    #[case] tcp_per_ip_connection_limit: usize,
) {
    once_setup();

    let rx_addr = find_unused_address();
    let tx1_addr = "127.0.0.1:0".parse().unwrap();
    let tx2_addr = "127.0.0.1:0".parse().unwrap();

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .with_tcp_connections_limit(tcp_connection_limit, tcp_per_ip_connection_limit)
        .build();

    let tx1 = DataplaneBuilder::new(&tx1_addr, UP_BANDWIDTH_MBPS).build();
    let tx2 = DataplaneBuilder::new(&tx2_addr, UP_BANDWIDTH_MBPS).build();

    assert!(rx.block_until_ready(Duration::from_secs(1)));

    let payload1: Vec<u8> = "first message".into();

    tx1.tcp_write(
        rx_addr,
        TcpMsg {
            msg: payload1.clone().into(),
            completion: None,
        },
    );

    let recv_msg = rx.tcp_read().await;
    assert_eq!(recv_msg.payload, payload1);

    let payload2: Vec<u8> = "second message".into();
    tx2.tcp_write(
        rx_addr,
        TcpMsg {
            msg: payload2.clone().into(),
            completion: None,
        },
    );

    let result = async {
        monoio::time::timeout(Duration::from_millis(50), rx.tcp_read())
            .await
            .ok()
    }
    .await;

    assert!(result.is_none());
}

#[rstest]
#[monoio::test(timer_enabled = true)]
async fn test_tcp_rps_limits() {
    once_setup();

    let rx_addr = find_unused_address();
    let tx1_addr = "127.0.0.1:0".parse().unwrap();

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .with_tcp_rps_burst(10, 2)
        .build();

    let tx1 = DataplaneBuilder::new(&tx1_addr, UP_BANDWIDTH_MBPS).build();

    assert!(rx.block_until_ready(Duration::from_secs(1)));

    let num_messages = 2;
    for i in 0..num_messages {
        let payload = format!("message {}", i).into_bytes();
        tx1.tcp_write(
            rx_addr,
            TcpMsg {
                msg: payload.into(),
                completion: None,
            },
        );
    }

    for i in 0..2 {
        let recv_msg = rx.tcp_read().await;
        let expected = format!("message {}", i).into_bytes();
        assert_eq!(recv_msg.payload, expected);
    }

    let result = async {
        monoio::time::timeout(Duration::from_millis(50), rx.tcp_read())
            .await
            .ok()
    }
    .await;
    assert!(result.is_none());
}

#[rstest]
#[case::total_limit_applied(1, 100)]
#[case::per_ip_limit(10000, 1)]
#[monoio::test(timer_enabled = true)]
async fn test_tcp_limits_ignored_for_trusted(
    #[case] tcp_connection_limit: usize,
    #[case] tcp_per_ip_connection_limit: usize,
) {
    once_setup();

    let rx_addr = find_unused_address();
    let tx1_addr = "127.0.0.1:0".parse().unwrap();
    let tx2_addr = "127.0.0.1:0".parse().unwrap();

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS)
        .with_tcp_connections_limit(tcp_connection_limit, tcp_per_ip_connection_limit)
        .build();
    rx.add_trusted("127.0.0.1".parse().unwrap());

    let tx1 = DataplaneBuilder::new(&tx1_addr, UP_BANDWIDTH_MBPS).build();
    let tx2 = DataplaneBuilder::new(&tx2_addr, UP_BANDWIDTH_MBPS).build();

    assert!(rx.block_until_ready(Duration::from_secs(1)));

    let payload1: Vec<u8> = "first message".into();

    tx1.tcp_write(
        rx_addr,
        TcpMsg {
            msg: payload1.clone().into(),
            completion: None,
        },
    );

    let recv_msg = rx.tcp_read().await;
    assert_eq!(recv_msg.payload, payload1);

    let payload2: Vec<u8> = "second message".into();
    tx2.tcp_write(
        rx_addr,
        TcpMsg {
            msg: payload2.clone().into(),
            completion: None,
        },
    );

    let recv_msg = rx.tcp_read().await;
    assert_eq!(recv_msg.payload, payload2);
}

#[monoio::test(timer_enabled = true)]
async fn test_tcp_banned() {
    once_setup();

    let rx_addr = find_unused_address();
    let tx1_addr = "127.0.0.1:0".parse().unwrap();

    let mut rx = DataplaneBuilder::new(&rx_addr, UP_BANDWIDTH_MBPS).build();
    let tx1 = DataplaneBuilder::new(&tx1_addr, UP_BANDWIDTH_MBPS).build();

    assert!(rx.block_until_ready(Duration::from_secs(1)));

    let payload1: Vec<u8> = "first message".into();
    tx1.tcp_write(
        rx_addr,
        TcpMsg {
            msg: payload1.clone().into(),
            completion: None,
        },
    );
    let recv_msg = rx.tcp_read().await;
    assert_eq!(recv_msg.payload, payload1);

    // once banned all further message will be dropped for next 5 minutes
    rx.ban("127.0.0.1".parse().unwrap());

    let payload2: Vec<u8> = "second message".into();
    tx1.tcp_write(
        rx_addr,
        TcpMsg {
            msg: payload2.clone().into(),
            completion: None,
        },
    );
    let result = async {
        monoio::time::timeout(Duration::from_millis(50), rx.tcp_read())
            .await
            .ok()
    }
    .await;
    assert!(result.is_none());
}

#[test]
#[timeout(1000)]
fn udp_large_stride() {
    once_setup();

    let rx_addr = "127.0.0.1:9034".parse().unwrap();
    let tx_addr = "127.0.0.1:9035".parse().unwrap();

    let rx_socket = std::net::UdpSocket::bind(rx_addr).unwrap();
    rx_socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let tx = DataplaneBuilder::new(&tx_addr, UP_BANDWIDTH_MBPS).build();
    assert!(tx.block_until_ready(Duration::from_secs(1)));

    let payload: Vec<u8> = (0..65536)
        .map(|_| rand::thread_rng().gen_range(0..255))
        .collect();

    tx.udp_write_broadcast(BroadcastMsg {
        targets: vec![rx_addr],
        payload: payload.clone().into(),
        stride: u16::MAX,
    });

    let mut received = Vec::new();
    let mut buf = [0u8; 65536];
    let mut datagram_count = 0;

    while received.len() < payload.len() {
        match rx_socket.recv_from(&mut buf) {
            Ok((len, _)) => {
                received.extend_from_slice(&buf[..len]);
                datagram_count += 1;
            }
            Err(_) => break,
        }
    }

    assert_eq!(received, payload);
    assert_eq!(datagram_count, 2);
}
