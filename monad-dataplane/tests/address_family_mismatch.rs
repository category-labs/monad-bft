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

use std::{thread::sleep, time::Duration};

use monad_dataplane::{udp::DEFAULT_SEGMENT_SIZE, BroadcastMsg, DataplaneBuilder, UdpSocketId};
use tracing::debug;

/// 1_000 = 1 Gbps, 10_000 = 10 Gbps
const UP_BANDWIDTH_MBPS: u64 = 1_000;

const BIND_ADDRS: [&str; 2] = ["0.0.0.0:19100", "127.0.0.1:19101"];

fn find_ipv6_address() -> std::net::SocketAddr {
    let socket = std::net::UdpSocket::bind("[::1]:0").unwrap();
    socket.local_addr().unwrap()
}

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

    let ipv4_target: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let ipv6_target = find_ipv6_address();

    for addr in BIND_ADDRS {
        let bind_addr = addr.parse().unwrap();
        let mut dataplane = DataplaneBuilder::new(UP_BANDWIDTH_MBPS)
            .with_udp_sockets([(UdpSocketId::Raptorcast, bind_addr)])
            .build();

        assert!(dataplane.block_until_ready(Duration::from_secs(1)));

        let socket = dataplane.udp_sockets.take(UdpSocketId::Raptorcast).unwrap();
        let local_addr = socket.local_addr();

        for tx_addr in [ipv4_target, ipv6_target] {
            debug!("sending to {} from {}", tx_addr, local_addr);

            socket.write_broadcast(BroadcastMsg {
                targets: vec![tx_addr; 1],
                payload: vec![0; DEFAULT_SEGMENT_SIZE.into()].into(),
                stride: DEFAULT_SEGMENT_SIZE,
            });
        }

        sleep(Duration::from_millis(10));
    }
}
