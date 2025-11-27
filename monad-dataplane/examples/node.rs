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
    net::SocketAddr,
    ops::DerefMut,
    pin::{pin, Pin},
    task::Poll,
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use futures::{executor, Stream};
use futures_util::FutureExt;
use monad_dataplane::{
    udp::DEFAULT_SEGMENT_SIZE, BroadcastMsg, DataplaneBuilder, RecvUdpMsg, TcpMsg, TcpSocketConfig,
    TcpSocketHandle, TcpSocketType, UdpSocketConfig, UdpSocketHandle, UdpSocketType,
};
use rand::Rng;

const NODE_ONE_ADDR: &str = "127.0.0.1:60000";
const NODE_TWO_ADDR: &str = "127.0.0.1:60001";

fn main() {
    env_logger::init();
    let tx = Node::new(&NODE_ONE_ADDR.parse().unwrap(), NODE_TWO_ADDR);
    let mut rx = Node::new(&NODE_TWO_ADDR.parse().unwrap(), NODE_ONE_ADDR);

    let num_pkts = 10;
    let pkt_size = 96342;

    println!(
        "sending {} pkts, {} bytes \n",
        num_pkts,
        num_pkts * pkt_size
    );

    let t2 = std::thread::spawn(move || {
        let mut rx_cnt = 0;
        let mut rx_bytes = 0;

        loop {
            let recv = executor::block_on_stream(&mut rx).next();
            let Some(rx_msg) = recv else {
                panic!();
            };

            rx_cnt += 1;
            rx_bytes += rx_msg.payload.len();

            if rx_bytes >= num_pkts * pkt_size {
                let end = Instant::now();
                println!("END: {:?}", end);
                println!("\nRXer: cnt={}, bytes={}", rx_cnt, rx_bytes);
                break;
            }
        }

        std::thread::sleep(std::time::Duration::from_secs(5));
    });

    let mut rng = rand::thread_rng();
    let rand_values: Vec<u8> = (0..num_pkts * pkt_size)
        .map(|_| rng.gen_range(0..255))
        .collect();

    let buf = BytesMut::from_iter(rand_values.iter());

    let t1 = std::thread::spawn(move || {
        let b = buf.freeze();

        println!("START: {:?}", Instant::now());
        for i in 0..num_pkts {
            tx.udp_socket.write_broadcast(BroadcastMsg {
                targets: vec![tx.target],
                payload: b.slice(i * pkt_size..(i + 1) * pkt_size),
                stride: DEFAULT_SEGMENT_SIZE,
            })
        }

        tx.tcp_socket.write(
            tx.target,
            TcpMsg {
                msg: Bytes::from(&b"Hello world"[..]),
                completion: None,
            },
        );

        std::thread::sleep(std::time::Duration::from_secs(5));
    });

    t1.join().unwrap();
    t2.join().unwrap();

    println!("run complete");
}

struct Node {
    udp_socket: UdpSocketHandle,
    tcp_socket: TcpSocketHandle,
    target: SocketAddr,
}

impl Node {
    pub fn new(addr: &SocketAddr, target_addr: &str) -> Self {
        let mut dataplane = DataplaneBuilder::new(1_000)
            .extend_udp_sockets(vec![UdpSocketConfig {
                socket_addr: *addr,
                socket: UdpSocketType::Original,
            }])
            .extend_tcp_sockets(vec![TcpSocketConfig {
                socket_addr: *addr,
                socket: TcpSocketType::Original,
            }])
            .build();
        let udp_socket = dataplane
            .take_udp_socket(UdpSocketType::Original)
            .expect("legacy socket");
        let tcp_socket = dataplane
            .take_tcp_socket(TcpSocketType::Original)
            .expect("tcp socket");
        Self {
            udp_socket,
            tcp_socket,
            target: target_addr.parse().unwrap(),
        }
    }
}

impl Stream for Node {
    type Item = RecvUdpMsg;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Poll::Ready(message) = pin!(this.udp_socket.recv()).poll_unpin(cx) {
            return Poll::Ready(Some(message));
        }
        Poll::Pending
    }
}
