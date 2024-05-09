use std::{net::SocketAddr, ops::DerefMut, pin::Pin, task::Poll, time::Instant};

use bytes::BytesMut;
use futures::{executor, Stream};
use futures_util::{FutureExt, StreamExt};
use monad_dataplane::event_loop::{BroadcastMsg, Dataplane, RecvMsg};
use rand::Rng;

const NODE_ONE_ADDR: &str = "127.0.0.1:60000";
const NODE_TWO_ADDR: &str = "127.0.0.1:60001";

fn main() {
    env_logger::init();
    let mut tx = Node::new(NODE_ONE_ADDR, NODE_TWO_ADDR);
    let mut rx = Node::new(NODE_TWO_ADDR, NODE_ONE_ADDR);

    let num_pkts = 1000;
    let pkt_size = 1460;

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

            if rx_cnt >= num_pkts {
                let end = Instant::now();
                println!("END: {:?}", end);
                println!("\nRXer: cnt={}, bytes={}", rx_cnt, rx_bytes);
                break;
            }
        }
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
            tx.network.broadcast(BroadcastMsg {
                targets: vec![tx.target],
                payload: b.slice(i * pkt_size..(i + 1) * pkt_size),
            })
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    println!("run complete");
}

struct Node {
    network: Dataplane,
    target: SocketAddr,
}

impl Node {
    pub fn new(addr: &str, target_addr: &str) -> Self {
        Self {
            network: Dataplane::new(addr),
            target: target_addr.parse().unwrap(),
        }
    }
}

impl Stream for Node {
    type Item = RecvMsg;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        futures::future::select_all(vec![this.network.next().boxed_local()])
            .map(|(event, _, _)| event)
            .poll_unpin(cx)
    }
}
