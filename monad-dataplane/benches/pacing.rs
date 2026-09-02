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
    hint::black_box,
    net::{Ipv4Addr, SocketAddrV4},
    num::NonZeroU64,
};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use monad_dataplane::{
    pacing::{PacingItem, PacingQueue},
    DataplaneMetrics,
};
use monad_types::UdpPriority;

#[derive(Debug)]
struct Item(u16);

impl PacingItem for Item {
    fn next_payload_bytes(&self) -> usize {
        1_472
    }
}

fn scheduler(c: &mut Criterion) {
    const PEERS: usize = 256;
    let rate = NonZeroU64::new(125_000_000).unwrap();
    let mut queue = PacingQueue::new(rate, rate, usize::MAX, DataplaneMetrics::default());
    for peer in 0..PEERS {
        let key = SocketAddrV4::new(Ipv4Addr::LOCALHOST, peer as u16);
        queue
            .enqueue(key, UdpPriority::Regular, Item(peer as u16), 1_472)
            .unwrap();
    }
    let mut group = c.benchmark_group("dataplane/pacing");
    group.throughput(Throughput::Elements(1));
    group.bench_function("dequeue_and_requeue_256_peers_1gbps", |b| {
        b.iter(|| {
            let now = queue.elapsed();
            let scheduled = queue.dequeue(now, usize::MAX).unwrap();
            let item = black_box(scheduled.item);
            let peer = item.0;
            let key = SocketAddrV4::new(Ipv4Addr::LOCALHOST, peer);
            queue
                .enqueue(key, UdpPriority::Regular, item, 1_472)
                .unwrap();
            black_box(peer)
        });
    });
    group.finish();
}

criterion_group!(benches, scheduler);
criterion_main!(benches);
