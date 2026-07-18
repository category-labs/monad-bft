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
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use monad_dataplane::pacing::{BatchLimits, PacingQueue, QueueCost};
use monad_types::UdpPriority;

fn scheduler(c: &mut Criterion) {
    const PEERS: usize = 256;
    let rate = NonZeroU64::new(125_000_000).unwrap();
    let wire_bytes = NonZeroUsize::new(1_500).unwrap();
    let cost = QueueCost {
        wire_bytes,
        batch_bytes: wire_bytes.get(),
        memory_bytes: wire_bytes.get(),
    };
    let mut queue = PacingQueue::new(rate, rate, usize::MAX);
    for peer in 0..PEERS {
        let key = SocketAddr::from(([127, 0, 0, 1], peer as u16));
        queue
            .enqueue(key, UdpPriority::Regular, peer as u64, cost)
            .unwrap();
    }
    let batch_limits = BatchLimits {
        max_bytes: usize::MAX,
        max_items: 1,
    };

    let mut group = c.benchmark_group("dataplane/pacing");
    group.throughput(Throughput::Elements(1));
    group.bench_function("dequeue_and_requeue_256_peers_1gbps", |b| {
        b.iter(|| {
            let mut batch = queue.batch(batch_limits);
            let scheduled = batch.next().unwrap();
            let item = black_box(scheduled.item);
            let key = SocketAddr::from(([127, 0, 0, 1], item as u16));
            batch
                .reenqueue(key, UdpPriority::Regular, item, cost)
                .unwrap();
            black_box(item)
        });
    });
    group.finish();
}

criterion_group!(benches, scheduler);
criterion_main!(benches);
