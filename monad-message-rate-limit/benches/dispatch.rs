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

use std::{hint::black_box, num::NonZeroU32, time::Duration};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use governor::Quota;
use monad_message_rate_limit::{MessageKind, RateLimiter, Rejection, DEFAULT_CAPACITY};
use rand::{seq::SliceRandom, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

const INTERLEAVED_BYTES: usize = 10 * 1024;
// Secp NodeId hashes the public key's 64-byte in-memory representation.
type Identity = [u8; 64];

struct Fixture {
    limiter: RateLimiter<Identity>,
    identities: Vec<Identity>,
    allocations: Vec<Box<[u8]>>,
}

fn setup(consume_token: bool) -> Fixture {
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut identities: Vec<_> = (0..DEFAULT_CAPACITY)
        .map(|index| {
            let mut identity = [0; 64];
            rng.fill_bytes(&mut identity);
            identity[..8].copy_from_slice(&(index as u64).to_le_bytes());
            identity
        })
        .collect();
    let quota = if consume_token {
        Quota::per_second(NonZeroU32::new(1_000_000_000).unwrap())
            .allow_burst(NonZeroU32::new(u32::MAX).unwrap())
    } else {
        Quota::per_hour(NonZeroU32::new(1).unwrap())
    };
    // Keep the production registry's bucket layout. Replace the selected quota
    // so each benchmark measures a stable admitted or rejected path.
    let mut limiter = RateLimiter::registered();
    limiter.register(MessageKind::VOTE, quota);
    let mut allocations = Vec::with_capacity(DEFAULT_CAPACITY);
    for identity in &identities {
        assert_eq!(limiter.check(identity, MessageKind::VOTE), Ok(()));
        // Touch every page and retain allocations throughout measurement.
        allocations.push(vec![0xa5; INTERLEAVED_BYTES].into_boxed_slice());
    }
    // Visit all identities, not just a hot key.
    identities.shuffle(&mut rng);
    Fixture {
        limiter,
        identities,
        allocations,
    }
}

fn dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatch_20k_interleaved_10kib");
    group.throughput(Throughput::Elements(1));
    for consume_token in [true, false] {
        let mut fixture = setup(consume_token);
        let name = if consume_token {
            "token_consumed"
        } else {
            "token_not_consumed"
        };
        let expected = if consume_token {
            Ok(())
        } else {
            Err(Rejection::RateLimited)
        };
        let mut index = 0;
        group.bench_function(name, |b| {
            b.iter(|| {
                let result = fixture.limiter.check(
                    black_box(&fixture.identities[index]),
                    black_box(MessageKind::VOTE),
                );
                assert_eq!(result, expected);
                index += 1;
                if index == fixture.identities.len() {
                    index = 0;
                }
                black_box(result)
            });
        });
        black_box(&fixture.allocations);
    }

    let mut fixture = setup(true);
    let mut next = DEFAULT_CAPACITY as u64;
    let mut allocation_index = 0;
    group.bench_function("eviction", |b| {
        b.iter_batched(
            || {
                // Interleave a live 10 KiB allocation between timed inserts.
                // Rotate retained chunks to bound memory; allocation is untimed.
                fixture.allocations[allocation_index] =
                    vec![0xa5; INTERLEAVED_BYTES].into_boxed_slice();
                allocation_index = (allocation_index + 1) % DEFAULT_CAPACITY;
                let mut identity = [0; 64];
                identity[..8].copy_from_slice(&next.to_le_bytes());
                next += 1;
                identity
            },
            |identity| {
                let result = fixture
                    .limiter
                    .check(black_box(&identity), black_box(MessageKind::VOTE));
                assert_eq!(result, Ok(()));
                black_box(result)
            },
            BatchSize::PerIteration,
        );
    });
    black_box(&fixture.allocations);
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(50);
    targets = dispatch
}
criterion_main!(benches);
