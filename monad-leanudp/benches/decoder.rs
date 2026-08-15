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
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use monad_leanudp::{
    Clock, Config, Decoder, Encoder, FragmentPolicy, IdentityScore, PacketHeader,
    MAX_CONCURRENT_MESSAGES_PER_IDENTITY,
};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};

#[cfg(all(not(target_env = "msvc"), feature = "jemallocator"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "jemallocator")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

#[derive(Clone, Copy)]
struct FixedClock(Instant);

impl Clock for FixedClock {
    fn now(&self) -> Instant {
        self.0
    }
}

struct AllRegular;

impl IdentityScore for AllRegular {
    type Identity = u64;
    fn score(&self, _: &u64) -> FragmentPolicy {
        FragmentPolicy::Regular
    }
}

fn make_packet(header: &PacketHeader, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(PacketHeader::SIZE + payload.len());
    buf.put_slice(zerocopy::IntoBytes::as_bytes(header));
    buf.put_slice(payload);
    buf.freeze()
}

fn make_config(max_regular_messages: usize) -> Config {
    Config {
        max_message_size: 128 * 1024,
        max_priority_messages: 100,
        max_regular_messages,
        max_messages_per_identity: MAX_CONCURRENT_MESSAGES_PER_IDENTITY,
        message_timeout: Duration::from_secs(60),
        max_fragment_payload: 1440,
    }
}

fn fill_pool<P, C>(
    decoder: &mut Decoder<u64, P, C>,
    encoder: &mut Encoder,
    count: usize,
    start_identity: u64,
) where
    P: IdentityScore<Identity = u64>,
    C: Clock,
{
    // Must span multiple fragments so injecting only fragment[0]
    // leaves each message pending in the pool.
    let payload = Bytes::from(vec![0u8; 3_000]);
    for i in 0..count {
        let identity = start_identity + i as u64;
        let fragments: Vec<_> = encoder.fragment(payload.clone()).unwrap().collect();
        debug_assert!(fragments.len() > 1);
        let (header, data) = &fragments[0];
        let packet = make_packet(header, data);
        let _ = decoder.decode(identity, packet);
    }
}

fn encode_packets(encoder: &mut Encoder, size: usize) -> Vec<Bytes> {
    let payload = Bytes::from(vec![0xAB; size]);
    encoder
        .fragment(payload)
        .unwrap()
        .map(|(header, data)| make_packet(&header, &data))
        .collect()
}

#[derive(Clone, Copy)]
enum Order {
    InOrder,
    Shuffled,
}

impl Order {
    fn as_str(self) -> &'static str {
        match self {
            Self::InOrder => "in_order",
            Self::Shuffled => "shuffled",
        }
    }
}

struct WorstCaseBenchCase {
    name: &'static str,
    payload_size: usize,
    max_fragment_payload: usize,
}

fn worst_case_bench_config(case: &WorstCaseBenchCase) -> Config {
    Config {
        max_message_size: case.payload_size,
        max_priority_messages: 8,
        max_regular_messages: 8,
        max_messages_per_identity: MAX_CONCURRENT_MESSAGES_PER_IDENTITY,
        message_timeout: Duration::from_secs(60),
        max_fragment_payload: case.max_fragment_payload,
    }
}

fn make_worst_case_bench_packets(case: &WorstCaseBenchCase, order: Order) -> Vec<Bytes> {
    let config = worst_case_bench_config(case);
    let clock = FixedClock(Instant::now());
    let (mut encoder, _decoder) = config.build_with_clock(AllRegular, clock);
    let mut packets = encode_packets(&mut encoder, case.payload_size);
    if matches!(order, Order::Shuffled) {
        let mut rng = StdRng::seed_from_u64(7);
        packets.shuffle(&mut rng);
    }
    packets
}

fn bench_worst_case_case(
    b: &mut criterion::Bencher<'_>,
    packets: &[Bytes],
    case: &WorstCaseBenchCase,
) {
    let config = worst_case_bench_config(case);
    let clock = FixedClock(Instant::now());
    let (_encoder, mut decoder) = config.build_with_clock(AllRegular, clock);
    let mut identity = 0u64;

    b.iter(|| {
        for packet in packets {
            let _ = black_box(decoder.decode(identity, packet.clone()));
        }
        identity += 1;
    });
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    for num_fragments in [1, 2, 4, 8, 16] {
        let message_size = num_fragments * 1400;
        group.throughput(Throughput::Bytes(message_size as u64));

        group.bench_with_input(
            BenchmarkId::new("half_full_pool", num_fragments),
            &num_fragments,
            |b, &num_frags| {
                let msg_size = num_frags * 1400;
                let config = make_config(200_000);
                let clock = FixedClock(Instant::now());
                let (mut encoder, mut decoder) = config.build_with_clock(AllRegular, clock);

                // Fill pool with 100K incomplete messages (identities 1M+)
                // These stay in the pool throughout the benchmark
                fill_pool(&mut decoder, &mut encoder, 100_000, 1_000_000);

                // Pre-encode test packets
                let packets = encode_packets(&mut encoder, msg_size);
                assert_eq!(packets.len(), num_frags);

                // Each iteration uses a fresh identity (0, 1, 2, ...)
                // Message completes and is removed, pool stays at ~100K
                let mut identity = 0u64;
                b.iter(|| {
                    for packet in &packets {
                        let _ = black_box(decoder.decode(identity, packet.clone()));
                    }
                    identity += 1;
                });
            },
        );
    }

    group.finish();
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    for num_fragments in [1, 2, 4, 8, 16] {
        let message_size = num_fragments * 1400;
        group.throughput(Throughput::Bytes(message_size as u64));

        group.bench_with_input(
            BenchmarkId::new("fragments", num_fragments),
            &num_fragments,
            |b, &num_frags| {
                let msg_size = num_frags * 1400;
                let config = make_config(1000);
                let clock = FixedClock(Instant::now());
                let (mut encoder, _decoder) = config.build_with_clock(AllRegular, clock);
                let payload = Bytes::from(vec![0xAB; msg_size]);

                b.iter(|| {
                    let frags: Vec<_> = encoder
                        .fragment(black_box(payload.clone()))
                        .unwrap()
                        .collect();
                    assert_eq!(frags.len(), num_frags);
                    frags
                });
            },
        );
    }

    group.finish();
}

fn bench_worst_case_bench(c: &mut Criterion) {
    let cases = [
        WorstCaseBenchCase {
            name: "512frags_1byte",
            payload_size: 512,
            max_fragment_payload: PacketHeader::SIZE + 1,
        },
        WorstCaseBenchCase {
            name: "366frags_default",
            payload_size: 512 * 1024,
            max_fragment_payload: 1440,
        },
        WorstCaseBenchCase {
            name: "512frags_worst",
            payload_size: 512 * 1024,
            max_fragment_payload: 1030,
        },
    ];

    for order in [Order::InOrder, Order::Shuffled] {
        let mut group = c.benchmark_group(format!("worst_case_bench/{}", order.as_str()));
        group.sample_size(30);
        group.measurement_time(Duration::from_secs(8));

        for case in &cases {
            let packets = make_worst_case_bench_packets(case, order);
            group.throughput(Throughput::Bytes(case.payload_size as u64));

            group.bench_with_input(
                BenchmarkId::new(case.name, packets.len()),
                &packets,
                |b, packets| bench_worst_case_case(b, packets, case),
            );
        }

        group.finish();
    }
}

criterion_group!(benches, bench_decode, bench_encode, bench_worst_case_bench);
criterion_main!(benches);
