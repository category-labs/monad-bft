use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use monad_leanudp::{Clock, Config, Decoder, Encoder, FragmentPolicy, IdentityScore, PacketHeader};

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
    let mut buf = BytesMut::with_capacity(8 + payload.len());
    buf.put_slice(zerocopy::IntoBytes::as_bytes(header));
    buf.put_slice(payload);
    buf.freeze()
}

fn make_config(max_regular_messages: usize) -> Config {
    Config {
        max_message_size: 128 * 1024,
        max_priority_messages: 100,
        max_regular_messages,
        max_messages_per_identity: 10,
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
    for i in 0..count {
        let identity = start_identity + i as u64;
        let payload = Bytes::from(vec![0u8; 100]);
        let fragments: Vec<_> = encoder.fragment(payload).unwrap().collect();
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

criterion_group!(benches, bench_decode, bench_encode);
criterion_main!(benches);
