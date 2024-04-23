use std::time::Duration;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignatureRecoverable,
    },
    hasher::{Hasher, HasherType},
    NopSignature,
};
use monad_gossip::seeder::{Chunker, Raptor};
use monad_secp::SecpSignature;
use monad_types::NodeId;
use raptorq::Encoder;

#[allow(clippy::useless_vec)]
pub fn criterion_benchmark(c: &mut Criterion) {
    let message_size = 10_000 * 400;
    let message: Bytes = vec![123_u8; message_size].into();

    let mut group = c.benchmark_group("seeder");
    group.throughput(Throughput::Bytes(message_size as u64));
    group.bench_function("Encoding", |b| {
        b.iter(|| {
            let raptor_symbol_size = 1200;
            let _ = Encoder::with_defaults(&message, raptor_symbol_size.try_into().unwrap());
        });
    });
    group.bench_function("NopSignature", |b| {
        raptor_bench::<NopSignature>(b, message.clone())
    });
    group.bench_function("SecpSignature", |b| {
        raptor_bench::<SecpSignature>(b, message.clone())
    });
    group.finish();
}

fn raptor_bench<ST: CertificateSignatureRecoverable>(b: &mut Bencher, message: Bytes) {
    let all_keys: Vec<_> = (1_u64..=2)
        .map(|n| {
            let mut hasher = HasherType::new();
            hasher.update(n.to_le_bytes());
            let mut hash = hasher.hash();
            <<ST as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
                &mut hash.0,
            )
            .unwrap()
        })
        .collect();

    let all_peers: Vec<_> = all_keys.iter().map(|k| NodeId::new(k.pubkey())).collect();
    b.iter(|| {
        let time = Duration::ZERO;
        let mut encoder =
            Raptor::<ST>::new_from_message(time, &all_peers, &all_keys[0], message.clone());
        let mut decoder =
            Raptor::<ST>::try_new_from_meta(time, &all_peers, &all_keys[1], encoder.meta().clone())
                .unwrap();
        loop {
            let (_, chunk, data) = encoder
                .generate_chunk()
                .expect("chunk generation shouldn't fail");
            if decoder
                .process_chunk(encoder.creator(), chunk, data)
                .unwrap()
                .is_some()
            {
                break;
            }
        }
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
