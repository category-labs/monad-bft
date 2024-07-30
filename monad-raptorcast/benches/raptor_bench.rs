use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use itertools::Itertools;
use monad_crypto::hasher::{Hasher, HasherType};
use monad_dataplane::network::MONAD_GSO_SIZE;
use monad_raptorcast::{build_messages, parse_message, BuildTarget, EpochValidators, Validator};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{NodeId, Stake};
use raptor_code::SourceBlockDecoder;

#[allow(clippy::useless_vec)]
pub fn criterion_benchmark(c: &mut Criterion) {
    let message_size = 10_000 * 400;
    let message: Bytes = vec![123_u8; message_size].into();

    let mut group = c.benchmark_group("encoder/decoder");
    group.throughput(Throughput::Bytes(message_size as u64));
    group.bench_function("Encoding", |b| {
        let keys = (0_u8..100_u8)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                KeyPair::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let mut validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Validator { stake: Stake(1) }))
                .collect(),
        };

        let known_addresses = keys
            .iter()
            .map(|key| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                )
            })
            .collect();

        b.iter(|| {
            let epoch_validators = validators.view_without(vec![&NodeId::new(keys[0].pubkey())]);
            let _ = build_messages::<SecpSignature>(
                &keys[0],
                message.clone(),
                2, // redundancy,
                0, // epoch_no
                0, // round_no
                BuildTarget::Raptorcast(epoch_validators),
                &known_addresses,
            );
        });
    });

    group.bench_function("Decoding", |b| {
        let keys = (0_u8..100_u8)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                KeyPair::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let mut validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Validator { stake: Stake(1) }))
                .collect(),
        };
        let epoch_validators = validators.view_without(vec![&NodeId::new(keys[0].pubkey())]);

        let known_addresses = keys
            .iter()
            .map(|key| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                )
            })
            .collect();

        let messages = build_messages::<SecpSignature>(
            &keys[0],
            message.clone(),
            2, // redundancy,
            0, // epoch_no
            0, // round_no
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        )
        .into_iter()
        .map(|(_to, message)| message)
        .collect_vec();

        let example_chunk = parse_message::<SecpSignature>(
            &mut Default::default(),
            messages[0].clone().split_to(MONAD_GSO_SIZE),
        )
        .expect("valid chunk");

        b.iter_batched(
            || messages.clone(),
            |messages| {
                let mut signature_cache = HashMap::new();
                let mut decoder = {
                    // data_size is always greater than zero, so this division is safe
                    let num_source_symbols = message_size.div_ceil(example_chunk.chunk.len());
                    SourceBlockDecoder::new(num_source_symbols)
                };
                let mut decode_success = false;
                for mut message in messages {
                    while !message.is_empty() {
                        let parsed_message = parse_message::<SecpSignature>(
                            &mut signature_cache,
                            message.split_to(MONAD_GSO_SIZE),
                        )
                        .expect("valid message");
                        decoder.push_encoding_symbol(
                            parsed_message.chunk,
                            parsed_message.chunk_id.into(),
                        );
                        if let Some(decoded) = decoder.decode(message_size) {
                            decode_success = true;
                            break;
                        }
                    }
                }
                assert!(decode_success);
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
