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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use itertools::Itertools;
use lru::LruCache;
use monad_crypto::hasher::{Hasher, HasherType};
use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
use monad_raptor::ManagedDecoder;
use monad_raptorcast::{
    packet::build_messages,
    udp::{parse_message, GroupId, MAX_REDUNDANCY, SIGNATURE_CACHE_SIZE},
    util::{BuildTarget, EpochValidators, Redundancy},
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{Epoch, NodeId, Stake};

#[allow(clippy::useless_vec)]
pub fn criterion_benchmark(c: &mut Criterion) {
    let message_size = 2 * 1024 * 1024; // 2 MB
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

        let validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
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
                DEFAULT_SEGMENT_SIZE, // segment_size
                message.clone(),
                Redundancy::from_u8(2),
                GroupId::Primary(Epoch(0)), // epoch_no
                0,                          // unix_ts_ms
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

        let validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
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
            DEFAULT_SEGMENT_SIZE, // segment_size
            message.clone(),
            Redundancy::from_u8(2),
            GroupId::Primary(Epoch(0)), // epoch_no
            0,                          // unix_ts_ms
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        )
        .into_iter()
        .map(|(_to, message)| message)
        .collect_vec();

        let example_chunk = parse_message::<SecpSignature, _>(
            &mut LruCache::new(SIGNATURE_CACHE_SIZE),
            messages[0].clone().split_to(DEFAULT_SEGMENT_SIZE.into()),
            u64::MAX,
            |_| Ok(()),
        )
        .expect("valid chunk");

        b.iter_batched(
            || messages.clone(),
            |messages| {
                let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);
                let mut decoder = {
                    let symbol_len = example_chunk.chunk.len();

                    // data_size is always greater than zero, so this division is safe
                    let num_source_symbols = message_size.div_ceil(symbol_len);
                    let encoded_symbol_capacity = MAX_REDUNDANCY.scale(num_source_symbols).unwrap();

                    ManagedDecoder::new(num_source_symbols, encoded_symbol_capacity, symbol_len)
                        .unwrap()
                };
                let mut decode_success = false;
                for mut message in messages {
                    while !message.is_empty() {
                        let parsed_message = parse_message::<SecpSignature, _>(
                            &mut signature_cache,
                            message.split_to(DEFAULT_SEGMENT_SIZE.into()),
                            u64::MAX,
                            |_| Ok(()),
                        )
                        .expect("valid message");
                        decoder.received_encoded_symbol(
                            &parsed_message.chunk,
                            parsed_message.chunk_id.into(),
                        );
                        if decoder.try_decode() {
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
