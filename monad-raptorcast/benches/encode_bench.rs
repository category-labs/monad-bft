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

use std::{collections::HashMap, net::SocketAddr, str::FromStr};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use itertools::Itertools as _;
use monad_crypto::certificate_signature::{CertificateSignature, CertificateSignaturePubKey};
use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
use monad_raptorcast::{
    packet,
    util::{BuildTarget, EpochValidators, Redundancy},
};
use monad_secp::SecpSignature;
use monad_testutil::signing::get_key;
use monad_types::{NodeId, Stake};

const NUM_NODES: usize = 100;

pub fn bench(c: &mut Criterion) {
    bench_build_messages(c, "Raptorcast 128K", 128 * 1024, "raptorcast");
    bench_build_messages(c, "Raptorcast 2M", 2 * 1024 * 1024, "raptorcast");

    bench_build_messages(c, "Broadcast 128K", 128 * 1024, "broadcast");
    // 2M broadcast yields more than 65535 packets, so we only
    // benchmark for 128K.
}

pub fn bench_build_messages(c: &mut Criterion, name: &str, message_size: usize, target: &str) {
    let message: Bytes = vec![123_u8; message_size].into();

    let mut group = c.benchmark_group(name);

    let (author, build_target, known_addrs) = match target {
        "raptorcast" => {
            group.throughput(Throughput::Bytes(message_size as u64));
            setup_raptorcast()
        }
        "broadcast" => {
            group.throughput(Throughput::Bytes((message_size * NUM_NODES) as u64));
            setup_broadcast()
        }
        _ => panic!("unsupported target"),
    };

    group.bench_function("packet::build_messages", |b| {
        b.iter(|| {
            let _ = packet::build_messages(
                &author,
                DEFAULT_SEGMENT_SIZE, // segment_size
                message.clone(),
                Redundancy::from_u8(2),
                0, // epoch_no
                0, // unix_ts_ms
                build_target.clone(),
                &known_addrs,
            );
        });
    });

    group.finish();
}

type ST = SecpSignature;
type PT = CertificateSignaturePubKey<ST>;
type KeyPair = <ST as CertificateSignature>::KeyPairType;

fn setup_raptorcast() -> (
    KeyPair,
    BuildTarget<'static, ST>,
    HashMap<NodeId<PT>, SocketAddr>,
) {
    let mut keys = (0..(NUM_NODES as u64)).map(get_key::<ST>).collect_vec();

    // leak the value to get a 'static reference
    let validators = Box::leak(Box::new(EpochValidators {
        validators: keys
            .iter()
            .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
            .collect(),
    }));

    let addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();
    let known_addresses = keys
        .iter()
        .map(|key| (NodeId::new(key.pubkey()), addr))
        .collect();

    let author = keys.pop().unwrap();
    let epoch_validators = validators.view_without(vec![&NodeId::new(author.pubkey())]);

    (
        author,
        BuildTarget::Raptorcast(epoch_validators),
        known_addresses,
    )
}

fn setup_broadcast() -> (
    KeyPair,
    BuildTarget<'static, ST>,
    HashMap<NodeId<PT>, SocketAddr>,
) {
    let mut keys = (0..100).map(get_key::<ST>).collect_vec();

    // leak the value to get a 'static reference
    let validators = Box::leak(Box::new(EpochValidators {
        validators: keys
            .iter()
            .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
            .collect(),
    }));

    let addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();
    let known_addresses = keys
        .iter()
        .map(|key| (NodeId::new(key.pubkey()), addr))
        .collect();

    let author = keys.pop().unwrap();
    let epoch_validators = validators.view_without(vec![&NodeId::new(author.pubkey())]);

    (
        author,
        BuildTarget::Broadcast(epoch_validators.into()),
        known_addresses,
    )
}

criterion_group!(benches, bench);
criterion_main!(benches);
