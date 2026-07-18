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
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
use monad_raptorcast::auth::{metrics::UDP_METRICS, AuthenticationProtocol, WireAuthProtocol};
use monad_secp::{KeyPair, PubKey};
use monad_wireauth::{Config, DEFAULT_RETRY_ATTEMPTS};
use zerocopy::IntoBytes;

const DATAGRAMS_PER_REBROADCAST: usize = 1;
const TARGET_COUNTS: [usize; 4] = [1, 4, 16, 100];

fn protocol(seed: u8) -> (WireAuthProtocol, PubKey) {
    let keypair = KeyPair::from_bytes(&mut [seed; 32]).unwrap();
    let public_key = keypair.pubkey();
    (
        WireAuthProtocol::new(&UDP_METRICS, Config::default(), Arc::new(keypair)),
        public_key,
    )
}

fn establish_session(
    sender: &mut WireAuthProtocol,
    sender_public_key: &PubKey,
    sender_addr: SocketAddr,
    receiver: &mut WireAuthProtocol,
    receiver_public_key: &PubKey,
    receiver_addr: SocketAddr,
) {
    sender
        .connect(receiver_public_key, receiver_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let (_, init) = sender.next_packet().unwrap();
    receiver.dispatch(&mut init.to_vec(), sender_addr).unwrap();

    let (_, response) = receiver.next_packet().unwrap();
    sender
        .dispatch(&mut response.to_vec(), receiver_addr)
        .unwrap();

    while let Some((_, packet)) = sender.next_packet() {
        receiver
            .dispatch(&mut packet.to_vec(), sender_addr)
            .unwrap();
    }

    assert!(sender.is_connected_socket_and_public_key(&receiver_addr, receiver_public_key));
    assert!(receiver.is_connected_socket_and_public_key(&sender_addr, sender_public_key));
}

// Measures CPU-side forwarding after RaptorCast has selected the payload and targets. Socket
// enqueue, pacing, packet parsing, validation, and decoding are deliberately excluded.
fn rebroadcast(
    protocol: &mut WireAuthProtocol,
    targets: &[(PubKey, SocketAddr)],
    payload: &Bytes,
    stride: u16,
) {
    let header_size = WireAuthProtocol::HEADER_SIZE as usize;

    for (public_key, addr) in targets {
        let connected = protocol.is_connected_socket_and_public_key(addr, public_key);
        let connecting = protocol.has_initiator_session_by_socket_and_public_key(addr, public_key);
        assert!(connected || connecting);

        let connected = protocol.is_connected_public_key(public_key);
        let connecting = protocol.has_initiator_session_by_public_key(public_key);
        assert!(connected || connecting);

        let mut plaintext = payload.clone();
        while !plaintext.is_empty() {
            let piece = plaintext.split_to(plaintext.len().min(stride.into()));
            assert!(protocol.is_connected_public_key(public_key));
            assert_eq!(protocol.get_socket_by_public_key(public_key), Some(*addr));

            let mut packet = BytesMut::with_capacity(header_size + piece.len());
            packet.resize(header_size, 0);
            packet.extend_from_slice(&piece);
            let header = protocol
                .encrypt_by_public_key(public_key, &mut packet[header_size..])
                .unwrap();
            packet[..header_size].copy_from_slice(header.as_bytes());
            black_box(packet.freeze());
        }
    }
}

fn bench_rebroadcast(c: &mut Criterion) {
    let sender_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 20_000));
    let (mut sender, sender_public_key) = protocol(1);
    let max_targets = *TARGET_COUNTS.last().unwrap();
    let mut targets = Vec::with_capacity(max_targets);

    for index in 0..max_targets {
        let receiver_addr = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            20_001 + index as u16,
        ));
        let (mut receiver, receiver_public_key) = protocol(index as u8 + 2);
        establish_session(
            &mut sender,
            &sender_public_key,
            sender_addr,
            &mut receiver,
            &receiver_public_key,
            receiver_addr,
        );
        targets.push((receiver_public_key, receiver_addr));
    }

    let stride = DEFAULT_SEGMENT_SIZE - WireAuthProtocol::HEADER_SIZE;
    let payload = Bytes::from(vec![0; usize::from(stride) * DATAGRAMS_PER_REBROADCAST]);
    let mut group = c.benchmark_group("authenticated_rebroadcast_cpu");

    for target_count in TARGET_COUNTS {
        group.throughput(Throughput::Bytes((payload.len() * target_count) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(target_count),
            &target_count,
            |b, &target_count| {
                b.iter(|| rebroadcast(&mut sender, &targets[..target_count], &payload, stride))
            },
        );
    }
}

criterion_group!(benches, bench_rebroadcast);
criterion_main!(benches);
