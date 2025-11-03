use std::net::SocketAddr;

use criterion::{criterion_group, criterion_main, Criterion};
use monad_wireauth_api::{Config, TestContext, API};
use monad_wireauth_protocol::messages::{DataPacketHeader, Packet};
use secp256k1::rand::rng;
use zerocopy::{FromBytes, IntoBytes};

fn create_test_manager() -> (API<TestContext>, monad_secp::PubKey, TestContext) {
    let mut rng = rng();
    let keypair = monad_secp::KeyPair::generate(&mut rng);
    let public_key = keypair.pubkey();
    let config = Config::default();
    let context = TestContext::new();
    let context_clone = context.clone();

    let manager = API::new(config, keypair, context);
    (manager, public_key, context_clone)
}

fn establish_session(
    peer1_manager: &mut API<TestContext>,
    peer2_manager: &mut API<TestContext>,
    _peer1_public: &monad_secp::PubKey,
    peer2_public: &monad_secp::PubKey,
) {
    let peer1_addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:51821".parse().unwrap();

    peer1_manager
        .connect(
            *peer2_public,
            peer2_addr,
            monad_wireauth_session::DEFAULT_RETRY_ATTEMPTS,
        )
        .expect("peer1 failed to init session");

    let init_packet = peer1_manager.next_packet().unwrap().1;

    let mut init_packet_mut = init_packet.to_vec();
    let parsed = Packet::try_from(&mut init_packet_mut[..]).unwrap();
    if let Packet::Control(control) = parsed {
        peer2_manager
            .dispatch_control(control, peer1_addr)
            .expect("peer2 failed to accept handshake");
    }

    let response_packet = peer2_manager.next_packet().unwrap().1;

    let mut response_packet_mut = response_packet.to_vec();
    let parsed = Packet::try_from(&mut response_packet_mut[..]).unwrap();
    if let Packet::Control(control) = parsed {
        peer1_manager
            .dispatch_control(control, peer2_addr)
            .expect("peer1 failed to complete handshake");
    }

    while let Some((_addr, packet)) = peer1_manager.next_packet() {
        let mut packet_mut = packet.to_vec();
        if let Ok(Packet::Control(control)) = Packet::try_from(&mut packet_mut[..]) {
            peer2_manager.dispatch_control(control, peer1_addr).ok();
        }
    }
}

// inline(never) for easier navigation in perf
#[inline(never)]
fn session_send_init(
    manager: &mut API<TestContext>,
    peer2_public: monad_secp::PubKey,
    peer2_addr: SocketAddr,
) {
    manager
        .connect(
            peer2_public,
            peer2_addr,
            monad_wireauth_session::DEFAULT_RETRY_ATTEMPTS,
        )
        .expect("failed to init session");
}

fn bench_session_send_init(c: &mut Criterion) {
    c.bench_function("session_send_init", |b| {
        b.iter_batched_ref(
            || {
                let (manager, _local_public, _) = create_test_manager();
                let (_peer2_manager, peer2_public, _) = create_test_manager();
                let peer2_addr: SocketAddr = "127.0.0.1:51821".parse().unwrap();
                (manager, peer2_public, peer2_addr)
            },
            |(manager, peer2_public, peer2_addr)| {
                session_send_init(manager, *peer2_public, *peer2_addr)
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

// inline(never) for easier navigation in perf
#[inline(never)]
fn session_handle_init(
    peer2_manager: &mut API<TestContext>,
    init_packet: &[u8],
    peer1_addr: SocketAddr,
) {
    let mut init_packet_mut = init_packet.to_vec();
    let parsed = Packet::try_from(&mut init_packet_mut[..]).unwrap();
    if let Packet::Control(control) = parsed {
        peer2_manager
            .dispatch_control(control, peer1_addr)
            .expect("failed to handle init");
    }
}

fn bench_session_handle_init(c: &mut Criterion) {
    c.bench_function("session_handle_init", |b| {
        b.iter_batched_ref(
            || {
                let (mut peer1_manager, _peer1_public, _) = create_test_manager();
                let (peer2_manager, peer2_public, _) = create_test_manager();
                let peer1_addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();
                let peer2_addr: SocketAddr = "127.0.0.1:51821".parse().unwrap();

                peer1_manager
                    .connect(
                        peer2_public,
                        peer2_addr,
                        monad_wireauth_session::DEFAULT_RETRY_ATTEMPTS,
                    )
                    .expect("failed to init session");
                let init_packet = peer1_manager.next_packet().unwrap().1;

                (peer2_manager, init_packet, peer1_addr)
            },
            |(peer2_manager, init_packet, peer1_addr)| {
                session_handle_init(peer2_manager, init_packet, *peer1_addr)
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

// inline(never) for easier navigation in perf
#[inline(never)]
fn session_handle_response(
    mgr1: &mut API<TestContext>,
    response_packet: &[u8],
    peer2_addr: SocketAddr,
) {
    let mut response_packet_mut = response_packet.to_vec();
    let parsed = Packet::try_from(&mut response_packet_mut[..]).unwrap();
    if let Packet::Control(control) = parsed {
        mgr1.dispatch_control(control, peer2_addr)
            .expect("handle response failed");
    }
}

fn bench_session_handle_response(c: &mut Criterion) {
    c.bench_function("session_handle_response", |b| {
        b.iter_batched_ref(
            || {
                let (mut mgr1, _peer1_public, _) = create_test_manager();
                let (mut mgr2, peer2_public, _) = create_test_manager();
                let peer1_addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();
                let peer2_addr: SocketAddr = "127.0.0.1:51821".parse().unwrap();

                mgr1.connect(
                    peer2_public,
                    peer2_addr,
                    monad_wireauth_session::DEFAULT_RETRY_ATTEMPTS,
                )
                .expect("init failed");
                let init_packet = mgr1.next_packet().unwrap().1;

                let mut init_packet_mut = init_packet.to_vec();
                let parsed = Packet::try_from(&mut init_packet_mut[..]).unwrap();
                if let Packet::Control(control) = parsed {
                    mgr2.dispatch_control(control, peer1_addr)
                        .expect("dispatch failed");
                }
                let response_packet = mgr2.next_packet().unwrap().1;

                (mgr1, response_packet, peer2_addr)
            },
            |(mgr1, response_packet, peer2_addr)| {
                session_handle_response(mgr1, response_packet, *peer2_addr)
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

// inline(never) for easier navigation in perf
#[inline(never)]
fn session_encrypt(
    mgr1: &mut API<TestContext>,
    peer2_public: &monad_secp::PubKey,
    plaintext: &mut [u8],
) -> DataPacketHeader {
    mgr1.encrypt_by_public_key(peer2_public, plaintext)
        .expect("encryption failed")
}

fn bench_session_encrypt(c: &mut Criterion) {
    let (mut mgr1, peer1_public, _) = create_test_manager();
    let (mut mgr2, peer2_public, _) = create_test_manager();

    establish_session(&mut mgr1, &mut mgr2, &peer1_public, &peer2_public);

    c.bench_function("session_encrypt", |b| {
        let mut plaintext = vec![0u8; 1024];
        b.iter(|| session_encrypt(&mut mgr1, &peer2_public, &mut plaintext))
    });
}

// inline(never) for easier navigation in perf
#[inline(never)]
fn session_decrypt(
    mgr2: &mut API<TestContext>,
    packet_data: &[u8],
    peer1_addr: SocketAddr,
    receiver_index: u32,
) {
    mgr2.reset_replay_filter_for_receiver(receiver_index);

    let mut packet_clone = packet_data.to_vec();
    let parsed = Packet::try_from(&mut packet_clone[..]).unwrap();
    if let Packet::Data(data) = parsed {
        mgr2.decrypt(data, peer1_addr).expect("decryption failed");
    }
}

fn bench_session_decrypt(c: &mut Criterion) {
    let (mut mgr1, peer1_public, _) = create_test_manager();
    let (mut mgr2, peer2_public, _) = create_test_manager();

    establish_session(&mut mgr1, &mut mgr2, &peer1_public, &peer2_public);

    let mut plaintext = vec![0u8; 1024];
    let header = mgr1
        .encrypt_by_public_key(&peer2_public, &mut plaintext)
        .expect("encryption failed");

    let mut packet_data = Vec::with_capacity(header.as_bytes().len() + plaintext.len());
    packet_data.extend_from_slice(header.as_bytes());
    packet_data.extend_from_slice(&plaintext);

    let header_ref = DataPacketHeader::ref_from_bytes(&packet_data[..DataPacketHeader::SIZE])
        .expect("failed to get header");
    let receiver_index = header_ref.receiver_index.get();

    let peer1_addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();

    c.bench_function("session_decrypt", |b| {
        b.iter(|| session_decrypt(&mut mgr2, &packet_data, peer1_addr, receiver_index))
    });
}

criterion_group!(
    benches,
    bench_session_send_init,
    bench_session_handle_init,
    bench_session_handle_response,
    bench_session_encrypt,
    bench_session_decrypt
);
criterion_main!(benches);
