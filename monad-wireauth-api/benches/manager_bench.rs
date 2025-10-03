use std::{net::SocketAddr, time::Duration};

use monad_wireauth_api::{Config, TestContext, API};
use divan::Bencher;
use monad_wireauth_protocol::{common::PublicKey, messages::DataPacketHeader};
use secp256k1::rand::rng;
use zerocopy::{FromBytes, IntoBytes};

fn main() {
    divan::main();
}

fn create_test_manager() -> (API<TestContext>, PublicKey, TestContext) {
    let mut rng = rng();
    let (public_key, private_key) =
        monad_wireauth_protocol::crypto::generate_keypair(&mut rng).unwrap();
    let psk = [0u8; 32];
    let config = Config {
        session_timeout: Duration::from_secs(10),
        session_timeout_jitter: Duration::from_secs(1),
        keepalive_interval: Duration::from_secs(5),
        keepalive_jitter: Duration::from_secs(1),
        rekey_interval: Duration::from_secs(120),
        rekey_jitter: Duration::from_secs(10),
        max_session_duration: Duration::from_secs(150),
        handshake_rate_limit: 50,
        handshake_rate_reset_interval: Duration::from_secs(1),
        cookie_refresh_duration: Duration::from_secs(120),
        low_watermark_sessions: 100,
        high_watermark_sessions: 500,
        max_sessions_per_ip: 10,
        ip_rate_limit_window: Duration::from_secs(60),
        max_requests_per_ip: 10,
        ip_history_capacity: 100_000,
        psk,
    };
    let context = TestContext::new();
    let context_clone = context.clone();

    let manager = API::new(config, private_key, public_key.clone(), context);
    (manager, public_key, context_clone)
}

fn establish_session(
    peer1_manager: &mut API<TestContext>,
    peer2_manager: &mut API<TestContext>,
    _peer1_public: &PublicKey,
    peer2_public: &PublicKey,
) {
    let peer1_addr: SocketAddr = "127.0.0.1:51820".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:51821".parse().unwrap();

    peer1_manager
        .connect(
            peer2_public.clone(),
            peer2_addr,
            monad_wireauth_session::DEFAULT_RETRY_ATTEMPTS,
        )
        .expect("peer1 failed to init session");

    let init_packet = peer1_manager.next_packet().unwrap().1;

    let mut init_packet_mut = init_packet.to_vec();
    peer2_manager
        .dispatch(&mut init_packet_mut, peer1_addr)
        .expect("peer2 failed to accept handshake");

    let response_packet = peer2_manager.next_packet().unwrap().1;

    let mut response_packet_mut = response_packet.to_vec();
    peer1_manager
        .dispatch(&mut response_packet_mut, peer2_addr)
        .expect("peer1 failed to complete handshake");

    while let Some((_addr, packet)) = peer1_manager.next_packet() {
        let mut packet_mut = packet.to_vec();
        peer2_manager.dispatch(&mut packet_mut, peer1_addr).ok();
    }
}

#[divan::bench]
fn bench_session_send_init(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let (manager, _local_public, _) = create_test_manager();
            let (_peer2_manager, peer2_public, _) = create_test_manager();
            let peer2_addr: SocketAddr = "127.0.0.1:51821".parse().unwrap();

            (manager, peer2_public, peer2_addr)
        })
        .bench_local_values(|(mut manager, peer2_public, peer2_addr)| {
            manager
                .connect(
                    peer2_public,
                    peer2_addr,
                    monad_wireauth_session::DEFAULT_RETRY_ATTEMPTS,
                )
                .expect("failed to init session");
        });
}

#[divan::bench]
fn bench_session_handle_init(bencher: Bencher) {
    bencher
        .with_inputs(|| {
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
        })
        .bench_local_values(|(mut peer2_manager, init_packet, peer1_addr)| {
            let mut init_packet_mut = init_packet.to_vec();
            peer2_manager
                .dispatch(&mut init_packet_mut, peer1_addr)
                .expect("failed to handle init");
        });
}

#[divan::bench]
fn bench_session_handle_response(bencher: Bencher) {
    bencher
        .with_inputs(|| {
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
            mgr2.dispatch(&mut init_packet_mut, peer1_addr)
                .expect("dispatch failed");
            let response_packet = mgr2.next_packet().unwrap().1;

            (mgr1, response_packet, peer2_addr)
        })
        .bench_local_values(|(mut mgr1, response_packet, peer2_addr)| {
            let mut response_packet_mut = response_packet.to_vec();
            mgr1.dispatch(&mut response_packet_mut, peer2_addr)
                .expect("handle response failed");
        });
}

#[divan::bench]
fn bench_session_encrypt(bencher: Bencher) {
    let (mut mgr1, peer1_public, _) = create_test_manager();
    let (mut mgr2, peer2_public, _) = create_test_manager();

    establish_session(&mut mgr1, &mut mgr2, &peer1_public, &peer2_public);

    let mut plaintext = vec![0u8; 1024];
    bencher.bench_local(|| {
        mgr1.encrypt_by_public_key(&peer2_public, &mut plaintext)
            .expect("encryption failed");
    });
}

#[divan::bench]
fn bench_session_decrypt(bencher: Bencher) {
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

    bencher.bench_local(|| {
        mgr2.reset_replay_filter_for_receiver(receiver_index);

        let mut packet_clone = packet_data.clone();
        mgr2.dispatch(&mut packet_clone, peer1_addr)
            .expect("decryption failed");
    });
}
