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

use std::{convert::TryFrom, net::SocketAddr, time::Duration};

use monad_wireauth::{
    messages::{CookieReply, DataPacketHeader, HandshakeInitiation, HandshakeResponse, Packet},
    Config, Context, TestContext, API, DEFAULT_METRICS, DEFAULT_RETRY_ATTEMPTS,
};
use secp256k1::rand::rng;
use tracing_subscriber::EnvFilter;
use zerocopy::IntoBytes;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

fn create_manager() -> (API<TestContext>, monad_secp::PubKey, TestContext, Config) {
    let mut rng = rng();
    let keypair = monad_secp::KeyPair::generate(&mut rng);
    let public_key = keypair.pubkey();
    let config = Config::default();
    let context = TestContext::new();
    let context_clone = context.clone();
    let manager = API::new(DEFAULT_METRICS, config.clone(), keypair, context);
    (manager, public_key, context_clone, config)
}

fn collect<T>(manager: &mut API<TestContext>) -> Vec<u8>
where
    for<'a> &'a T: std::convert::TryFrom<&'a [u8]>,
    for<'a> <&'a T as std::convert::TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
{
    let (_, packet) = manager.next_packet().unwrap();
    let bytes = packet.to_vec();
    let _ = <&T>::try_from(&bytes[..]).unwrap();
    bytes
}

fn dispatch(manager: &mut API<TestContext>, packet: &[u8], from: SocketAddr) -> Option<Vec<u8>> {
    let mut packet_mut = packet.to_vec();
    let parsed_packet = Packet::try_from(&mut packet_mut[..]).ok()?;

    match parsed_packet {
        Packet::Control(control) => {
            manager.dispatch_control(control, from).ok()?;
            None
        }
        Packet::Data(data_packet) => {
            let (plaintext, _public_key) = manager.decrypt(data_packet, from).ok()?;
            Some(plaintext.as_slice().to_vec())
        }
    }
}

fn encrypt(
    manager: &mut API<TestContext>,
    peer_pubkey: &monad_secp::PubKey,
    plaintext: &mut [u8],
) -> Vec<u8> {
    let header = manager
        .encrypt_by_public_key(peer_pubkey, plaintext)
        .unwrap();
    let mut packet = Vec::with_capacity(DataPacketHeader::SIZE + plaintext.len());
    packet.extend_from_slice(header.as_bytes());
    packet.extend_from_slice(plaintext);
    packet
}

fn decrypt(manager: &mut API<TestContext>, packet: &[u8], from: SocketAddr) -> Vec<u8> {
    dispatch(manager, packet, from).unwrap()
}

#[test]
fn test_concurrent_init() {
    init_tracing();
    let (mut peer1, peer1_pubkey, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // 1. peer1 initiates to peer2
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    // 2. peer2 initiates to peer1
    peer2
        .connect(peer1_pubkey, peer1_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init1 = collect::<HandshakeInitiation>(&mut peer1);
    let init2 = collect::<HandshakeInitiation>(&mut peer2);

    // 3. peer2 receives peer1 init and sends response
    dispatch(&mut peer2, &init1, peer1_addr);
    // 4. peer1 receives peer2 init and sends response
    dispatch(&mut peer1, &init2, peer2_addr);

    let resp2 = collect::<HandshakeResponse>(&mut peer2);
    let resp1 = collect::<HandshakeResponse>(&mut peer1);

    // 5. peer1 receives peer2 response
    dispatch(&mut peer1, &resp2, peer2_addr);
    // 6. peer2 receives peer1 response
    dispatch(&mut peer2, &resp1, peer1_addr);

    // 7. peer1 encrypts message to peer2
    let mut plaintext1 = b"hello from peer1".to_vec();
    let packet1 = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext1);
    // 8. peer2 decrypts message from peer1
    let decrypted1 = decrypt(&mut peer2, &packet1, peer1_addr);
    assert_eq!(decrypted1, b"hello from peer1");

    // 9. peer2 encrypts message to peer1
    let mut plaintext2 = b"hello from peer2".to_vec();
    let packet2 = encrypt(&mut peer2, &peer1_pubkey, &mut plaintext2);
    // 10. peer1 decrypts message from peer2
    let decrypted2 = decrypt(&mut peer1, &packet2, peer2_addr);
    assert_eq!(decrypted2, b"hello from peer2");
}

#[test]
fn test_retries() {
    init_tracing();
    let (mut peer1, _, peer1_ctx, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // 1. peer1 connects to peer2 with 2 retries
    peer1.connect(peer2_pubkey, peer2_addr, 2).unwrap();

    // 2. peer1 sends first init - dropped
    let _init1 = collect::<HandshakeInitiation>(&mut peer1);

    // 3. advance time and tick - peer1 retries
    peer1_ctx.advance_time(Duration::from_secs(11));
    peer1.tick();
    // 4. peer1 sends second init - dropped
    let _init2 = collect::<HandshakeInitiation>(&mut peer1);

    // 5. advance time and tick - peer1 retries
    peer1_ctx.advance_time(Duration::from_secs(11));
    peer1.tick();
    // 6. peer1 sends third init - delivered to peer2
    let init3 = collect::<HandshakeInitiation>(&mut peer1);

    dispatch(&mut peer2, &init3, peer1_addr);
    // 7. peer2 sends response
    let resp = collect::<HandshakeResponse>(&mut peer2);
    // 8. peer1 receives response and completes handshake
    dispatch(&mut peer1, &resp, peer2_addr);

    // 9. exchange several messages
    let mut plaintext1 = b"message1".to_vec();
    let packet1 = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext1);
    let decrypted1 = decrypt(&mut peer2, &packet1, peer1_addr);
    assert_eq!(decrypted1, b"message1");

    let mut plaintext2 = b"message2".to_vec();
    let packet2 = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext2);
    let decrypted2 = decrypt(&mut peer2, &packet2, peer1_addr);
    assert_eq!(decrypted2, b"message2");

    let mut plaintext3 = b"message3".to_vec();
    let packet3 = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext3);
    let decrypted3 = decrypt(&mut peer2, &packet3, peer1_addr);
    assert_eq!(decrypted3, b"message3");
}

#[test]
fn test_encrypt_by_pubkey_and_socket() {
    init_tracing();
    let (mut peer1, _, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // 1. peer1 initiates to peer2
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    // 2. complete handshake
    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);
    let resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &resp, peer2_addr);

    // 3. peer1 encrypts by public key and sends to peer2
    let mut plaintext1 = b"by pubkey".to_vec();
    let packet1 = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext1);
    // 5a. peer2 decrypts message sent by public key
    let decrypted1 = decrypt(&mut peer2, &packet1, peer1_addr);
    assert_eq!(decrypted1, b"by pubkey");

    // 4. peer1 encrypts by socket and sends to peer2
    let mut plaintext2 = b"by socket".to_vec();
    let header2 = peer1
        .encrypt_by_socket(&peer2_addr, &mut plaintext2)
        .unwrap();
    let mut packet2 = Vec::with_capacity(DataPacketHeader::SIZE + plaintext2.len());
    packet2.extend_from_slice(header2.as_bytes());
    packet2.extend_from_slice(&plaintext2);
    // 5b. peer2 decrypts message sent by socket
    let decrypted2 = decrypt(&mut peer2, &packet2, peer1_addr);
    assert_eq!(decrypted2, b"by socket");
}

#[test]
fn test_cookie_reply_on_init() {
    init_tracing();
    // 1. create managers with zero unverified budget to trigger an immediate cookie reply
    let config = Config {
        handshake_cookie_unverified_rate_limit: 0,
        handshake_cookie_verified_rate_limit: 10,
        session_timeout_jitter: Duration::ZERO, // to avoid randomness in tests
        ..Config::default()
    };
    let session_timeout = config.session_timeout;

    let mut rng = rng();
    let keypair1 = monad_secp::KeyPair::generate(&mut rng);
    let context1 = TestContext::new();
    let mut peer1 = API::new(DEFAULT_METRICS, config.clone(), keypair1, context1.clone());

    let keypair2 = monad_secp::KeyPair::generate(&mut rng);
    let public_key2 = keypair2.pubkey();
    let context2 = TestContext::new();
    let mut peer2 = API::new(DEFAULT_METRICS, config, keypair2, context2);

    let peer1_addr: SocketAddr = "192.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "192.0.0.2:8002".parse().unwrap();

    // 2. peer1 initiates again and peer2 immediately sends a cookie reply
    peer1
        .connect(public_key2, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init2 = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init2, peer1_addr);

    // 3. peer2 sends cookie reply, peer1 receives and stores it
    let cookie = collect::<CookieReply>(&mut peer2);
    dispatch(&mut peer1, &cookie, peer2_addr);

    // 4. advance time past session timeout, tick triggers retry with stored cookie
    context1.advance_time(session_timeout);
    peer1.tick();

    // 5. peer1 sends init with valid mac2 (using stored cookie)
    let init3 = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init3, peer1_addr);
    let _resp2 = collect::<HandshakeResponse>(&mut peer2);
}

#[test]
fn test_connect_after_established() {
    init_tracing();
    let (mut peer1, _, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // 1. peer1 establishes session with peer2
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);
    let resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &resp, peer2_addr);

    let mut plaintext = b"before reconnect".to_vec();
    let packet = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext);
    let decrypted = decrypt(&mut peer2, &packet, peer1_addr);
    assert_eq!(decrypted, b"before reconnect");

    // 2. peer1 attempts connect again to peer2
    let _ = peer1.connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS);

    // 3. exchange messages to verify session still works
    let mut plaintext = b"after reconnect".to_vec();
    let packet = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext);
    let decrypted = decrypt(&mut peer2, &packet, peer1_addr);
    assert_eq!(decrypted, b"after reconnect");
}

#[test]
fn test_connect_rate_limit() {
    init_tracing();
    let mut rng = rng();
    let keypair1 = monad_secp::KeyPair::generate(&mut rng);
    let keypair2 = monad_secp::KeyPair::generate(&mut rng);
    let peer2_pubkey = keypair2.pubkey();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    let config = Config {
        connect_rate_limit: 2,
        connect_rate_reset_interval: Duration::from_secs(60),
        ..Config::default()
    };

    let context = TestContext::new();
    let context_clone = context.clone();
    let mut peer1 = API::new(DEFAULT_METRICS, config, keypair1, context);

    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let err = peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap_err();
    assert!(matches!(
        err,
        monad_wireauth::Error::ConnectRateLimited { .. }
    ));

    context_clone.advance_time(Duration::from_secs(60));
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
}

#[test]
fn test_timestamp_replay() {
    init_tracing();
    let (mut peer1, _, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // 1. peer1 initiates to peer2
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init = collect::<HandshakeInitiation>(&mut peer1);

    // 2. peer2 accepts init and sends response
    dispatch(&mut peer2, &init, peer1_addr);
    let _resp = collect::<HandshakeResponse>(&mut peer2);

    // 3. peer1 sends same init again and verify peer2 rejects replay
    let result2 = dispatch(&mut peer2, &init, peer1_addr);
    assert!(result2.is_none());
}

#[test]
fn test_too_many_accepted_sessions() {
    init_tracing();
    // 1. create responder with max 5 pending sessions
    let config = Config {
        max_pending_accepted_sessions: 5,
        ..Default::default()
    };

    let mut rng = rng();
    let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
    let responder_public = responder_keypair.pubkey();
    let responder_ctx = TestContext::new();
    let mut responder = API::new(
        DEFAULT_METRICS,
        config.clone(),
        responder_keypair,
        responder_ctx,
    );
    let responder_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 2. 10 initiators each send init to responder
    for i in 0..10 {
        let initiator_ctx = TestContext::new();
        let initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let mut initiator = API::new(
            DEFAULT_METRICS,
            config.clone(),
            initiator_keypair,
            initiator_ctx,
        );
        let initiator_addr: SocketAddr = format!("127.0.0.1:800{}", i).parse().unwrap();

        initiator
            .connect(responder_public, responder_addr, DEFAULT_RETRY_ATTEMPTS)
            .unwrap();

        let init = collect::<HandshakeInitiation>(&mut initiator);
        dispatch(&mut responder, &init, initiator_addr);
    }

    // 3. verify responder only accepted 5 sessions (max_pending_accepted_sessions limit)
    let mut pkts = vec![];
    while let Some(pkt) = responder.next_packet() {
        pkts.push(pkt);
    }
    assert_eq!(pkts.len(), 5);
}

#[test]
fn test_random_packet_error() {
    init_tracing();

    // 1. dispatch random invalid packet
    let random_packet = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let mut packet = random_packet;
    let result = Packet::try_from(&mut packet[..]);
    // 2. verify error is returned
    assert!(result.is_err());
}

#[test]
fn test_filter_drop_rate_limit() {
    init_tracing();
    // 1. create manager with low handshake rate limit (3 per interval)
    let config = Config {
        handshake_cookie_unverified_rate_limit: 3,
        handshake_cookie_verified_rate_limit: 3,
        ..Config::default()
    };

    let mut rng = rng();
    let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
    let responder_public = responder_keypair.pubkey();
    let responder_ctx = TestContext::new();
    let mut responder = API::new(
        DEFAULT_METRICS,
        config.clone(),
        responder_keypair,
        responder_ctx,
    );
    let responder_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 2. exceed rate limit with 4 inits (limit is 3)
    for i in 0..4 {
        let initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let initiator_ctx = TestContext::new();
        let mut initiator = API::new(
            DEFAULT_METRICS,
            config.clone(),
            initiator_keypair,
            initiator_ctx,
        );
        let initiator_addr: SocketAddr = format!("127.0.0.1:800{}", i).parse().unwrap();

        initiator
            .connect(responder_public, responder_addr, DEFAULT_RETRY_ATTEMPTS)
            .unwrap();

        let init = collect::<HandshakeInitiation>(&mut initiator);
        dispatch(&mut responder, &init, initiator_addr);
    }

    // 3. verify 3 handshake responses + 1 cookie reply (4th init is challenged, not silently dropped)
    let mut pkts = vec![];
    while let Some(pkt) = responder.next_packet() {
        pkts.push(pkt);
    }
    assert_eq!(pkts.len(), 4);
}

#[test]
fn test_next_deadline() {
    init_tracing();
    let (mut peer1, _, peer1_ctx, config) = create_manager();
    let (_, peer2_pubkey, _, _) = create_manager();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // 1. create manager and verify initial filter reset deadline
    let initial_deadline = peer1.next_deadline();
    let expected_filter_deadline =
        peer1_ctx.convert_duration_since_start_to_deadline(config.handshake_rate_reset_interval);
    assert_eq!(initial_deadline, Some(expected_filter_deadline));

    // 2. initiate connection and verify session deadline is set
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let session_deadline = peer1.next_deadline();
    assert!(session_deadline.is_some());
    let deadline_instant = session_deadline.unwrap();
    let max_expected_deadline =
        peer1_ctx.convert_duration_since_start_to_deadline(Duration::from_secs(10));
    assert!(deadline_instant <= max_expected_deadline);

    // 3. advance time partially and verify deadline remains unchanged
    peer1_ctx.advance_time(Duration::from_secs(5));

    let deadline_after_time_advance = peer1.next_deadline();
    assert!(deadline_after_time_advance.is_some());
    assert_eq!(deadline_after_time_advance.unwrap(), deadline_instant);

    // 4. advance time past deadline and verify deadline is now in the past
    peer1_ctx.advance_time(Duration::from_secs(20));

    let deadline_in_past = peer1.next_deadline();
    assert!(deadline_in_past.is_some());
    let current_instant =
        peer1_ctx.convert_duration_since_start_to_deadline(peer1_ctx.duration_since_start());
    assert!(deadline_in_past.unwrap() <= current_instant);
}

#[test]
fn test_next_deadline_includes_filter_reset() {
    init_tracing();
    let mut rng = rng();
    // 1. create manager with custom filter reset interval (5 seconds)
    let filter_reset_interval = Duration::from_secs(5);
    let config = Config {
        handshake_rate_reset_interval: filter_reset_interval,
        ..Config::default()
    };

    let peer_keypair = monad_secp::KeyPair::generate(&mut rng);
    let peer_ctx = TestContext::new();
    let peer = API::new(DEFAULT_METRICS, config, peer_keypair, peer_ctx.clone());

    // 2. verify next_deadline returns filter reset deadline
    let deadline = peer.next_deadline();
    assert!(deadline.is_some());
    let expected_deadline =
        peer_ctx.convert_duration_since_start_to_deadline(filter_reset_interval);
    assert_eq!(deadline.unwrap(), expected_deadline);
}

#[test]
fn test_next_deadline_returns_minimum_of_session_and_filter() {
    init_tracing();
    let mut rng = rng();
    let config = Config::default();
    let keapalive_interval = config.keepalive_interval;

    let peer1_keypair = monad_secp::KeyPair::generate(&mut rng);
    let peer1_ctx = TestContext::new();
    let mut peer1 = API::new(
        DEFAULT_METRICS,
        config.clone(),
        peer1_keypair,
        peer1_ctx.clone(),
    );

    let peer2_keypair = monad_secp::KeyPair::generate(&mut rng);
    let peer2_public = peer2_keypair.pubkey();
    let peer2_ctx = TestContext::new();
    let mut peer2 = API::new(DEFAULT_METRICS, config, peer2_keypair, peer2_ctx);

    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();

    // 1. establish session between peer1 and peer2
    peer1
        .connect(peer2_public, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);

    let response = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &response, peer2_addr);

    collect::<DataPacketHeader>(&mut peer1);

    // 2. verify next_deadline returns keepalive deadline
    let keepalive_deadline = peer1.next_deadline();
    assert!(keepalive_deadline.is_some());
    let deadline_instant = keepalive_deadline.unwrap();
    // 3. verify deadline is in the future but within keepalive interval
    let max_keepalive_deadline =
        peer1_ctx.convert_duration_since_start_to_deadline(keapalive_interval);
    let current_instant =
        peer1_ctx.convert_duration_since_start_to_deadline(peer1_ctx.duration_since_start());
    assert!(deadline_instant <= max_keepalive_deadline);
    assert!(deadline_instant > current_instant);
}

#[test]
fn test_disconnect() {
    init_tracing();
    let (mut peer1, _peer1_pubkey, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);

    let response = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &response, peer2_addr);

    collect::<DataPacketHeader>(&mut peer1);

    let mut plaintext = b"hello".to_vec();
    let encrypted = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext);
    let decrypted = decrypt(&mut peer2, &encrypted, peer1_addr);
    assert_eq!(&decrypted, b"hello");

    peer1.disconnect(&peer2_pubkey);

    let mut plaintext2 = b"world".to_vec();
    let result = peer1.encrypt_by_public_key(&peer2_pubkey, &mut plaintext2);
    assert!(result.is_err());
}

#[test]
fn test_is_connected_no_connection() {
    init_tracing();
    let (peer1, _, _, _) = create_manager();
    let (_, peer2_pubkey, _, _) = create_manager();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    assert!(!peer1.is_connected_socket(&peer2_addr));
    assert!(!peer1.is_connected_public_key(&peer2_pubkey));
}

#[test]
fn test_is_connected_after_handshake() {
    init_tracing();
    let (mut peer1, _, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);

    let response = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &response, peer2_addr);

    collect::<DataPacketHeader>(&mut peer1);

    assert!(peer1.is_connected_socket(&peer2_addr));
    assert!(peer1.is_connected_public_key(&peer2_pubkey));
}

#[test]
fn test_reordered_data_packet_after_reinit() {
    init_tracing();
    let (mut peer1, _peer1_pubkey, peer1_ctx, _) = create_manager();
    let (mut peer2, peer2_pubkey, peer2_ctx, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // 1. establish session between peer1 and peer2
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);
    let resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &resp, peer2_addr);
    let confirm = collect::<DataPacketHeader>(&mut peer1);

    // deliver the confirm to establish peer2's responder session
    dispatch(&mut peer2, &confirm, peer1_addr);

    // 2. peer1 encrypts a data packet (simulating a packet about to be sent)
    let mut plaintext_a = b"packet A".to_vec();
    let packet_a = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext_a);

    // 3. peer1 initiates a new session (e.g., due to rekey or reconnect)
    peer1_ctx.advance_time(Duration::from_secs(1));
    peer2_ctx.advance_time(Duration::from_secs(1));
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let new_init = collect::<HandshakeInitiation>(&mut peer1);

    // 4. network reorders: peer2 receives new_init BEFORE packet_a
    dispatch(&mut peer2, &new_init, peer1_addr);
    let new_resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &new_resp, peer2_addr);
    let new_confirm = collect::<DataPacketHeader>(&mut peer1);

    // deliver the confirm to establish peer2's new responder session
    // this will replace peer2's old responder transport
    dispatch(&mut peer2, &new_confirm, peer1_addr);

    // 5. now the old data packet_a arrives at peer2
    // the old session is kept as previous, so decryption still works
    let decrypted_a = decrypt(&mut peer2, &packet_a, peer1_addr);
    assert_eq!(decrypted_a, b"packet A");
}

#[test]
fn test_keepalive_reset_on_encrypt() {
    init_tracing();
    let config = Config {
        keepalive_interval: Duration::from_secs(3),
        keepalive_jitter: Duration::from_millis(0),
        session_timeout: Duration::from_secs(1000),
        session_timeout_jitter: Duration::from_secs(0),
        ..Config::default()
    };

    let mut rng = rng();
    let keypair1 = monad_secp::KeyPair::generate(&mut rng);
    let context1 = TestContext::new();
    let mut peer1 = API::new(DEFAULT_METRICS, config.clone(), keypair1, context1.clone());

    let keypair2 = monad_secp::KeyPair::generate(&mut rng);
    let peer2_pubkey = keypair2.pubkey();
    let context2 = TestContext::new();
    let mut peer2 = API::new(DEFAULT_METRICS, config, keypair2, context2);

    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);

    let response = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &response, peer2_addr);

    collect::<DataPacketHeader>(&mut peer1);

    for i in 0..10 {
        context1.advance_time(Duration::from_millis(500));
        peer1.tick();

        let mut plaintext = format!("data{}", i).into_bytes();
        let packet = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext);
        decrypt(&mut peer2, &packet, peer1_addr);

        assert!(
            peer1.next_packet().is_none(),
            "unexpected packet at iteration {}",
            i
        );
    }

    context1.advance_time(Duration::from_secs(4));
    peer1.tick();

    let keepalive_packet = peer1.next_packet();
    assert!(
        keepalive_packet.is_some(),
        "expected keepalive after idle period"
    );

    let mut plaintext = b"more data".to_vec();
    let packet = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext);
    let decrypted = decrypt(&mut peer2, &packet, peer1_addr);
    assert_eq!(decrypted, b"more data");

    context1.advance_time(Duration::from_secs(2));
    peer1.tick();
    let unexpected_packet = peer1.next_packet();
    assert!(
        unexpected_packet.is_none(),
        "unexpected packet after sending data: {:?}",
        unexpected_packet
    );
}

#[test]
fn test_message_buffering_during_handshake() {
    init_tracing();
    let (mut peer1, _, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    peer1
        .buffer_message(&peer2_pubkey, bytes::Bytes::from_static(b"buffered1"))
        .unwrap();
    peer1
        .buffer_message(&peer2_pubkey, bytes::Bytes::from_static(b"buffered2"))
        .unwrap();
    peer1
        .buffer_message(&peer2_pubkey, bytes::Bytes::from_static(b"buffered3"))
        .unwrap();

    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);

    let response = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &response, peer2_addr);

    let packet1 = peer1.next_packet().unwrap();
    let decrypted1 = decrypt(&mut peer2, &packet1.1, peer1_addr);
    assert_eq!(decrypted1, b"buffered1");

    let packet2 = peer1.next_packet().unwrap();
    let decrypted2 = decrypt(&mut peer2, &packet2.1, peer1_addr);
    assert_eq!(decrypted2, b"buffered2");

    let packet3 = peer1.next_packet().unwrap();
    let decrypted3 = decrypt(&mut peer2, &packet3.1, peer1_addr);
    assert_eq!(decrypted3, b"buffered3");

    assert!(peer1.next_packet().is_none());
}

#[test]
fn test_handshake_response_address_mismatch_rejected() {
    init_tracing();
    let (mut peer1, _, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();
    let spoofed_addr: SocketAddr = "127.0.0.1:9009".parse().unwrap();

    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);

    let response = collect::<HandshakeResponse>(&mut peer2);
    let mut response_packet = response.clone();
    let parsed_packet = Packet::try_from(&mut response_packet[..]).unwrap();
    let err = match parsed_packet {
        Packet::Control(control) => peer1.dispatch_control(control, spoofed_addr).unwrap_err(),
        Packet::Data(_) => panic!("expected control packet"),
    };
    assert!(matches!(
        err,
        monad_wireauth::Error::HandshakeResponseAddressMismatch { expected, actual }
        if expected == peer2_addr && actual == spoofed_addr
    ));

    // Same response succeeds from the expected source address.
    let mut response_packet = response;
    let parsed_packet = Packet::try_from(&mut response_packet[..]).unwrap();
    match parsed_packet {
        Packet::Control(control) => peer1.dispatch_control(control, peer2_addr).unwrap(),
        Packet::Data(_) => panic!("expected control packet"),
    }

    let mut plaintext = b"hello after address check".to_vec();
    let packet = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext);
    let decrypted = decrypt(&mut peer2, &packet, peer1_addr);
    assert_eq!(decrypted, b"hello after address check");
}

#[rstest::rstest]
#[case(1)]
#[case(1_000)]
fn test_stale_handshake_response_does_not_poison_pending_initiator(
    #[case] max_pending_initiated_sessions: usize,
) {
    init_tracing();
    let config = Config {
        max_pending_initiated_sessions,
        ..Config::default()
    };
    let stale_peer1_keypair = monad_secp::KeyPair::from_ikm(b"initiator key").unwrap();
    let peer1_keypair = monad_secp::KeyPair::from_ikm(b"initiator key").unwrap();
    let peer1_context = TestContext::new();
    peer1_context.advance_time(Duration::from_secs(1));

    let mut stale_peer1 = API::new(
        DEFAULT_METRICS,
        config.clone(),
        stale_peer1_keypair,
        TestContext::new(),
    );
    let mut peer1 = API::new(DEFAULT_METRICS, config, peer1_keypair, peer1_context);
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // stale_peer1 creates a valid but obsolete initiation from the same static key.
    stale_peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let stale_init = collect::<HandshakeInitiation>(&mut stale_peer1);
    dispatch(&mut peer2, &stale_init, peer1_addr);
    let stale_response = collect::<HandshakeResponse>(&mut peer2);

    // peer1 creates the real pending initiation with the same receiver index and a newer timestamp.
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);
    let response = collect::<HandshakeResponse>(&mut peer2);

    // the stale response passes source address, receiver index, and mac1 checks before auth fails.
    let mut stale_response_packet = stale_response;
    let parsed_packet = Packet::try_from(&mut stale_response_packet[..]).unwrap();
    let err = match parsed_packet {
        Packet::Control(control) => peer1.dispatch_control(control, peer2_addr).unwrap_err(),
        Packet::Data(_) => panic!("expected control packet"),
    };
    assert!(matches!(err, monad_wireauth::Error::Session(_)));

    // the authentic response still succeeds because the stale response did not mutate state.
    let mut response_packet = response;
    let parsed_packet = Packet::try_from(&mut response_packet[..]).unwrap();
    match parsed_packet {
        Packet::Control(control) => peer1.dispatch_control(control, peer2_addr).unwrap(),
        Packet::Data(_) => panic!("expected control packet"),
    }

    // data can flow on the session established by the authentic response.
    let mut plaintext = b"handshake survives stale response".to_vec();
    let packet = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext);
    let decrypted = decrypt(&mut peer2, &packet, peer1_addr);
    assert_eq!(decrypted, b"handshake survives stale response");
}

#[test]
fn test_max_pending_initiated_sessions_limit() {
    init_tracing();
    let config = Config {
        max_pending_initiated_sessions: 3,
        ..Config::default()
    };

    let mut rng = rng();
    let keypair = monad_secp::KeyPair::generate(&mut rng);
    let context = TestContext::new();
    let mut peer = API::new(DEFAULT_METRICS, config, keypair, context);

    for i in 0..3 {
        let remote_keypair = monad_secp::KeyPair::generate(&mut rng);
        let remote_pubkey = remote_keypair.pubkey();
        let remote_addr: SocketAddr = format!("127.0.0.1:800{}", i).parse().unwrap();
        peer.connect(remote_pubkey, remote_addr, DEFAULT_RETRY_ATTEMPTS)
            .unwrap();
    }

    let extra_keypair = monad_secp::KeyPair::generate(&mut rng);
    let extra_pubkey = extra_keypair.pubkey();
    let extra_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let result = peer.connect(extra_pubkey, extra_addr, DEFAULT_RETRY_ATTEMPTS);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(
        err,
        monad_wireauth::Error::TooManyInitiatedSessions { limit: 3 }
    ));
}

#[test]
fn test_handshake_response_completes_at_pending_capacity() {
    let mut rng = rng();
    let initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
    let initiator_pubkey = initiator_keypair.pubkey();
    let mut initiator = API::new(
        DEFAULT_METRICS,
        Config {
            max_pending_initiated_sessions: 1,
            max_pending_accepted_sessions: 1,
            ..Config::default()
        },
        initiator_keypair,
        TestContext::new(),
    );
    let (mut responder, responder_pubkey, _, _) = create_manager();
    let (mut other, other_pubkey, _, _) = create_manager();
    let initiator_addr: SocketAddr = "10.0.0.1:8000".parse().unwrap();
    let responder_addr: SocketAddr = "10.0.0.2:8000".parse().unwrap();
    let other_addr: SocketAddr = "10.0.0.3:8000".parse().unwrap();

    initiator
        .connect(responder_pubkey, responder_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init = collect::<HandshakeInitiation>(&mut initiator);
    dispatch(&mut responder, &init, initiator_addr);
    let response = collect::<HandshakeResponse>(&mut responder);

    // Fill the accepted pool independently of our occupied initiated slot.
    other
        .connect(initiator_pubkey, initiator_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let other_init = collect::<HandshakeInitiation>(&mut other);
    dispatch(&mut initiator, &other_init, other_addr);
    let other_response = collect::<HandshakeResponse>(&mut initiator);
    let (mut extra, _, _, _) = create_manager();
    let extra_addr: SocketAddr = "10.0.0.4:8000".parse().unwrap();
    extra
        .connect(initiator_pubkey, initiator_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let extra_init = collect::<HandshakeInitiation>(&mut extra);
    dispatch(&mut initiator, &extra_init, extra_addr);
    assert!(initiator.next_packet().is_none());
    assert!(matches!(
        initiator.connect(other_pubkey, other_addr, DEFAULT_RETRY_ATTEMPTS),
        Err(monad_wireauth::Error::TooManyInitiatedSessions { limit: 1 })
    ));

    // Exemption from the pending limit must not bypass source-address validation.
    let mut wrong_source_response = response.clone();
    let Packet::Control(control) = Packet::try_from(&mut wrong_source_response[..]).unwrap() else {
        panic!("expected handshake response");
    };
    assert!(matches!(
        initiator.dispatch_control(control, other_addr),
        Err(monad_wireauth::Error::HandshakeResponseAddressMismatch { .. })
    ));
    assert!(!initiator.is_connected_public_key(&responder_pubkey));

    // A valid response completes even while both pending budgets are full.
    dispatch(&mut initiator, &response, responder_addr);
    assert!(initiator.is_connected_public_key(&responder_pubkey));
    let (_, confirmation) = initiator.next_packet().unwrap();
    dispatch(&mut responder, &confirmation, initiator_addr);
    assert!(responder.is_connected_public_key(&initiator_pubkey));

    // Completion frees the initiated slot despite the accepted pool remaining full.
    initiator
        .connect(other_pubkey, other_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    collect::<HandshakeInitiation>(&mut initiator);

    // The first authenticated packet also promotes an accepted session at capacity.
    dispatch(&mut other, &other_response, initiator_addr);
    let confirmation = collect::<DataPacketHeader>(&mut other);
    dispatch(&mut initiator, &confirmation, other_addr);
    assert!(initiator.is_connected_public_key(&other_pubkey));
}

#[test]
fn test_buffer_limit_per_session() {
    init_tracing();
    let config = Config {
        max_buffered_bytes_per_session: 100,
        ..Config::default()
    };

    let mut rng = rng();
    let keypair = monad_secp::KeyPair::generate(&mut rng);
    let context = TestContext::new();
    let mut peer = API::new(DEFAULT_METRICS, config, keypair, context);

    let remote_keypair = monad_secp::KeyPair::generate(&mut rng);
    let remote_pubkey = remote_keypair.pubkey();
    let remote_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    peer.connect(remote_pubkey, remote_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    peer.buffer_message(&remote_pubkey, bytes::Bytes::from(vec![0u8; 50]))
        .unwrap();

    peer.buffer_message(&remote_pubkey, bytes::Bytes::from(vec![0u8; 40]))
        .unwrap();

    let result = peer.buffer_message(&remote_pubkey, bytes::Bytes::from(vec![0u8; 20]));
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(
        err,
        monad_wireauth::Error::BufferLimitExceeded {
            size: 110,
            limit: 100
        }
    ));
}

#[test]
fn test_buffer_message_session_not_found() {
    init_tracing();
    let (mut peer, _, _, _) = create_manager();

    let mut rng = rng();
    let remote_keypair = monad_secp::KeyPair::generate(&mut rng);
    let remote_pubkey = remote_keypair.pubkey();

    let result = peer.buffer_message(&remote_pubkey, bytes::Bytes::from_static(b"test"));
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, monad_wireauth::Error::SessionNotFound));
}

#[test]
fn test_gc_timer_reset_on_useful_data() {
    init_tracing();
    let config = Config {
        gc_idle_timeout: Duration::from_secs(10),
        session_timeout: Duration::from_secs(1000),
        session_timeout_jitter: Duration::ZERO,
        keepalive_interval: Duration::from_secs(3),
        keepalive_jitter: Duration::ZERO,
        ..Config::default()
    };

    let mut rng = rng();
    let keypair1 = monad_secp::KeyPair::generate(&mut rng);
    let peer1_pubkey = keypair1.pubkey();
    let context1 = TestContext::new();
    let mut peer1 = API::new(DEFAULT_METRICS, config.clone(), keypair1, context1.clone());

    let keypair2 = monad_secp::KeyPair::generate(&mut rng);
    let peer2_pubkey = keypair2.pubkey();
    let context2 = TestContext::new();
    let mut peer2 = API::new(DEFAULT_METRICS, config, keypair2, context2.clone());

    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);
    let resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &resp, peer2_addr);
    let confirm = collect::<DataPacketHeader>(&mut peer1);
    dispatch(&mut peer2, &confirm, peer1_addr);

    // advance close to gc timeout, peer1 sends (resets peer1 gc), peer2 receives (resets peer2 gc)
    context1.advance_time(Duration::from_secs(8));
    context2.advance_time(Duration::from_secs(8));
    peer1.tick();
    peer2.tick();
    let mut plaintext1 = b"from peer1".to_vec();
    let packet1 = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext1);
    assert_eq!(decrypt(&mut peer2, &packet1, peer1_addr), b"from peer1");

    // advance again, peer2 sends (resets peer2 gc), peer1 receives (resets peer1 gc)
    context1.advance_time(Duration::from_secs(8));
    context2.advance_time(Duration::from_secs(8));
    peer1.tick();
    peer2.tick();
    let mut plaintext2 = b"from peer2".to_vec();
    let packet2 = encrypt(&mut peer2, &peer1_pubkey, &mut plaintext2);
    assert_eq!(decrypt(&mut peer1, &packet2, peer2_addr), b"from peer2");

    // both sessions still alive after useful data exchange
    assert!(peer1.is_connected_public_key(&peer2_pubkey));
    assert!(peer2.is_connected_public_key(&peer1_pubkey));

    // send keepalives multiple times - they should NOT reset gc timer
    for _ in 0..3 {
        context1.advance_time(Duration::from_secs(4));
        context2.advance_time(Duration::from_secs(4));
        peer1.tick();
        peer2.tick();

        // keepalives are triggered and exchanged
        if let Some((addr, keepalive)) = peer1.next_packet() {
            assert_eq!(addr, peer2_addr);
            dispatch(&mut peer2, &keepalive, peer1_addr);
        }
        if let Some((addr, keepalive)) = peer2.next_packet() {
            assert_eq!(addr, peer1_addr);
            dispatch(&mut peer1, &keepalive, peer2_addr);
        }
    }

    // sessions terminated by gc despite keepalives being exchanged
    assert!(!peer1.is_connected_public_key(&peer2_pubkey));
    assert!(!peer2.is_connected_public_key(&peer1_pubkey));
}

#[test]
fn test_gc_terminate_has_no_side_effects() {
    init_tracing();
    let config = Config {
        gc_idle_timeout: Duration::from_secs(5),
        session_timeout: Duration::from_secs(1000),
        session_timeout_jitter: Duration::ZERO,
        keepalive_interval: Duration::from_secs(1),
        keepalive_jitter: Duration::ZERO,
        ..Config::default()
    };

    let mut rng = rng();
    let keypair1 = monad_secp::KeyPair::generate(&mut rng);
    let peer1_pubkey = keypair1.pubkey();
    let context1 = TestContext::new();
    let mut peer1 = API::new(DEFAULT_METRICS, config.clone(), keypair1, context1.clone());

    let keypair2 = monad_secp::KeyPair::generate(&mut rng);
    let peer2_pubkey = keypair2.pubkey();
    let context2 = TestContext::new();
    let mut peer2 = API::new(DEFAULT_METRICS, config, keypair2, context2.clone());

    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    // Establish session without exchanging any useful data.
    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);
    let resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &resp, peer2_addr);
    let confirm = collect::<DataPacketHeader>(&mut peer1);
    dispatch(&mut peer2, &confirm, peer1_addr);

    // Drain any leftover packets from handshake.
    while peer1.next_packet().is_some() {}
    while peer2.next_packet().is_some() {}

    // Jump past GC and keepalive deadline; the tick should terminate sessions without
    // enqueueing a keepalive (or other actions) in the same tick.
    context1.advance_time(Duration::from_secs(6));
    context2.advance_time(Duration::from_secs(6));
    peer1.tick();
    peer2.tick();

    assert!(peer1.next_packet().is_none());
    assert!(peer2.next_packet().is_none());

    assert!(!peer1.is_connected_public_key(&peer2_pubkey));
    assert!(!peer2.is_connected_public_key(&peer1_pubkey));
}

// API-only peers for exercising admission, overlap, and timer-driven handshakes.
struct LimitPeer {
    api: API<TestContext>,
    public_key: monad_secp::PubKey,
    context: TestContext,
    addr: SocketAddr,
}

impl LimitPeer {
    fn new(addr: &str, config: Config) -> Self {
        let key = monad_secp::KeyPair::generate(&mut rng());
        let public_key = key.pubkey();
        let context = TestContext::new();
        Self {
            api: API::new(DEFAULT_METRICS, config, key, context.clone()),
            public_key,
            context,
            addr: addr.parse().unwrap(),
        }
    }

    fn receive(&mut self, mut packet: Vec<u8>, from: SocketAddr) -> monad_wireauth::Result<()> {
        match Packet::try_from(packet.as_mut_slice()).unwrap() {
            Packet::Control(control) => self.api.dispatch_control(control, from),
            Packet::Data(data) => self.api.decrypt(data, from).map(|_| ()),
        }
    }

    fn finish(&mut self, responder: &mut Self) -> monad_wireauth::Result<()> {
        responder.receive(collect::<HandshakeInitiation>(&mut self.api), self.addr)?;
        self.receive(
            collect::<HandshakeResponse>(&mut responder.api),
            responder.addr,
        )?;
        responder.receive(collect::<DataPacketHeader>(&mut self.api), self.addr)
    }

    fn establish(&mut self, responder: &mut Self) -> monad_wireauth::Result<()> {
        self.context.advance_time(Duration::from_millis(1));
        responder.context.advance_time(Duration::from_millis(1));
        self.api
            .connect(responder.public_key, responder.addr, DEFAULT_RETRY_ATTEMPTS)?;
        self.finish(responder)
    }

    fn metric(&self, definition: &'static monad_executor::MetricDef) -> u64 {
        self.api
            .metrics()
            .into_inner()
            .into_iter()
            .find(|(name, _, _)| *name == definition.name)
            .unwrap()
            .1
    }
}

#[rstest::rstest]
#[case(Some(1), true)]
#[case(Some(1), false)]
#[case(Some(4), true)]
#[case(Some(4), false)]
#[case(None, true)]
#[case(None, false)]
fn test_existing_peer_exception_bounds_established_sessions(
    #[case] configured_limit: Option<usize>,
    #[case] victim_initiates: bool,
) {
    let mut config = Config::default();
    if let Some(limit) = configured_limit {
        config.max_established_peers_per_ip = limit;
    }
    let limit = config.max_established_peers_per_ip;
    let mut victim = LimitPeer::new("10.0.0.1:9000", config);
    let mut peers: Vec<_> = (0..limit)
        .map(|i| LimitPeer::new(&format!("10.0.0.2:{}", 9000 + i), Config::default()))
        .collect();
    let mut expected_transports = 0;
    let mut oldest_packets = Vec::new();
    // Repeated API handshakes in both roles, with source-port churn. Never tick:
    // removal must happen on promotion, independently of timeout cleanup.
    for round in 0..16 {
        for (i, peer) in peers.iter_mut().enumerate() {
            peer.addr
                .set_port((9000 + round * limit + i).try_into().unwrap());
            for local_initiator in [true, false] {
                if local_initiator {
                    victim.establish(peer).unwrap();
                } else {
                    peer.establish(&mut victim).unwrap();
                }
                if round < 2 {
                    expected_transports += 1;
                }
                assert_eq!(
                    victim.metric(DEFAULT_METRICS.state_transport_sessions),
                    expected_transports
                );
                assert_eq!(
                    victim.metric(DEFAULT_METRICS.state_allocated_indices),
                    expected_transports
                );
                if round == 0 {
                    // Save a packet for the first session in each role. It must
                    // remain decryptable as the previous session, then be evicted.
                    let packet =
                        encrypt(&mut peer.api, &victim.public_key, &mut b"old key".to_vec());
                    oldest_packets.push((peer.addr, packet));
                }
            }
        }
        if round == 1 {
            for (from, packet) in &oldest_packets {
                victim.receive(packet.clone(), *from).unwrap();
            }
        } else if round == 2 {
            for (from, packet) in &oldest_packets {
                // Require a missing receiver index, not merely a replay error.
                assert!(matches!(
                    victim.receive(packet.clone(), *from),
                    Err(monad_wireauth::Error::SessionIndexNotFound { .. })
                ));
            }
        }
    }
    assert_eq!(expected_transports, (4 * limit) as u64);
    for peer in &mut peers {
        let packet = encrypt(
            &mut victim.api,
            &peer.public_key,
            &mut b"after rekey".to_vec(),
        );
        assert_eq!(decrypt(&mut peer.api, &packet, victim.addr), b"after rekey");
    }
    let decrypt_errors = victim.metric(DEFAULT_METRICS.error_decrypt);

    let mut extra = LimitPeer::new("10.0.0.2:9999", Config::default());
    let result = if victim_initiates {
        victim.establish(&mut extra)
    } else {
        extra.establish(&mut victim)
    };
    assert!(
        matches!(result, Err(monad_wireauth::Error::TooManyEstablishedPeersForIp { limit: actual, .. }) if actual == limit)
    );
    assert!(!victim.api.is_connected_public_key(&extra.public_key));
    assert_eq!(
        victim.metric(DEFAULT_METRICS.error_established_peer_limit),
        1
    );
    assert_eq!(victim.metric(DEFAULT_METRICS.error_session_exhausted), 0);
    assert_eq!(victim.metric(DEFAULT_METRICS.error_decrypt), decrypt_errors);
    assert_eq!(victim.metric(DEFAULT_METRICS.state_initiating_sessions), 0);
    assert_eq!(victim.metric(DEFAULT_METRICS.state_responding_sessions), 0);
    assert_eq!(
        victim.metric(DEFAULT_METRICS.state_allocated_indices),
        expected_transports
    );

    victim.api.disconnect(&peers[0].public_key);
    if victim_initiates {
        victim.establish(&mut extra).unwrap();
    } else {
        extra.establish(&mut victim).unwrap();
    }
    assert!(victim.api.is_connected_public_key(&extra.public_key));
}

#[rstest::rstest]
#[case(true)]
#[case(false)]
fn test_peer_allowance_survives_old_session_expiry(#[case] victim_initiates: bool) {
    let config = Config {
        max_established_peers_per_ip: 1,
        session_timeout: Duration::from_secs(1000),
        session_timeout_jitter: Duration::ZERO,
        keepalive_interval: Duration::from_secs(1000),
        max_session_duration: Duration::from_secs(5),
        ..Config::default()
    };
    let mut victim = LimitPeer::new("10.0.0.1:9000", config.clone());
    let mut peer = LimitPeer::new("10.0.0.2:9000", config);
    let mut extra = LimitPeer::new("10.0.0.2:9001", Config::default());
    for round in 0..2 {
        if round != 0 {
            victim.context.advance_time(Duration::from_secs(3));
            peer.context.advance_time(Duration::from_secs(3));
        }
        if victim_initiates {
            victim.establish(&mut peer).unwrap();
        } else {
            peer.establish(&mut victim).unwrap();
        }
    }
    victim.context.advance_time(Duration::from_millis(2100));
    victim.api.tick();
    assert_eq!(victim.metric(DEFAULT_METRICS.state_transport_sessions), 1);
    assert!(matches!(
        extra.establish(&mut victim),
        Err(monad_wireauth::Error::TooManyEstablishedPeersForIp { .. })
    ));
    victim.context.advance_time(Duration::from_secs(3));
    victim.api.tick();
    assert_eq!(victim.metric(DEFAULT_METRICS.state_transport_sessions), 0);
    extra.establish(&mut victim).unwrap();
}

#[test]
fn test_existing_identity_must_fit_new_ip_limit() {
    let mut victim = LimitPeer::new(
        "10.0.0.1:9000",
        Config {
            max_established_peers_per_ip: 1,
            ..Config::default()
        },
    );
    let mut moving = LimitPeer::new("10.0.0.2:9000", Config::default());
    let mut resident = LimitPeer::new("10.0.0.3:9000", Config::default());
    moving.establish(&mut victim).unwrap();
    resident.establish(&mut victim).unwrap();
    moving.addr = "10.0.0.3:9001".parse().unwrap();
    assert!(matches!(
        moving.establish(&mut victim),
        Err(monad_wireauth::Error::TooManyEstablishedPeersForIp { .. })
    ));
    victim.api.disconnect(&resident.public_key);
    moving.establish(&mut victim).unwrap();
    // The same identity still occupies its allowance on the old IP as well.
    let mut extra = LimitPeer::new("10.0.0.2:9001", Config::default());
    assert!(matches!(
        extra.establish(&mut victim),
        Err(monad_wireauth::Error::TooManyEstablishedPeersForIp { .. })
    ));
    victim.api.disconnect(&moving.public_key);
    extra.establish(&mut victim).unwrap();
}

#[rstest::rstest]
#[case(1, 3, true)]
#[case(3, 1, true)]
#[case(1, 3, false)]
#[case(3, 1, false)]
#[case(0, 2, true)]
#[case(2, 0, false)]
fn test_pending_accepted_and_initiated_budgets_are_independent(
    #[case] accepted_limit: usize,
    #[case] initiated_limit: usize,
    #[case] accepted_first: bool,
) {
    let mut victim = LimitPeer::new(
        "10.0.0.1:9000",
        Config {
            max_pending_accepted_sessions: accepted_limit,
            max_pending_initiated_sessions: initiated_limit,
            ..Config::default()
        },
    );
    for fill_accepted in [accepted_first, !accepted_first] {
        let limit = if fill_accepted {
            accepted_limit
        } else {
            initiated_limit
        };
        for i in 0..limit {
            let mut peer = LimitPeer::new(&format!("10.0.0.2:{}", 9000 + i), Config::default());
            if fill_accepted {
                peer.api.connect(victim.public_key, victim.addr, 0).unwrap();
                victim
                    .receive(collect::<HandshakeInitiation>(&mut peer.api), peer.addr)
                    .unwrap();
                collect::<HandshakeResponse>(&mut victim.api);
            } else {
                victim.api.connect(peer.public_key, peer.addr, 0).unwrap();
                collect::<HandshakeInitiation>(&mut victim.api);
            }
        }
        // Verify rejection in this direction before filling the other budget.
        let mut extra = LimitPeer::new("10.0.0.3:9000", Config::default());
        if fill_accepted {
            extra
                .api
                .connect(victim.public_key, victim.addr, 0)
                .unwrap();
            victim
                .receive(collect::<HandshakeInitiation>(&mut extra.api), extra.addr)
                .unwrap();
            assert!(victim.api.next_packet().is_none());
            assert_eq!(
                victim.metric(DEFAULT_METRICS.filter_pending_accepted_session_limit),
                1
            );
        } else {
            assert!(
                matches!(victim.api.connect(extra.public_key, extra.addr, 0),
                Err(monad_wireauth::Error::TooManyInitiatedSessions { limit }) if limit == initiated_limit)
            );
            assert_eq!(
                victim.metric(DEFAULT_METRICS.error_pending_initiated_session_limit),
                1
            );
        }
    }
    assert_eq!(
        victim.metric(DEFAULT_METRICS.state_initiating_sessions),
        initiated_limit as u64
    );
    assert_eq!(
        victim.metric(DEFAULT_METRICS.state_responding_sessions),
        accepted_limit as u64
    );
    assert_eq!(victim.metric(DEFAULT_METRICS.error_session_exhausted), 0);
}

#[test]
fn test_pending_peer_does_not_reserve_established_allowance() {
    let mut victim = LimitPeer::new(
        "10.0.0.1:9000",
        Config {
            max_established_peers_per_ip: 1,
            ..Config::default()
        },
    );
    let mut pending = LimitPeer::new("10.0.0.2:9000", Config::default());
    let mut honest = LimitPeer::new("10.0.0.2:9001", Config::default());
    pending
        .api
        .connect(victim.public_key, victim.addr, 0)
        .unwrap();
    victim
        .receive(
            collect::<HandshakeInitiation>(&mut pending.api),
            pending.addr,
        )
        .unwrap();
    let held_response = collect::<HandshakeResponse>(&mut victim.api);
    // Pending state for another key on the same IP does not prevent promotion.
    honest.establish(&mut victim).unwrap();
    pending.receive(held_response, victim.addr).unwrap();
    let confirmation = collect::<DataPacketHeader>(&mut pending.api);
    assert!(matches!(
        victim.receive(confirmation, pending.addr),
        Err(monad_wireauth::Error::TooManyEstablishedPeersForIp { .. })
    ));
    assert!(victim.api.is_connected_public_key(&honest.public_key));
    assert!(!victim
        .api
        .has_any_session_by_public_key(&pending.public_key));
}

#[test]
fn test_pending_responders_cannot_exceed_transport_limit_on_promotion() {
    let mut victim = LimitPeer::new(
        "10.0.0.1:9000",
        Config {
            total_transport_sessions: 1,
            ..Config::default()
        },
    );
    let mut first = LimitPeer::new("10.0.0.2:9000", Config::default());
    let mut second = LimitPeer::new("10.0.0.3:9000", Config::default());

    for peer in [&mut first, &mut second] {
        peer.api.connect(victim.public_key, victim.addr, 0).unwrap();
        victim
            .receive(collect::<HandshakeInitiation>(&mut peer.api), peer.addr)
            .unwrap();
        peer.receive(collect::<HandshakeResponse>(&mut victim.api), victim.addr)
            .unwrap();
    }
    assert_eq!(victim.metric(DEFAULT_METRICS.state_responding_sessions), 2);

    victim
        .receive(collect::<DataPacketHeader>(&mut first.api), first.addr)
        .unwrap();
    assert!(matches!(
        victim.receive(collect::<DataPacketHeader>(&mut second.api), second.addr),
        Err(monad_wireauth::Error::TooManyTransportSessions { limit: 1 })
    ));
    assert_eq!(victim.metric(DEFAULT_METRICS.state_transport_sessions), 1);
    assert_eq!(victim.metric(DEFAULT_METRICS.state_responding_sessions), 0);
    assert_eq!(victim.metric(DEFAULT_METRICS.state_allocated_indices), 1);
    assert_eq!(
        victim.metric(DEFAULT_METRICS.error_transport_session_limit),
        1
    );
    assert!(victim.api.is_connected_public_key(&first.public_key));
    assert!(!victim.api.has_any_session_by_public_key(&second.public_key));
}
