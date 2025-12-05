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
    Config, Context, TestContext, API, DEFAULT_RETRY_ATTEMPTS,
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
    let manager = API::new(config.clone(), keypair, context);
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
    // 1. create managers with low_watermark_sessions=0 to trigger cookie immediate cookie reply under load
    let config = Config {
        handshake_rate_limit: 10,
        low_watermark_sessions: 0,
        session_timeout_jitter: Duration::ZERO, // to avoid randomness in tests
        ..Config::default()
    };
    let session_timeout = config.session_timeout;

    let mut rng = rng();
    let keypair1 = monad_secp::KeyPair::generate(&mut rng);
    let context1 = TestContext::new();
    let mut peer1 = API::new(config.clone(), keypair1, context1.clone());

    let keypair2 = monad_secp::KeyPair::generate(&mut rng);
    let public_key2 = keypair2.pubkey();
    let context2 = TestContext::new();
    let mut peer2 = API::new(config, keypair2, context2);

    let peer1_addr: SocketAddr = "192.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "192.0.0.2:8002".parse().unwrap();

    // 2. peer1 initiates again - peer2 is now at low_watermark, sends cookie reply
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
    // 1. create responder with max 5 accepted sessions
    let config = Config {
        low_watermark_sessions: 5,
        high_watermark_sessions: 5,
        ..Default::default()
    };

    let mut rng = rng();
    let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
    let responder_public = responder_keypair.pubkey();
    let responder_ctx = TestContext::new();
    let mut responder = API::new(config.clone(), responder_keypair, responder_ctx);
    let responder_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 2. 10 initiators each send init to responder
    for i in 0..10 {
        let initiator_ctx = TestContext::new();
        let initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let mut initiator = API::new(config.clone(), initiator_keypair, initiator_ctx);
        let initiator_addr: SocketAddr = format!("127.0.0.1:800{}", i).parse().unwrap();

        initiator
            .connect(responder_public, responder_addr, DEFAULT_RETRY_ATTEMPTS)
            .unwrap();

        let init = collect::<HandshakeInitiation>(&mut initiator);
        dispatch(&mut responder, &init, initiator_addr);
    }

    // 3. verify responder only accepted 5 sessions (high_watermark_sessions limit)
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
        handshake_rate_limit: 3,
        ..Config::default()
    };

    let mut rng = rng();
    let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
    let responder_public = responder_keypair.pubkey();
    let responder_ctx = TestContext::new();
    let mut responder = API::new(config.clone(), responder_keypair, responder_ctx);
    let responder_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 2. exceed rate limit with 4 inits (limit is 3)
    for i in 0..4 {
        let initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let initiator_ctx = TestContext::new();
        let mut initiator = API::new(config.clone(), initiator_keypair, initiator_ctx);
        let initiator_addr: SocketAddr = format!("127.0.0.1:800{}", i).parse().unwrap();

        initiator
            .connect(responder_public, responder_addr, DEFAULT_RETRY_ATTEMPTS)
            .unwrap();

        let init = collect::<HandshakeInitiation>(&mut initiator);
        dispatch(&mut responder, &init, initiator_addr);
    }

    // 3. verify only 2 responses (first consumed token, remaining 2 accepted)
    let mut pkts = vec![];
    while let Some(pkt) = responder.next_packet() {
        pkts.push(pkt);
    }
    assert_eq!(pkts.len(), 2);
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
    let peer = API::new(config, peer_keypair, peer_ctx.clone());

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
    let mut peer1 = API::new(config.clone(), peer1_keypair, peer1_ctx.clone());

    let peer2_keypair = monad_secp::KeyPair::generate(&mut rng);
    let peer2_public = peer2_keypair.pubkey();
    let peer2_ctx = TestContext::new();
    let mut peer2 = API::new(config, peer2_keypair, peer2_ctx);

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
    let mut peer1 = API::new(config.clone(), keypair1, context1.clone());

    let keypair2 = monad_secp::KeyPair::generate(&mut rng);
    let peer2_pubkey = keypair2.pubkey();
    let context2 = TestContext::new();
    let mut peer2 = API::new(config, keypair2, context2);

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
