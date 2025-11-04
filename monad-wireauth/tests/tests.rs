use std::{convert::TryFrom, net::SocketAddr, time::Duration};

use monad_wireauth::{
    messages::{CookieReply, DataPacketHeader, HandshakeInitiation, HandshakeResponse, Packet},
    Config, TestContext, API, DEFAULT_RETRY_ATTEMPTS,
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

//1. peer1 initiates to peer2
//2. peer2 initiates to peer1
//3. peer2 receives peer1 init and sends response
//4. peer1 receives peer2 init and sends response
//5. peer1 receives peer2 response
//6. peer2 receives peer1 response
//7. peer1 encrypts message to peer2
//8. peer2 decrypts message from peer1
//9. peer2 encrypts message to peer1
//10. peer1 decrypts message from peer2
#[test]
fn test_concurrent_init() {
    init_tracing();
    let (mut peer1, peer1_pubkey, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_pubkey.clone(), peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    peer2
        .connect(peer1_pubkey.clone(), peer1_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init1 = collect::<HandshakeInitiation>(&mut peer1);
    let init2 = collect::<HandshakeInitiation>(&mut peer2);

    dispatch(&mut peer2, &init1, peer1_addr);
    dispatch(&mut peer1, &init2, peer2_addr);

    let resp2 = collect::<HandshakeResponse>(&mut peer2);
    let resp1 = collect::<HandshakeResponse>(&mut peer1);

    dispatch(&mut peer1, &resp2, peer2_addr);
    dispatch(&mut peer2, &resp1, peer1_addr);

    let mut plaintext1 = b"hello from peer1".to_vec();
    let packet1 = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext1);
    let decrypted1 = decrypt(&mut peer2, &packet1, peer1_addr);
    assert_eq!(decrypted1, b"hello from peer1");

    let mut plaintext2 = b"hello from peer2".to_vec();
    let packet2 = encrypt(&mut peer2, &peer1_pubkey, &mut plaintext2);
    let decrypted2 = decrypt(&mut peer1, &packet2, peer2_addr);
    assert_eq!(decrypted2, b"hello from peer2");
}

//1. peer1 connects to peer2 with 2 retries
//2. peer1 sends first init - dropped
//3. advance time and tick - peer1 retries
//4. peer1 sends second init - dropped
//5. advance time and tick - peer1 retries
//6. peer1 sends third init - delivered to peer2
//7. peer2 sends response
//8. peer1 receives response and completes handshake
//9. exchange several messages
#[test]
fn test_retries() {
    init_tracing();
    let (mut peer1, _, peer1_ctx, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1.connect(peer2_pubkey.clone(), peer2_addr, 2).unwrap();

    let _init1 = collect::<HandshakeInitiation>(&mut peer1);

    peer1_ctx.advance_time(Duration::from_secs(11));
    peer1.tick();
    let _init2 = collect::<HandshakeInitiation>(&mut peer1);

    peer1_ctx.advance_time(Duration::from_secs(11));
    peer1.tick();
    let init3 = collect::<HandshakeInitiation>(&mut peer1);

    dispatch(&mut peer2, &init3, peer1_addr);
    let resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &resp, peer2_addr);

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

//1. create 5 peers with independent managers
//2. each peer initiates to all other peers
//3. complete all handshakes
//4. each peer exchanges messages with all other peers
#[test]
fn test_five_peers() {
    init_tracing();
    let (mut m0, pk0, _, _) = create_manager();
    let (mut m1, pk1, _, _) = create_manager();
    let (mut m2, pk2, _, _) = create_manager();
    let (mut m3, pk3, _, _) = create_manager();
    let (mut m4, pk4, _, _) = create_manager();
    let a0: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let a1: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let a2: SocketAddr = "127.0.0.1:8002".parse().unwrap();
    let a3: SocketAddr = "127.0.0.1:8003".parse().unwrap();
    let a4: SocketAddr = "127.0.0.1:8004".parse().unwrap();

    m0.connect(pk1.clone(), a1, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m0.connect(pk2.clone(), a2, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m0.connect(pk3.clone(), a3, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m0.connect(pk4.clone(), a4, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m1.connect(pk0.clone(), a0, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m1.connect(pk2.clone(), a2, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m1.connect(pk3.clone(), a3, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m1.connect(pk4.clone(), a4, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m2.connect(pk0.clone(), a0, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m2.connect(pk1.clone(), a1, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m2.connect(pk3.clone(), a3, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m2.connect(pk4.clone(), a4, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m3.connect(pk0.clone(), a0, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m3.connect(pk1.clone(), a1, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m3.connect(pk2.clone(), a2, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m3.connect(pk4, a4, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m4.connect(pk0.clone(), a0, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m4.connect(pk1.clone(), a1, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m4.connect(pk2, a2, DEFAULT_RETRY_ATTEMPTS).unwrap();
    m4.connect(pk3.clone(), a3, DEFAULT_RETRY_ATTEMPTS).unwrap();

    let i01 = collect::<HandshakeInitiation>(&mut m0);
    let i02 = collect::<HandshakeInitiation>(&mut m0);
    let i03 = collect::<HandshakeInitiation>(&mut m0);
    let i04 = collect::<HandshakeInitiation>(&mut m0);
    let i10 = collect::<HandshakeInitiation>(&mut m1);
    let i12 = collect::<HandshakeInitiation>(&mut m1);
    let i13 = collect::<HandshakeInitiation>(&mut m1);
    let i14 = collect::<HandshakeInitiation>(&mut m1);
    let i20 = collect::<HandshakeInitiation>(&mut m2);
    let i21 = collect::<HandshakeInitiation>(&mut m2);
    let i23 = collect::<HandshakeInitiation>(&mut m2);
    let i24 = collect::<HandshakeInitiation>(&mut m2);
    let i30 = collect::<HandshakeInitiation>(&mut m3);
    let i31 = collect::<HandshakeInitiation>(&mut m3);
    let i32 = collect::<HandshakeInitiation>(&mut m3);
    let i34 = collect::<HandshakeInitiation>(&mut m3);
    let i40 = collect::<HandshakeInitiation>(&mut m4);
    let i41 = collect::<HandshakeInitiation>(&mut m4);
    let i42 = collect::<HandshakeInitiation>(&mut m4);
    let i43 = collect::<HandshakeInitiation>(&mut m4);

    dispatch(&mut m1, &i01, a0);
    dispatch(&mut m2, &i02, a0);
    dispatch(&mut m3, &i03, a0);
    dispatch(&mut m4, &i04, a0);
    dispatch(&mut m0, &i10, a1);
    dispatch(&mut m2, &i12, a1);
    dispatch(&mut m3, &i13, a1);
    dispatch(&mut m4, &i14, a1);
    dispatch(&mut m0, &i20, a2);
    dispatch(&mut m1, &i21, a2);
    dispatch(&mut m3, &i23, a2);
    dispatch(&mut m4, &i24, a2);
    dispatch(&mut m0, &i30, a3);
    dispatch(&mut m1, &i31, a3);
    dispatch(&mut m2, &i32, a3);
    dispatch(&mut m4, &i34, a3);
    dispatch(&mut m0, &i40, a4);
    dispatch(&mut m1, &i41, a4);
    dispatch(&mut m2, &i42, a4);
    dispatch(&mut m3, &i43, a4);

    let r10 = collect::<HandshakeResponse>(&mut m1);
    let r20 = collect::<HandshakeResponse>(&mut m2);
    let r30 = collect::<HandshakeResponse>(&mut m3);
    let r40 = collect::<HandshakeResponse>(&mut m4);
    let r01 = collect::<HandshakeResponse>(&mut m0);
    let r21 = collect::<HandshakeResponse>(&mut m2);
    let r31 = collect::<HandshakeResponse>(&mut m3);
    let r41 = collect::<HandshakeResponse>(&mut m4);
    let r02 = collect::<HandshakeResponse>(&mut m0);
    let r12 = collect::<HandshakeResponse>(&mut m1);
    let r32 = collect::<HandshakeResponse>(&mut m3);
    let r42 = collect::<HandshakeResponse>(&mut m4);
    let r03 = collect::<HandshakeResponse>(&mut m0);
    let r13 = collect::<HandshakeResponse>(&mut m1);
    let r23 = collect::<HandshakeResponse>(&mut m2);
    let r43 = collect::<HandshakeResponse>(&mut m4);
    let r04 = collect::<HandshakeResponse>(&mut m0);
    let r14 = collect::<HandshakeResponse>(&mut m1);
    let r24 = collect::<HandshakeResponse>(&mut m2);
    let r34 = collect::<HandshakeResponse>(&mut m3);

    dispatch(&mut m0, &r10, a1);
    dispatch(&mut m0, &r20, a2);
    dispatch(&mut m0, &r30, a3);
    dispatch(&mut m0, &r40, a4);
    dispatch(&mut m1, &r01, a0);
    dispatch(&mut m1, &r21, a2);
    dispatch(&mut m1, &r31, a3);
    dispatch(&mut m1, &r41, a4);
    dispatch(&mut m2, &r02, a0);
    dispatch(&mut m2, &r12, a1);
    dispatch(&mut m2, &r32, a3);
    dispatch(&mut m2, &r42, a4);
    dispatch(&mut m3, &r03, a0);
    dispatch(&mut m3, &r13, a1);
    dispatch(&mut m3, &r23, a2);
    dispatch(&mut m3, &r43, a4);
    dispatch(&mut m4, &r04, a0);
    dispatch(&mut m4, &r14, a1);
    dispatch(&mut m4, &r24, a2);
    dispatch(&mut m4, &r34, a3);

    let mut plaintext = b"0->1".to_vec();
    let packet = encrypt(&mut m0, &pk1, &mut plaintext);
    let decrypted = decrypt(&mut m1, &packet, a0);
    assert_eq!(decrypted, b"0->1");

    let mut plaintext = b"2->3".to_vec();
    let packet = encrypt(&mut m2, &pk3, &mut plaintext);
    let decrypted = decrypt(&mut m3, &packet, a2);
    assert_eq!(decrypted, b"2->3");

    let mut plaintext = b"4->0".to_vec();
    let packet = encrypt(&mut m4, &pk0, &mut plaintext);
    let decrypted = decrypt(&mut m0, &packet, a4);
    assert_eq!(decrypted, b"4->0");
}

//1. peer1 initiates to peer2
//2. complete handshake
//3. peer1 encrypts by public key and sends to peer2
//4. peer1 encrypts by socket and sends to peer2
//5. peer2 decrypts both messages
#[test]
fn test_encrypt_by_pubkey_and_socket() {
    init_tracing();
    let (mut peer1, _, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_pubkey.clone(), peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);
    let resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &resp, peer2_addr);

    let mut plaintext1 = b"by pubkey".to_vec();
    let packet1 = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext1);
    let decrypted1 = decrypt(&mut peer2, &packet1, peer1_addr);
    assert_eq!(decrypted1, b"by pubkey");

    let mut plaintext2 = b"by socket".to_vec();
    let header2 = peer1
        .encrypt_by_socket(&peer2_addr, &mut plaintext2)
        .unwrap();
    let mut packet2 = Vec::with_capacity(DataPacketHeader::SIZE + plaintext2.len());
    packet2.extend_from_slice(header2.as_bytes());
    packet2.extend_from_slice(&plaintext2);
    let decrypted2 = decrypt(&mut peer2, &packet2, peer1_addr);
    assert_eq!(decrypted2, b"by socket");
}

//1. peer1 initiates to peer2 with low handshake rate limit
//2. peer2 sends cookie reply to 1st init
//3. peer1 receives cookie reply and stores it
//4. advance time past session timeout
//5. peer1 tick triggers retry with stored cookie
//6. peer2 accepts init with valid mac2
#[test]
fn test_cookie_reply_on_init() {
    init_tracing();
    let config = Config {
        handshake_rate_limit: 10,
        low_watermark_sessions: 1,
        ..Config::default()
    };

    let mut rng = rng();
    let keypair1 = monad_secp::KeyPair::generate(&mut rng);
    let context1 = TestContext::new();
    let mut peer1 = API::new(config.clone(), keypair1, context1.clone());

    let keypair2 = monad_secp::KeyPair::generate(&mut rng);
    let public_key2 = keypair2.pubkey();
    let context2 = TestContext::new();
    let mut peer2 = API::new(config, keypair2, context2);

    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(public_key2.clone(), peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init1 = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init1, peer1_addr);

    let resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &resp, peer2_addr);

    let data = collect::<DataPacketHeader>(&mut peer1);
    dispatch(&mut peer2, &data, peer1_addr);

    peer1
        .connect(public_key2, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init2 = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init2, peer1_addr);

    let cookie = collect::<CookieReply>(&mut peer2);
    dispatch(&mut peer1, &cookie, peer2_addr);

    context1.advance_time(Duration::from_secs(11));
    peer1.tick();

    let _init2 = collect::<HandshakeInitiation>(&mut peer1);
}

//1. peer1 establishes session with peer2
//2. peer1 attempts connect again to peer2
//3. exchange messages to verify session still works
#[test]
fn test_connect_after_established() {
    init_tracing();
    let (mut peer1, _, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_pubkey.clone(), peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();
    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);
    let resp = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &resp, peer2_addr);

    let mut plaintext = b"before reconnect".to_vec();
    let packet = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext);
    let decrypted = decrypt(&mut peer2, &packet, peer1_addr);
    assert_eq!(decrypted, b"before reconnect");

    let _ = peer1.connect(peer2_pubkey.clone(), peer2_addr, DEFAULT_RETRY_ATTEMPTS);

    let mut plaintext = b"after reconnect".to_vec();
    let packet = encrypt(&mut peer1, &peer2_pubkey, &mut plaintext);
    let decrypted = decrypt(&mut peer2, &packet, peer1_addr);
    assert_eq!(decrypted, b"after reconnect");
}

//1. peer1 initiates to peer2
//2. peer2 accepts init and sends response
//3. peer1 sends same init again
//4. verify peer2 rejects replay
#[test]
fn test_timestamp_replay() {
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
    let _resp = collect::<HandshakeResponse>(&mut peer2);

    let result2 = dispatch(&mut peer2, &init, peer1_addr);
    assert!(result2.is_none());
}

//1. create 10 peer managers with same keypair
//2. each initiates to one responder with distinct key
//3. verify responder hits max accepted sessions limit
#[test]
fn test_too_many_accepted_sessions() {
    init_tracing();
    let config = Config::default();

    let mut rng = rng();
    let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
    let responder_public = responder_keypair.pubkey();
    let responder_ctx = TestContext::new();
    let mut responder = API::new(config.clone(), responder_keypair, responder_ctx);
    let responder_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    let shared_keypair = monad_secp::KeyPair::generate(&mut rng);
    let shared_secret_bytes = shared_keypair.secret_bytes();

    for i in 0..5 {
        let initiator_ctx = TestContext::new();
        let initiator_keypair =
            monad_secp::KeyPair::from_bytes(&mut shared_secret_bytes.clone()).unwrap();
        let mut initiator = API::new(config.clone(), initiator_keypair, initiator_ctx);
        let initiator_addr: SocketAddr = format!("127.0.0.1:800{}", i).parse().unwrap();

        initiator
            .connect(
                responder_public.clone(),
                responder_addr,
                DEFAULT_RETRY_ATTEMPTS,
            )
            .unwrap();

        let init = collect::<HandshakeInitiation>(&mut initiator);
        dispatch(&mut responder, &init, initiator_addr);
    }

    assert!(responder.next_packet().is_some());
}

//1. dispatch random invalid packet
//2. verify error is returned
#[test]
fn test_random_packet_error() {
    init_tracing();

    let random_packet = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let mut packet = random_packet;
    let result = Packet::try_from(&mut packet[..]);
    assert!(result.is_err());
}

//1. create manager with low handshake rate limit
//2. exceed rate limit with multiple inits
//3. verify packets get dropped due to rate limit
#[test]
fn test_filter_drop_rate_limit() {
    init_tracing();
    let config = Config {
        handshake_rate_limit: 2,
        ..Config::default()
    };

    let mut rng = rng();
    let responder_keypair = monad_secp::KeyPair::generate(&mut rng);
    let responder_public = responder_keypair.pubkey();
    let responder_ctx = TestContext::new();
    let mut responder = API::new(config.clone(), responder_keypair, responder_ctx);
    let responder_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    for i in 0..4 {
        let initiator_keypair = monad_secp::KeyPair::generate(&mut rng);
        let initiator_ctx = TestContext::new();
        let mut initiator = API::new(config.clone(), initiator_keypair, initiator_ctx);
        let initiator_addr: SocketAddr = format!("127.0.0.1:800{}", i).parse().unwrap();

        initiator
            .connect(
                responder_public.clone(),
                responder_addr,
                DEFAULT_RETRY_ATTEMPTS,
            )
            .unwrap();

        let init = collect::<HandshakeInitiation>(&mut initiator);
        dispatch(&mut responder, &init, initiator_addr);
    }

    let response_count = (0..4).filter(|_| responder.next_packet().is_some()).count();

    assert!(response_count <= 2);
}

//1. create manager and initiate connection
//2. check next_timer returns some duration
//3. advance time partially
//4. verify timer decreases
//5. advance time past deadline
//6. verify timer returns zero
#[test]
fn test_next_timer() {
    init_tracing();
    let (mut peer1, _, peer1_ctx, config) = create_manager();
    let (_, peer2_pubkey, _, _) = create_manager();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    let timer_before = peer1.next_timer();
    assert_eq!(timer_before, Some(config.handshake_rate_reset_interval));

    peer1
        .connect(peer2_pubkey, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let timer_after = peer1.next_timer();
    assert!(timer_after.is_some());
    let initial_timer = timer_after.unwrap();
    assert!(initial_timer.as_secs() <= 10);

    peer1_ctx.advance_time(Duration::from_secs(5));

    let timer_decreased = peer1.next_timer();
    assert!(timer_decreased.is_some());
    assert!(timer_decreased.unwrap() < initial_timer);

    peer1_ctx.advance_time(Duration::from_secs(20));

    let timer_zero = peer1.next_timer();
    assert!(timer_zero.is_some());
    assert_eq!(timer_zero.unwrap(), Duration::ZERO);
}

//1. peer1 initiates to peer2
//2. peer2 receives init and creates responder session
//3. advance time past session timeout on peer2
//4. peer2 tick triggers responder timeout
//5. verify responder session terminated
#[test]
fn test_responder_timeout() {
    init_tracing();
    let config = Config::default();

    let mut rng = rng();
    let peer1_keypair = monad_secp::KeyPair::generate(&mut rng);
    let peer1_ctx = TestContext::new();
    let mut peer1 = API::new(config.clone(), peer1_keypair, peer1_ctx);

    let peer2_keypair = monad_secp::KeyPair::generate(&mut rng);
    let peer2_public = peer2_keypair.pubkey();
    let peer2_ctx = TestContext::new();
    let mut peer2 = API::new(config, peer2_keypair, peer2_ctx.clone());

    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_public, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);

    let timer_before_timeout = peer2.next_timer();
    assert!(timer_before_timeout.is_some());

    peer2_ctx.advance_time(Duration::from_secs(11));
    peer2.tick();

    let timer_after_timeout = peer2.next_timer();
    assert!(timer_after_timeout.is_some());
}

#[test]
fn test_next_timer_includes_filter_reset() {
    init_tracing();
    let mut rng = rng();
    let config = Config {
        handshake_rate_reset_interval: Duration::from_secs(5),
        ..Config::default()
    };

    let peer_keypair = monad_secp::KeyPair::generate(&mut rng);
    let peer_ctx = TestContext::new();
    let peer = API::new(config, peer_keypair, peer_ctx);

    let next_timer = peer.next_timer();
    assert!(next_timer.is_some());
    assert_eq!(next_timer.unwrap(), Duration::from_secs(5));
}

#[test]
fn test_next_timer_returns_minimum_of_session_and_filter() {
    init_tracing();
    let mut rng = rng();
    let config = Config::default();

    let peer1_keypair = monad_secp::KeyPair::generate(&mut rng);
    let peer1_ctx = TestContext::new();
    let mut peer1 = API::new(config.clone(), peer1_keypair, peer1_ctx);

    let peer2_keypair = monad_secp::KeyPair::generate(&mut rng);
    let peer2_public = peer2_keypair.pubkey();
    let peer2_ctx = TestContext::new();
    let mut peer2 = API::new(config, peer2_keypair, peer2_ctx);

    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();

    peer1
        .connect(peer2_public, peer2_addr, DEFAULT_RETRY_ATTEMPTS)
        .unwrap();

    let init = collect::<HandshakeInitiation>(&mut peer1);
    dispatch(&mut peer2, &init, peer1_addr);

    let response = collect::<HandshakeResponse>(&mut peer2);
    dispatch(&mut peer1, &response, peer2_addr);

    collect::<DataPacketHeader>(&mut peer1);

    let next_timer = peer1.next_timer();
    assert!(next_timer.is_some());
    let timer_value = next_timer.unwrap();
    assert!(timer_value <= Duration::from_secs(3));
    assert!(timer_value > Duration::ZERO);
}

#[test]
fn test_disconnect() {
    init_tracing();
    let (mut peer1, _peer1_pubkey, _, _) = create_manager();
    let (mut peer2, peer2_pubkey, _, _) = create_manager();
    let peer1_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let peer2_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    peer1
        .connect(peer2_pubkey.clone(), peer2_addr, DEFAULT_RETRY_ATTEMPTS)
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
