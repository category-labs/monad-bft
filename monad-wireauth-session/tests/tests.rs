use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, SystemTime},
};

use monad_wireauth_protocol::{
    common::{PrivateKey, PublicKey, SerializedPublicKey, SessionIndex},
    cookies,
    messages::DataPacket,
};
use monad_wireauth_session::*;
use secp256k1::rand::{rngs::StdRng, SeedableRng};

struct TestEnv {
    rng: StdRng,
    time: Duration,
    system_time: SystemTime,
    config: Config,
}

impl TestEnv {
    fn new() -> Self {
        Self {
            rng: StdRng::seed_from_u64(42),
            time: Duration::ZERO,
            system_time: SystemTime::UNIX_EPOCH,
            config: Config {
                session_timeout: Duration::from_secs(10),
                session_timeout_jitter: Duration::ZERO,
                keepalive_interval: Duration::from_secs(3),
                keepalive_jitter: Duration::ZERO,
                rekey_interval: Duration::from_secs(60),
                rekey_jitter: Duration::ZERO,
                max_session_duration: Duration::from_secs(70),
                ..Default::default()
            },
        }
    }

    fn advance(&mut self, duration: Duration) {
        self.time += duration;
        self.system_time += duration;
    }
}

struct Peer {
    static_key: PrivateKey,
    static_public: PublicKey,
    addr: SocketAddr,
    session_index: SessionIndex,
}

impl Peer {
    fn new(port: u16, index: u32) -> Self {
        let mut rng = StdRng::seed_from_u64(port as u64);
        let (static_public, static_key) =
            monad_wireauth_protocol::crypto::generate_keypair(&mut rng).unwrap();
        Self {
            static_key,
            static_public,
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            session_index: SessionIndex::new(index),
        }
    }
}

//1. alice initiates handshake
//2. bob validates initiation
//3. bob sends response
//4. alice validates response
//5. alice establishes transport
//6. bob establishes transport
//7. alice encrypts data
//8. bob decrypts data
#[test]
fn test_handshake_and_data_exchange() {
    let mut env = TestEnv::new();
    let alice = Peer::new(8001, 1);
    let bob = Peer::new(8002, 2);

    let (mut initiator, (_timer, mut init_msg)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        None,
        DEFAULT_RETRY_ATTEMPTS,
    )
    .unwrap();

    let validated_init =
        Responder::validate_init(&bob.static_key, &bob.static_public, &mut init_msg).unwrap();

    let (responder, _timer, mut resp_msg) = Responder::new(
        &mut env.rng,
        env.time,
        &env.config,
        bob.session_index,
        None,
        validated_init,
        alice.addr,
    )
    .unwrap();

    let validated_resp = initiator
        .validate_response(
            &env.config,
            &alice.static_key,
            &alice.static_public,
            &mut resp_msg,
        )
        .unwrap();

    let (mut initiator_transport, _timer, _header) = initiator.establish(
        &mut env.rng,
        &env.config,
        env.time,
        validated_resp,
        bob.addr,
    );

    let (mut responder_transport, _timer) =
        responder.establish(&mut env.rng, &env.config, env.time);

    let mut plaintext = b"hello world".to_vec();
    let (header, _timer) = initiator_transport.encrypt(&env.config, env.time, &mut plaintext);

    let data_packet = DataPacket {
        header: &header,
        plaintext: &mut plaintext,
    };

    let _timer = responder_transport
        .decrypt(&env.config, env.time, data_packet)
        .unwrap();
    assert_eq!(&plaintext, b"hello world");
}

//1. alice initiates handshake
//2. bob validates initiation
//3. bob sends response
//4. alice validates response
//5. alice establishes transport
//6. bob establishes transport
//7. advance time by keepalive interval -> bob tick triggers first keepalive
//9. advance time by keepalive interval -> bob tick triggers second keepalive
#[test]
fn test_keepalive_sends_twice() {
    let mut env = TestEnv::new();
    let alice = Peer::new(8001, 1);
    let bob = Peer::new(8002, 2);

    let (mut initiator, (_timer, mut init_msg)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        None,
        DEFAULT_RETRY_ATTEMPTS,
    )
    .unwrap();

    let validated_init =
        Responder::validate_init(&bob.static_key, &bob.static_public, &mut init_msg).unwrap();

    let (responder, _timer, mut resp_msg) = Responder::new(
        &mut env.rng,
        env.time,
        &env.config,
        bob.session_index,
        None,
        validated_init,
        alice.addr,
    )
    .unwrap();

    let validated_resp = initiator
        .validate_response(
            &env.config,
            &alice.static_key,
            &alice.static_public,
            &mut resp_msg,
        )
        .unwrap();

    let (_initiator_transport, _timer, _header) = initiator.establish(
        &mut env.rng,
        &env.config,
        env.time,
        validated_resp,
        bob.addr,
    );

    let (mut responder_transport, _timer) =
        responder.establish(&mut env.rng, &env.config, env.time);

    env.advance(env.config.keepalive_interval);

    let result = responder_transport.tick(&env.config, env.time).unwrap();
    assert!(result.1.is_some());
    let message_event = result.1.unwrap();
    assert_eq!(message_event.remote_addr, alice.addr);

    env.advance(env.config.keepalive_interval);

    let result = responder_transport.tick(&env.config, env.time).unwrap();
    assert!(result.1.is_some());
    let message_event = result.1.unwrap();
    assert_eq!(message_event.remote_addr, alice.addr);
}

//1. alice initiates handshake
//2. bob validates initiation
//3. bob sends response
//4. alice validates response
//5. alice establishes transport
//6. bob establishes transport
//7. advance time by keepalive interval -> bob tick triggers keepalive
//8. advance time by session timeout -> alice tick triggers session timeout and rekey
#[test]
fn test_session_timeout_triggers_rekey_after_keepalive() {
    let mut env = TestEnv::new();
    let alice = Peer::new(8001, 1);
    let bob = Peer::new(8002, 2);

    let (mut initiator, (_timer, mut init_msg)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        None,
        DEFAULT_RETRY_ATTEMPTS,
    )
    .unwrap();

    let validated_init =
        Responder::validate_init(&bob.static_key, &bob.static_public, &mut init_msg).unwrap();

    let (responder, _timer, mut resp_msg) = Responder::new(
        &mut env.rng,
        env.time,
        &env.config,
        bob.session_index,
        None,
        validated_init,
        alice.addr,
    )
    .unwrap();

    let validated_resp = initiator
        .validate_response(
            &env.config,
            &alice.static_key,
            &alice.static_public,
            &mut resp_msg,
        )
        .unwrap();

    let (mut initiator_transport, _timer, _header) = initiator.establish(
        &mut env.rng,
        &env.config,
        env.time,
        validated_resp,
        bob.addr,
    );

    let (mut responder_transport, _timer) =
        responder.establish(&mut env.rng, &env.config, env.time);

    env.advance(env.config.keepalive_interval);

    let result = responder_transport.tick(&env.config, env.time).unwrap();
    assert!(result.1.is_some());

    env.advance(env.config.session_timeout);

    let result = initiator_transport.tick(&env.config, env.time).unwrap();
    assert!(result.2.is_some());
    let rekey_event = result.2.unwrap();
    assert_eq!(rekey_event.remote_addr, bob.addr);
}

//1. alice initiates handshake
//2. advance time by session timeout -> alice tick triggers rekey with retry attempts decremented
#[test]
fn test_initiator_timeout_triggers_rekey() {
    let mut env = TestEnv::new();
    let alice = Peer::new(8001, 1);
    let bob = Peer::new(8002, 2);

    let (mut initiator, (_timer, _init_msg)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        None,
        DEFAULT_RETRY_ATTEMPTS,
    )
    .unwrap();

    env.advance(env.config.session_timeout);

    let result = initiator.tick(env.time).unwrap().unwrap();
    assert!(result.1.rekey.is_some());
    let rekey_event = result.1.rekey.unwrap();
    assert_eq!(rekey_event.remote_addr, bob.addr);
    assert_eq!(rekey_event.retry_attempts, DEFAULT_RETRY_ATTEMPTS - 1);
}

//1. alice initiates handshake
//2. bob validates initiation and creates responder
//3. advance time by session timeout -> bob tick triggers termination without rekey
#[test]
fn test_responder_timeout_terminates_session() {
    let mut env = TestEnv::new();
    let alice = Peer::new(8001, 1);
    let bob = Peer::new(8002, 2);

    let (_initiator, (_timer, mut init_msg)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        None,
        DEFAULT_RETRY_ATTEMPTS,
    )
    .unwrap();

    let validated_init =
        Responder::validate_init(&bob.static_key, &bob.static_public, &mut init_msg).unwrap();

    let (mut responder, _timer, _resp_msg) = Responder::new(
        &mut env.rng,
        env.time,
        &env.config,
        bob.session_index,
        None,
        validated_init,
        alice.addr,
    )
    .unwrap();

    env.advance(env.config.session_timeout);

    let result = responder.tick(env.time).unwrap().unwrap();
    assert!(result.1.rekey.is_none());
    assert_eq!(result.1.terminated.remote_addr, alice.addr);
}

//1. alice initiates handshake
//2. alice receives and stores cookie reply
//3. advance time by session timeout -> alice tick triggers rekey with stored cookie
//4. alice re-initiates with stored cookie
//5. bob validates initiation and sends response
//6. alice validates response and establishes transport
#[test]
fn test_cookie_stored_and_reused_after_timeout() {
    let mut env = TestEnv::new();
    let alice = Peer::new(8001, 1);
    let bob = Peer::new(8002, 2);

    let (mut initiator, (_timer, init_msg)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        None,
        DEFAULT_RETRY_ATTEMPTS,
    )
    .unwrap();

    let mac1 = init_msg.mac1.0;
    let cookie_secret = [0u8; 32];
    let nonce = 0u64;
    let cookie = cookies::generate_cookie(&cookie_secret, nonce, &alice.addr);

    let nonce_secret = [0u8; 32];
    let nonce_counter = 0u128;
    let mut cookie_reply = cookies::send_cookie_reply(
        &nonce_secret,
        nonce_counter,
        &SerializedPublicKey::from(&bob.static_public),
        alice.session_index.as_u32(),
        &mac1,
        &cookie,
    )
    .unwrap();

    initiator.handle_cookie(&mut cookie_reply).unwrap();
    let stored_cookie = initiator.stored_cookie().unwrap();

    env.advance(env.config.session_timeout);

    let result = initiator.tick(env.time).unwrap().unwrap();
    assert!(result.1.rekey.is_some());
    let rekey_event = result.1.rekey.unwrap();
    assert_eq!(rekey_event.stored_cookie, Some(stored_cookie));

    let (mut initiator2, (_timer, mut init_msg2)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        Some(stored_cookie),
        DEFAULT_RETRY_ATTEMPTS - 1,
    )
    .unwrap();

    let validated_init =
        Responder::validate_init(&bob.static_key, &bob.static_public, &mut init_msg2).unwrap();

    let (_responder, _timer, mut resp_msg) = Responder::new(
        &mut env.rng,
        env.time,
        &env.config,
        bob.session_index,
        None,
        validated_init,
        alice.addr,
    )
    .unwrap();

    let validated_resp = initiator2
        .validate_response(
            &env.config,
            &alice.static_key,
            &alice.static_public,
            &mut resp_msg,
        )
        .unwrap();

    let (_initiator_transport, _timer, _header) = initiator2.establish(
        &mut env.rng,
        &env.config,
        env.time,
        validated_resp,
        bob.addr,
    );
}

//1. alice initiates handshake with cookie
//2. bob creates responder with stored cookie
//3. verify response has non-zero mac2
#[test]
fn test_rekey_without_cookie_receives_cookie_reply() {
    let mut env = TestEnv::new();
    let alice = Peer::new(8001, 1);
    let bob = Peer::new(8002, 2);

    let cookie_secret = [0u8; 32];
    let nonce = 0u64;
    let cookie = cookies::generate_cookie(&cookie_secret, nonce, &alice.addr);

    let (_initiator, (_timer, mut init_msg)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        Some(cookie),
        DEFAULT_RETRY_ATTEMPTS,
    )
    .unwrap();

    let validated_init =
        Responder::validate_init(&bob.static_key, &bob.static_public, &mut init_msg).unwrap();

    let (_responder, _timer, resp_msg) = Responder::new(
        &mut env.rng,
        env.time,
        &env.config,
        bob.session_index,
        Some(&cookie),
        validated_init,
        alice.addr,
    )
    .unwrap();

    assert_ne!(resp_msg.mac2.0, [0u8; 16]);
}

//1. alice initiates handshake with 0 retries
//2. bob validates initiation and sends response
//3. alice validates response and establishes transport
//4. bob establishes transport
//5. advance time to trigger multiple keepalives
//6. advance time to rekey interval -> alice tick triggers rekey without terminate
#[test]
fn test_rekey_interval_with_zero_retries() {
    let mut env = TestEnv::new();
    let alice = Peer::new(8001, 1);
    let bob = Peer::new(8002, 2);

    let (mut initiator, (_timer, mut init_msg)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        None,
        0,
    )
    .unwrap();

    let validated_init =
        Responder::validate_init(&bob.static_key, &bob.static_public, &mut init_msg).unwrap();

    let (responder, _timer, mut resp_msg) = Responder::new(
        &mut env.rng,
        env.time,
        &env.config,
        bob.session_index,
        None,
        validated_init,
        alice.addr,
    )
    .unwrap();

    let validated_resp = initiator
        .validate_response(
            &env.config,
            &alice.static_key,
            &alice.static_public,
            &mut resp_msg,
        )
        .unwrap();

    let (mut initiator_transport, _timer, _header) = initiator.establish(
        &mut env.rng,
        &env.config,
        env.time,
        validated_resp,
        bob.addr,
    );

    let (mut responder_transport, _timer) =
        responder.establish(&mut env.rng, &env.config, env.time);

    let keepalive_count =
        (env.config.rekey_interval.as_secs() / env.config.keepalive_interval.as_secs()) as usize;
    for _ in 0..keepalive_count {
        env.advance(env.config.keepalive_interval);
        let result = responder_transport.tick(&env.config, env.time).unwrap();
        assert!(result.1.is_some());
        let message_event = result.1.unwrap();
        let mut plaintext = vec![];
        let data_packet = DataPacket {
            header: &message_event.header,
            plaintext: &mut plaintext,
        };
        let _timer = initiator_transport
            .decrypt(&env.config, env.time, data_packet)
            .unwrap();
    }

    let result = initiator_transport.tick(&env.config, env.time).unwrap();
    assert!(result.2.is_some());
    assert!(result.3.is_none());
    let rekey_event = result.2.unwrap();
    assert_eq!(rekey_event.remote_addr, bob.addr);
    assert_eq!(rekey_event.retry_attempts, 0);
}

//1. alice initiates handshake
//2. bob validates initiation and sends response
//3. alice validates response and establishes transport
//4. bob establishes transport
//5. advance time to rekey interval -> alice tick triggers rekey
//6. send keepalives during post-rekey period
//7. advance time to max session duration -> alice tick triggers termination despite keapalives
#[test]
fn test_max_session_duration_terminates_after_rekey() {
    let mut env = TestEnv::new();
    let alice = Peer::new(8001, 1);
    let bob = Peer::new(8002, 2);

    let (mut initiator, (_timer, mut init_msg)) = Initiator::new(
        &mut env.rng,
        env.system_time,
        env.time,
        &env.config,
        alice.session_index,
        &alice.static_key,
        alice.static_public.clone(),
        bob.static_public.clone(),
        bob.addr,
        None,
        DEFAULT_RETRY_ATTEMPTS,
    )
    .unwrap();

    let validated_init =
        Responder::validate_init(&bob.static_key, &bob.static_public, &mut init_msg).unwrap();

    let (responder, _timer, mut resp_msg) = Responder::new(
        &mut env.rng,
        env.time,
        &env.config,
        bob.session_index,
        None,
        validated_init,
        alice.addr,
    )
    .unwrap();

    let validated_resp = initiator
        .validate_response(
            &env.config,
            &alice.static_key,
            &alice.static_public,
            &mut resp_msg,
        )
        .unwrap();

    let (mut initiator_transport, _timer, _header) = initiator.establish(
        &mut env.rng,
        &env.config,
        env.time,
        validated_resp,
        bob.addr,
    );

    let (mut responder_transport, _timer) =
        responder.establish(&mut env.rng, &env.config, env.time);

    env.advance(env.config.rekey_interval);

    let result = initiator_transport.tick(&env.config, env.time).unwrap();
    assert!(result.2.is_some());

    let remaining_time = env.config.max_session_duration - env.config.rekey_interval;
    let keepalive_count = remaining_time.as_secs() / env.config.keepalive_interval.as_secs();

    for _ in 0..keepalive_count {
        env.advance(env.config.keepalive_interval);
        let result = responder_transport.tick(&env.config, env.time).unwrap();
        assert!(result.1.is_some());
        let message_event = result.1.unwrap();
        let mut plaintext = vec![];
        let data_packet = DataPacket {
            header: &message_event.header,
            plaintext: &mut plaintext,
        };
        let _timer = initiator_transport
            .decrypt(&env.config, env.time, data_packet)
            .unwrap();
    }

    env.advance(Duration::from_secs(1));

    let result = initiator_transport.tick(&env.config, env.time).unwrap();
    assert!(result.2.is_none());
    assert!(result.3.is_some());
    let terminated_event = result.3.unwrap();
    assert_eq!(terminated_event.remote_addr, bob.addr);
}
