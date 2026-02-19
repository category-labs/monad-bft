use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use monad_leanudp::{
    max_payload_for_mtu,
    metrics::{
        COUNTER_LEANUDP_DECODE_EVICTED_RANDOM, COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT,
        COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY, COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR,
        GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES, GAUGE_LEANUDP_POOL_REGULAR_MESSAGES,
    },
    Clock, Config, DecodeError, DecodeOutcome, Decoder, EncodeError, Encoder, FragmentPolicy,
    IdentityScore, LEANUDP_HEADER_SIZE, MAX_CONCURRENT_MESSAGES_PER_IDENTITY,
};
use monad_peer_score::{PeerStatus, Score};
use zerocopy::IntoBytes;

#[derive(Clone)]
struct FixedClock(Rc<Cell<Instant>>);

impl FixedClock {
    fn new() -> Self {
        Self(Rc::new(Cell::new(Instant::now())))
    }

    fn advance_ms(&self, ms: u64) {
        self.0.set(self.0.get() + Duration::from_millis(ms));
    }
}

impl Clock for FixedClock {
    fn now(&self) -> Instant {
        self.0.get()
    }
}

struct AllRegular;

impl IdentityScore for AllRegular {
    type Identity = u64;

    fn score(&self, _identity: &Self::Identity) -> FragmentPolicy {
        FragmentPolicy::Regular
    }
}

#[derive(Clone)]
struct ToggleScore(Rc<Cell<bool>>);

impl IdentityScore for ToggleScore {
    type Identity = u64;

    fn score(&self, _identity: &Self::Identity) -> FragmentPolicy {
        if self.0.get() {
            FragmentPolicy::Prioritized
        } else {
            FragmentPolicy::Regular
        }
    }
}

fn packet_from_fragment(header: &monad_leanudp::PacketHeader, payload: &Bytes) -> Bytes {
    let mut buf = BytesMut::with_capacity(LEANUDP_HEADER_SIZE + payload.len());
    buf.put_slice(header.as_bytes());
    buf.put_slice(payload);
    buf.freeze()
}

fn fragment_packets(encoder: &mut Encoder, payload: Bytes) -> Vec<Bytes> {
    encoder
        .fragment(payload)
        .unwrap()
        .map(|(header, payload)| packet_from_fragment(&header, &payload))
        .collect()
}

fn build_regular(config: Config) -> (Encoder, Decoder<u64, AllRegular, FixedClock>, FixedClock) {
    let clock = FixedClock::new();
    let (encoder, decoder) = config.build_with_clock(AllRegular, clock.clone());
    (encoder, decoder, clock)
}

fn packet_with_seq(packet: &Bytes, seq: u16) -> Bytes {
    let mut raw = packet.to_vec();
    raw[5] = (seq & 0x00FF) as u8;
    raw[6] = (seq >> 8) as u8;
    Bytes::from(raw)
}

#[test]
fn test_default_config_values() {
    let config = Config::default();
    assert_eq!(config.max_message_size, 128 * 1024);
    assert_eq!(config.max_priority_messages, 8_192);
    assert_eq!(config.max_regular_messages, 4_096);
    assert_eq!(
        config.max_messages_per_identity,
        MAX_CONCURRENT_MESSAGES_PER_IDENTITY
    );
    assert_eq!(config.message_timeout, Duration::from_millis(100));
    assert_eq!(config.max_fragment_payload, 1440);
}

#[test]
#[should_panic(expected = "max_message_size must be > 0")]
fn test_invalid_config_panics() {
    let config = Config {
        max_message_size: 0,
        ..Config::default()
    };
    let _ = config.build(AllRegular);
}

#[test]
#[should_panic(expected = "max_messages_per_identity must be > 0")]
fn test_invalid_max_messages_per_identity_panics() {
    let config = Config {
        max_messages_per_identity: 0,
        ..Config::default()
    };
    let _ = config.build(AllRegular);
}

#[test]
#[should_panic(expected = "message_timeout must be > 0")]
fn test_invalid_message_timeout_panics() {
    let config = Config {
        message_timeout: Duration::from_millis(0),
        ..Config::default()
    };
    let _ = config.build(AllRegular);
}

#[test]
#[should_panic(expected = "max_fragment_payload must be > LEANUDP_HEADER_SIZE")]
fn test_invalid_max_fragment_payload_panics() {
    let config = Config {
        max_fragment_payload: LEANUDP_HEADER_SIZE,
        ..Config::default()
    };
    let _ = config.build(AllRegular);
}

#[test]
fn test_max_payload_for_mtu() {
    assert_eq!(max_payload_for_mtu(1500), 1432);
    assert_eq!(max_payload_for_mtu(1400), 1332);
    assert_eq!(max_payload_for_mtu(576), 508);
}

#[test]
fn test_peer_score_adapter_mapping() {
    struct PeerScoreMock(PeerStatus);

    impl monad_peer_score::IdentityScore for PeerScoreMock {
        type Identity = u64;

        fn score(&self, _identity: &Self::Identity) -> PeerStatus {
            self.0
        }
    }

    let promoted = PeerScoreMock(PeerStatus::Promoted(Score::try_from(1.0).unwrap()));
    assert_eq!(
        IdentityScore::score(&promoted, &7),
        FragmentPolicy::Prioritized
    );

    let unknown = PeerScoreMock(PeerStatus::Unknown);
    assert_eq!(IdentityScore::score(&unknown, &7), FragmentPolicy::Regular);
}

#[test]
fn test_roundtrip_single_fragment_message() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let packets = fragment_packets(&mut encoder, Bytes::from_static(b"hello"));
    assert_eq!(packets.len(), 1);
    assert_eq!(
        decoder.decode(42, packets[0].clone()),
        Ok(DecodeOutcome::Complete(Bytes::from_static(b"hello")))
    );
}

#[test]
fn test_encoder_oversize_payload_is_rejected() {
    let (mut encoder, _decoder, _clock) = build_regular(Config::default());
    let max_payload = encoder.max_payload_size();
    let payload = Bytes::from(vec![0u8; max_payload + 1]);
    assert!(matches!(
        encoder.fragment(payload),
        Err(EncodeError::PayloadTooLarge { .. })
    ));
}

#[test]
fn test_encoder_accessors_are_usable() {
    let (mut encoder, _decoder, _clock) = build_regular(Config::default());
    let _ = encoder.executor_metrics();
    let _ = encoder.metrics();
    let _ = encoder.max_payload_size();
    let _ = encoder
        .fragment(Bytes::from_static(b"ok"))
        .unwrap()
        .collect::<Vec<_>>();
}

#[test]
fn test_reassembles_multi_fragment_out_of_order() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let packets = fragment_packets(&mut encoder, Bytes::from(vec![b'X'; 3000]));
    assert!(packets.len() > 1);

    let last = packets.last().unwrap().clone();
    assert_eq!(decoder.decode(7, last), Ok(DecodeOutcome::Pending));

    for packet in packets.iter().take(packets.len() - 2) {
        assert_eq!(
            decoder.decode(7, packet.clone()),
            Ok(DecodeOutcome::Pending)
        );
    }

    assert_eq!(
        decoder.decode(7, packets[packets.len() - 2].clone()),
        Ok(DecodeOutcome::Complete(Bytes::from(vec![b'X'; 3000])))
    );
}

#[test]
fn test_duplicate_fragment_is_rejected() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let packets = fragment_packets(&mut encoder, Bytes::from(vec![1u8; 3000]));
    let first = packets[0].clone();

    assert_eq!(decoder.decode(1, first.clone()), Ok(DecodeOutcome::Pending));
    assert!(matches!(
        decoder.decode(1, first),
        Err(DecodeError::DuplicateFragment { .. })
    ));
}

#[test]
fn test_too_many_fragments_error_path() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let packet = fragment_packets(&mut encoder, Bytes::from_static(b"A"))[0].clone();
    let out_of_range = packet_with_seq(&packet, 128);
    assert_eq!(
        decoder.decode(1, out_of_range),
        Err(DecodeError::TooManyFragments {
            count: 129,
            max: 128,
        })
    );
}

#[test]
fn test_conflicting_end_marker_error_path() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let packets = fragment_packets(&mut encoder, Bytes::from(vec![3u8; 3000]));
    assert_eq!(
        decoder.decode(1, packets[0].clone()),
        Ok(DecodeOutcome::Pending)
    );
    assert_eq!(
        decoder.decode(1, packets[2].clone()),
        Ok(DecodeOutcome::Pending)
    );

    let conflicting_end = packet_with_seq(&packets[2], 5);
    assert_eq!(
        decoder.decode(1, conflicting_end),
        Err(DecodeError::ConflictingEndMarker {
            expected: 3,
            actual: 6,
        })
    );
}

#[test]
fn test_message_size_limit_is_enforced() {
    let config = Config {
        max_message_size: 3,
        ..Config::default()
    };
    let (mut encoder, mut decoder, _clock) = build_regular(config);
    let packets = fragment_packets(&mut encoder, Bytes::from_static(b"abcd"));

    assert_eq!(
        decoder.decode(1, packets[0].clone()),
        Err(DecodeError::MessageSizeExceeded { size: 4, max: 3 })
    );
}

#[test]
fn test_header_errors() {
    let (_encoder, mut decoder, _clock) = build_regular(Config::default());

    assert_eq!(
        decoder.decode(1, Bytes::from_static(b"short")),
        Err(DecodeError::InvalidHeaderSize {
            actual: 5,
            required: LEANUDP_HEADER_SIZE,
        })
    );

    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let mut packet = fragment_packets(&mut encoder, Bytes::from_static(b"ok"))[0].to_vec();
    packet[0] = 99;
    assert_eq!(
        decoder.decode(1, Bytes::from(packet)),
        Err(DecodeError::UnsupportedVersion {
            version: 99,
            expected: 1,
        })
    );
}

#[test]
fn test_identity_limit_and_recovery_after_completion() {
    let config = Config {
        max_messages_per_identity: 1,
        ..Config::default()
    };
    let (mut encoder, mut decoder, _clock) = build_regular(config);

    let packets_a = fragment_packets(&mut encoder, Bytes::from(vec![b'A'; 3000]));
    let packets_b = fragment_packets(&mut encoder, Bytes::from(vec![b'B'; 3000]));

    assert_eq!(
        decoder.decode(11, packets_a[0].clone()),
        Ok(DecodeOutcome::Pending)
    );
    assert_eq!(
        decoder.decode(11, packets_b[0].clone()),
        Err(DecodeError::IdentityLimitExceeded { max: 1 })
    );

    for packet in packets_a.iter().skip(1).take(packets_a.len() - 2) {
        assert_eq!(
            decoder.decode(11, packet.clone()),
            Ok(DecodeOutcome::Pending)
        );
    }
    assert_eq!(
        decoder.decode(11, packets_a.last().unwrap().clone()),
        Ok(DecodeOutcome::Complete(Bytes::from(vec![b'A'; 3000])))
    );

    assert_eq!(
        decoder.decode(11, packets_b[0].clone()),
        Ok(DecodeOutcome::Pending)
    );
}

#[test]
fn test_identity_limit_combined_across_priority_and_regular_pools() {
    let promoted = Rc::new(Cell::new(true));
    let score = ToggleScore(promoted.clone());
    let clock = FixedClock::new();
    let config = Config {
        max_messages_per_identity: 2,
        ..Config::default()
    };
    let (mut encoder, mut decoder) = config.build_with_clock(score, clock);

    let p0 = fragment_packets(&mut encoder, Bytes::from(vec![0u8; 3000]))[0].clone();
    let p1 = fragment_packets(&mut encoder, Bytes::from(vec![1u8; 3000]))[0].clone();
    let p2 = fragment_packets(&mut encoder, Bytes::from(vec![2u8; 3000]))[0].clone();

    assert_eq!(decoder.decode(1000, p0), Ok(DecodeOutcome::Pending));
    promoted.set(false);
    assert_eq!(decoder.decode(1000, p1), Ok(DecodeOutcome::Pending));
    assert_eq!(
        decoder.decode(1000, p2),
        Err(DecodeError::IdentityLimitExceeded { max: 2 })
    );

    assert_eq!(
        decoder.metrics()[COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY],
        1
    );
    assert_eq!(
        decoder.metrics()[COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR],
        2
    );
}

#[test]
fn test_identity_stale_messages_are_evicted_first() {
    let config = Config {
        max_messages_per_identity: 1,
        message_timeout: Duration::from_millis(100),
        ..Config::default()
    };
    let (mut encoder, mut decoder, clock) = build_regular(config);

    let p0 = fragment_packets(&mut encoder, Bytes::from(vec![0u8; 3000]))[0].clone();
    let p1 = fragment_packets(&mut encoder, Bytes::from(vec![1u8; 3000]))[0].clone();

    assert_eq!(decoder.decode(1, p0), Ok(DecodeOutcome::Pending));
    clock.advance_ms(200);
    assert_eq!(decoder.decode(1, p1), Ok(DecodeOutcome::Pending));

    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT], 1);
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_RANDOM], 0);
}

#[test]
fn test_pool_full_eviction_prefers_random_then_timeout() {
    let no_timeout_config = Config {
        max_regular_messages: 1,
        message_timeout: Duration::from_secs(10),
        ..Config::default()
    };
    let (mut encoder, mut decoder, _clock) = build_regular(no_timeout_config);

    let p0 = fragment_packets(&mut encoder, Bytes::from(vec![0u8; 3000]))[0].clone();
    let p1 = fragment_packets(&mut encoder, Bytes::from(vec![1u8; 3000]))[0].clone();

    assert_eq!(decoder.decode(10, p0), Ok(DecodeOutcome::Pending));
    assert_eq!(decoder.decode(20, p1), Ok(DecodeOutcome::Pending));
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_RANDOM], 1);
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT], 0);
    assert_eq!(decoder.metrics()[GAUGE_LEANUDP_POOL_REGULAR_MESSAGES], 1);

    let timeout_config = Config {
        max_regular_messages: 1,
        message_timeout: Duration::from_millis(100),
        ..Config::default()
    };
    let (mut encoder, mut decoder, clock) = build_regular(timeout_config);
    let p0 = fragment_packets(&mut encoder, Bytes::from(vec![0u8; 3000]))[0].clone();
    let p1 = fragment_packets(&mut encoder, Bytes::from(vec![1u8; 3000]))[0].clone();

    assert_eq!(decoder.decode(10, p0), Ok(DecodeOutcome::Pending));
    clock.advance_ms(200);
    assert_eq!(decoder.decode(20, p1), Ok(DecodeOutcome::Pending));
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT], 1);
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_RANDOM], 0);
}

#[test]
fn test_pool_full_when_capacity_is_zero() {
    let config = Config {
        max_regular_messages: 0,
        ..Config::default()
    };
    let (mut encoder, mut decoder, _clock) = build_regular(config);
    let packet = fragment_packets(&mut encoder, Bytes::from_static(b"X"))[0].clone();
    assert_eq!(decoder.decode(1, packet), Err(DecodeError::PoolFull));
}

#[test]
fn test_accepts_large_inflight_data_bounded_by_message_count_only() {
    let config = Config {
        max_messages_per_identity: 3,
        ..Config::default()
    };
    let (mut encoder, mut decoder, _clock) = build_regular(config);

    for seed in [11u8, 22u8, 33u8] {
        let packets = fragment_packets(&mut encoder, Bytes::from(vec![seed; 120_000]));
        for packet in packets.iter().take(packets.len() - 1) {
            assert_eq!(
                decoder.decode(555, packet.clone()),
                Ok(DecodeOutcome::Pending)
            );
        }
    }

    assert_eq!(decoder.metrics()[GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES], 0);
    assert_eq!(decoder.metrics()[GAUGE_LEANUDP_POOL_REGULAR_MESSAGES], 3);
}

#[test]
fn test_decoder_executor_metrics_accessor() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let packet = fragment_packets(&mut encoder, Bytes::from_static(b"ok"))[0].clone();
    let _ = decoder.decode(1, packet);
    let _ = decoder.executor_metrics();
    let _ = decoder.metrics();
}
