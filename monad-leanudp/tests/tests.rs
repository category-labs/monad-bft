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
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use monad_leanudp::{
    metrics::{
        COUNTER_LEANUDP_DECODE_EVICTED_RANDOM, COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT,
        COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY, COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR,
        COUNTER_LEANUDP_ERROR_INVALID_HEADER, COUNTER_LEANUDP_ERROR_UNSUPPORTED_VERSION,
        GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES, GAUGE_LEANUDP_POOL_REGULAR_MESSAGES,
    },
    Clock, Config, DecodeError, DecodeOutcome, Decoder, EncodeError, Encoder, FragmentPolicy,
    IdentityScore, LEANUDP_HEADER_SIZE,
};
use zerocopy::IntoBytes;

const SEQ_NUM_OFFSET: usize = 3;
const FLAGS_OFFSET: usize = 5;

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

fn two_fragment_payload(max_fragment_payload: usize, fill: u8) -> Bytes {
    Bytes::from(vec![fill; max_fragment_payload - LEANUDP_HEADER_SIZE + 1])
}

fn packet_with_seq(packet: &Bytes, seq: u16) -> Bytes {
    let mut raw = packet.to_vec();
    raw[SEQ_NUM_OFFSET] = (seq & 0x00FF) as u8;
    raw[SEQ_NUM_OFFSET + 1] = (seq >> 8) as u8;
    Bytes::from(raw)
}

fn packet_with_flags(packet: &Bytes, flags: u8) -> Bytes {
    let mut raw = packet.to_vec();
    raw[FLAGS_OFFSET] = flags;
    Bytes::from(raw)
}

fn first_packet(encoder: &mut Encoder, payload: Bytes) -> Bytes {
    fragment_packets(encoder, payload)
        .into_iter()
        .next()
        .expect("fragmentation must produce at least one packet")
}

fn assert_build_panics(config: Config, expected: &str) {
    let panic = std::panic::catch_unwind(|| {
        let _ = config.build(AllRegular);
    })
    .expect_err("config.build should panic");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&'static str>().copied())
        .expect("panic payload should be a string");
    assert!(
        message.contains(expected),
        "panic message `{message}` did not contain `{expected}`"
    );
}

macro_rules! assert_pending {
    ($decoder:expr, $identity:expr, $packet:expr) => {
        assert_eq!(
            $decoder.decode($identity, $packet),
            Ok(DecodeOutcome::Pending)
        );
    };
}

#[test]
fn test_invalid_config_panics() {
    let cases = [
        (
            Config {
                max_message_size: 0,
                ..Config::default()
            },
            "max_message_size must be > 0",
        ),
        (
            Config {
                max_messages_per_identity: 0,
                ..Config::default()
            },
            "max_messages_per_identity must be > 0",
        ),
        (
            Config {
                message_timeout: Duration::from_millis(0),
                ..Config::default()
            },
            "message_timeout must be > 0",
        ),
        (
            Config {
                max_fragment_payload: LEANUDP_HEADER_SIZE,
                ..Config::default()
            },
            "max_fragment_payload must be > LEANUDP_HEADER_SIZE",
        ),
    ];
    for (config, expected) in cases {
        assert_build_panics(config, expected);
    }
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
fn test_default_config_roundtrip_512k_message() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let payload = Bytes::from(vec![0xAB; 512 * 1024]);
    let packets = fragment_packets(&mut encoder, payload.clone());

    assert!(
        packets.len() > 128,
        "512 KiB payload should require more than the old fragment cap"
    );

    for packet in packets.iter().take(packets.len() - 1) {
        assert_pending!(decoder, 7, packet.clone());
    }

    assert_eq!(
        decoder.decode(
            7,
            packets.last().expect("packets must be non-empty").clone()
        ),
        Ok(DecodeOutcome::Complete(payload))
    );
}

#[test]
fn test_reassembles_multi_fragment_out_of_order() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let packets = fragment_packets(&mut encoder, Bytes::from(vec![b'X'; 3000]));
    assert!(packets.len() > 1);

    let last = packets.last().unwrap().clone();
    assert_pending!(decoder, 7, last);

    for packet in packets.iter().take(packets.len() - 2) {
        assert_pending!(decoder, 7, packet.clone());
    }

    assert_eq!(
        decoder.decode(7, packets[packets.len() - 2].clone()),
        Ok(DecodeOutcome::Complete(Bytes::from(vec![b'X'; 3000])))
    );
}

#[test]
fn test_duplicate_fragment_is_rejected() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let first = first_packet(&mut encoder, Bytes::from(vec![1u8; 3000]));

    assert_pending!(decoder, 1, first.clone());
    assert!(matches!(
        decoder.decode(1, first),
        Err(DecodeError::DuplicateFragment { .. })
    ));
}

#[test]
fn test_too_many_fragments_error_path() {
    let config = Config::default();
    let (mut encoder, mut decoder, _clock) = build_regular(config.clone());
    let max_fragments =
        encoder.max_payload_size() / (config.max_fragment_payload - LEANUDP_HEADER_SIZE);
    let packet = first_packet(&mut encoder, Bytes::from_static(b"A"));
    let out_of_range = packet_with_flags(
        &packet_with_seq(
            &packet,
            u16::try_from(max_fragments).expect("max_fragments must fit in u16"),
        ),
        0,
    );
    assert_eq!(
        decoder.decode(1, out_of_range),
        Err(DecodeError::TooManyFragments {
            count: max_fragments + 1,
            max: max_fragments,
        })
    );
}

#[test]
fn test_conflicting_end_marker_error_path() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let packets = fragment_packets(&mut encoder, Bytes::from(vec![3u8; 3000]));
    assert_pending!(decoder, 1, packets[0].clone());
    assert_pending!(decoder, 1, packets[2].clone());

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
fn test_fragment_past_declared_end_is_rejected() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());
    let packets = fragment_packets(&mut encoder, Bytes::from(vec![7u8; 3000]));
    assert!(packets.len() > 2);

    assert_pending!(decoder, 1, packets.last().unwrap().clone());

    let out_of_range = packet_with_seq(&packets[1], packets.len() as u16);
    assert_eq!(
        decoder.decode(1, out_of_range),
        Err(DecodeError::ConflictingEndMarker {
            expected: packets.len() as u16,
            actual: packets.len() as u16 + 1,
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
    let packet = first_packet(&mut encoder, Bytes::from_static(b"abcd"));

    assert_eq!(
        decoder.decode(1, packet),
        Err(DecodeError::MessageSizeExceeded { size: 4, max: 3 })
    );
}

#[test]
fn test_header_errors() {
    let (mut encoder, mut decoder, _clock) = build_regular(Config::default());

    assert_eq!(
        decoder.decode(1, Bytes::from_static(b"short")),
        Err(DecodeError::InvalidHeaderSize {
            actual: 5,
            required: LEANUDP_HEADER_SIZE,
        })
    );
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_ERROR_INVALID_HEADER], 1);

    let mut packet = first_packet(&mut encoder, Bytes::from_static(b"ok")).to_vec();
    packet[0] = 99;
    assert_eq!(
        decoder.decode(1, Bytes::from(packet)),
        Err(DecodeError::UnsupportedVersion {
            version: 99,
            expected: 1,
        })
    );
    assert_eq!(
        decoder.metrics()[COUNTER_LEANUDP_ERROR_UNSUPPORTED_VERSION],
        1
    );

    assert_eq!(
        decoder.decode(
            2,
            packet_with_flags(
                &first_packet(&mut encoder, Bytes::from_static(b"flags")),
                0x80
            ),
        ),
        Err(DecodeError::InvalidHeader)
    );
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_ERROR_INVALID_HEADER], 2);

    let packet = first_packet(&mut encoder, Bytes::from_static(b"layout"));
    let inferred_start = packet_with_flags(&packet, 0);
    assert_eq!(
        decoder.decode(3, inferred_start),
        Ok(DecodeOutcome::Pending)
    );
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_ERROR_INVALID_HEADER], 2);
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

    assert_pending!(decoder, 11, packets_a[0].clone());
    assert_eq!(
        decoder.decode(11, packets_b[0].clone()),
        Err(DecodeError::IdentityLimitExceeded { max: 1 })
    );

    for packet in packets_a.iter().skip(1).take(packets_a.len() - 2) {
        assert_pending!(decoder, 11, packet.clone());
    }
    assert_eq!(
        decoder.decode(11, packets_a.last().unwrap().clone()),
        Ok(DecodeOutcome::Complete(Bytes::from(vec![b'A'; 3000])))
    );

    assert_pending!(decoder, 11, packets_b[0].clone());
}

#[test]
fn test_identity_limit_is_independent_per_pool() {
    let promoted = Rc::new(Cell::new(true));
    let score = ToggleScore(promoted.clone());
    let clock = FixedClock::new();
    let config = Config {
        max_messages_per_identity: 2,
        ..Config::default()
    };
    let (mut encoder, mut decoder) = config.build_with_clock(score, clock);

    let p0 = first_packet(&mut encoder, Bytes::from(vec![0u8; 3000]));
    let p1 = first_packet(&mut encoder, Bytes::from(vec![1u8; 3000]));
    let p2 = first_packet(&mut encoder, Bytes::from(vec![2u8; 3000]));

    assert_pending!(decoder, 1000, p0);
    promoted.set(false);
    assert_pending!(decoder, 1000, p1);
    assert_pending!(decoder, 1000, p2);

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
fn test_regular_dynamic_cap_applies_under_pressure() {
    let config = Config {
        max_messages_per_identity: 200,
        max_regular_messages: 1_000,
        ..Config::default()
    };
    let (mut encoder, mut decoder, _clock) = build_regular(config);

    for id in 1..=800u64 {
        let first = first_packet(&mut encoder, Bytes::from(vec![id as u8; 3_000]));
        assert_pending!(decoder, id, first);
    }

    for i in 0..10u64 {
        let first = first_packet(&mut encoder, Bytes::from(vec![i as u8; 3_000]));
        assert_pending!(decoder, 9_999, first);
    }

    let over = first_packet(&mut encoder, Bytes::from(vec![0u8; 3_000]));
    assert_eq!(
        decoder.decode(9_999, over),
        Err(DecodeError::IdentityLimitExceeded { max: 10 })
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

    let p0 = first_packet(&mut encoder, Bytes::from(vec![0u8; 3000]));
    let p1 = first_packet(&mut encoder, Bytes::from(vec![1u8; 3000]));

    assert_pending!(decoder, 1, p0);
    clock.advance_ms(200);
    assert_pending!(decoder, 1, p1);

    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT], 1);
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_RANDOM], 0);
}

#[test]
fn test_late_fragment_after_timeout_does_not_complete_message() {
    let config = Config {
        message_timeout: Duration::from_millis(100),
        ..Config::default()
    };
    let payload = two_fragment_payload(config.max_fragment_payload, b'T');
    let (mut encoder, mut decoder, clock) = build_regular(config);
    let packets = fragment_packets(&mut encoder, payload);
    assert_eq!(packets.len(), 2);

    assert_pending!(decoder, 9, packets[0].clone());
    clock.advance_ms(5_000);

    assert_eq!(
        decoder.decode(9, packets[1].clone()),
        Ok(DecodeOutcome::Pending)
    );
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT], 1);
}

#[test]
fn test_same_key_retry_evicts_timed_out_message_immediately() {
    let config = Config {
        message_timeout: Duration::from_millis(100),
        ..Config::default()
    };
    let payload = two_fragment_payload(config.max_fragment_payload, b'R');
    let (mut encoder, mut decoder, clock) = build_regular(config);
    let first = first_packet(&mut encoder, payload);

    assert_pending!(decoder, 7, first.clone());
    clock.advance_ms(5_000);

    assert_eq!(decoder.decode(7, first), Ok(DecodeOutcome::Pending));
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT], 1);
}

#[test]
fn test_pool_full_eviction_prefers_random_then_timeout() {
    let no_timeout_config = Config {
        max_regular_messages: 1,
        message_timeout: Duration::from_secs(10),
        ..Config::default()
    };
    let (mut encoder, mut decoder, _clock) = build_regular(no_timeout_config);

    let p0 = first_packet(&mut encoder, Bytes::from(vec![0u8; 3000]));
    let p1 = first_packet(&mut encoder, Bytes::from(vec![1u8; 3000]));

    assert_pending!(decoder, 10, p0);
    assert_pending!(decoder, 20, p1);
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_RANDOM], 1);
    assert_eq!(decoder.metrics()[COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT], 0);
    assert_eq!(decoder.metrics()[GAUGE_LEANUDP_POOL_REGULAR_MESSAGES], 1);

    let timeout_config = Config {
        max_regular_messages: 1,
        message_timeout: Duration::from_millis(100),
        ..Config::default()
    };
    let (mut encoder, mut decoder, clock) = build_regular(timeout_config);
    let p0 = first_packet(&mut encoder, Bytes::from(vec![0u8; 3000]));
    let p1 = first_packet(&mut encoder, Bytes::from(vec![1u8; 3000]));

    assert_pending!(decoder, 10, p0);
    clock.advance_ms(200);
    assert_pending!(decoder, 20, p1);
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
    let packet = first_packet(&mut encoder, Bytes::from_static(b"X"));
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
            assert_pending!(decoder, 555, packet.clone());
        }
    }

    assert_eq!(decoder.metrics()[GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES], 0);
    assert_eq!(decoder.metrics()[GAUGE_LEANUDP_POOL_REGULAR_MESSAGES], 3);
}
