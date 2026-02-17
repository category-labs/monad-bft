mod decoder;
mod encoder;
pub mod metrics;

use std::{hash::Hash, time::Duration};

pub use decoder::{
    Clock, DecodeError, DecodeOutcome, Decoder, DecoderMetrics, Packet, SystemClock,
};
pub use encoder::{EncodeError, Encoder, EncoderMetrics, max_payload_for_mtu};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, LE, U16, U32};

pub const LEANUDP_HEADER_SIZE: usize = PacketHeader::SIZE;
pub(crate) const LEANUDP_PROTOCOL_VERSION: u8 = 1;
/// Hard upper bound enforced by the decoder for in-flight messages per identity, per pool.
pub const MAX_CONCURRENT_MESSAGES_PER_IDENTITY: usize = 10;
/// Default in-flight bytes per identity, per pool.
pub const MAX_CONCURRENT_BYTES_PER_IDENTITY: usize = 256 * 1024;

const DEFAULT_MESSAGE_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FragmentType {
    Start,
    Middle,
    End,
    Complete,
}

impl From<u8> for FragmentType {
    #[inline]
    fn from(flags: u8) -> Self {
        let is_start = flags & PacketHeader::START_FLAG != 0;
        let is_end = flags & PacketHeader::END_FLAG != 0;
        match (is_start, is_end) {
            (true, true) => Self::Complete,
            (true, false) => Self::Start,
            (false, true) => Self::End,
            (false, false) => Self::Middle,
        }
    }
}

impl From<FragmentType> for u8 {
    #[inline]
    fn from(fragment_type: FragmentType) -> Self {
        match fragment_type {
            FragmentType::Start => PacketHeader::START_FLAG,
            FragmentType::Middle => 0,
            FragmentType::End => PacketHeader::END_FLAG,
            FragmentType::Complete => PacketHeader::START_FLAG | PacketHeader::END_FLAG,
        }
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct PacketHeader {
    version: u8,
    msg_id: U32<LE>,
    seq_num: U16<LE>,
    flags: u8,
}

impl PacketHeader {
    pub const SIZE: usize = 8;
    const START_FLAG: u8 = 0x01;
    const END_FLAG: u8 = 0x02;

    #[inline]
    pub(crate) fn new(msg_id: u32, seq_num: u16, fragment_type: FragmentType) -> Self {
        Self {
            version: LEANUDP_PROTOCOL_VERSION,
            msg_id: U32::new(msg_id),
            seq_num: U16::new(seq_num),
            flags: fragment_type.into(),
        }
    }

    #[inline]
    pub(crate) fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    pub(crate) fn msg_id(&self) -> u32 {
        self.msg_id.get()
    }

    #[inline]
    pub(crate) fn seq_num(&self) -> u16 {
        self.seq_num.get()
    }

    #[inline]
    pub(crate) fn fragment_type(&self) -> FragmentType {
        self.flags.into()
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub max_message_size: usize,
    pub max_priority_messages: usize,
    pub max_regular_messages: usize,
    /// Effective limit is clamped to `MAX_CONCURRENT_MESSAGES_PER_IDENTITY`.
    pub max_messages_per_identity: usize,
    pub max_bytes_per_identity: usize,
    pub message_timeout: Duration,
    /// Max fragment size on wire (excluding leanudp header). Defaults to 1432.
    pub max_fragment_payload: usize,
}

impl Config {
    fn validate(&self) {
        assert!(self.max_message_size > 0, "max_message_size must be > 0");
        assert!(
            self.max_messages_per_identity > 0,
            "max_messages_per_identity must be > 0"
        );
        assert!(
            self.max_bytes_per_identity > 0,
            "max_bytes_per_identity must be > 0"
        );
        assert!(
            !self.message_timeout.is_zero(),
            "message_timeout must be > 0"
        );
        assert!(
            self.max_fragment_payload > LEANUDP_HEADER_SIZE,
            "max_fragment_payload must be > LEANUDP_HEADER_SIZE"
        );
    }

    pub fn build<I, P>(self, identity_score: P) -> (Encoder, Decoder<I, P, SystemClock>)
    where
        I: Eq + Hash + Clone + Ord,
        P: IdentityScore<Identity = I>,
    {
        self.build_with_clock(identity_score, SystemClock)
    }

    pub fn build_with_clock<I, P, C>(
        self,
        identity_score: P,
        clock: C,
    ) -> (Encoder, Decoder<I, P, C>)
    where
        I: Eq + Hash + Clone + Ord,
        P: IdentityScore<Identity = I>,
        C: Clock,
    {
        self.validate();
        let encoder = Encoder::new(self.max_fragment_payload);
        let decoder = Decoder::with_clock(&self, identity_score, clock);
        (encoder, decoder)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_message_size: 128 * 1024,
            max_priority_messages: 10_000,
            max_regular_messages: 1_000,
            max_messages_per_identity: MAX_CONCURRENT_MESSAGES_PER_IDENTITY,
            max_bytes_per_identity: MAX_CONCURRENT_BYTES_PER_IDENTITY,
            message_timeout: DEFAULT_MESSAGE_TIMEOUT,
            max_fragment_payload: 1440, // MTU(1500) - IP(20) - UDP(8) - WireAuth(32)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FragmentPolicy {
    #[default]
    Regular,
    Prioritized,
}

pub trait IdentityScore {
    type Identity;

    fn score(&self, identity: &Self::Identity) -> FragmentPolicy;
}

impl<T> IdentityScore for T
where
    T: monad_peer_score::IdentityScore,
{
    type Identity = T::Identity;

    fn score(&self, identity: &Self::Identity) -> FragmentPolicy {
        match monad_peer_score::IdentityScore::score(self, identity) {
            monad_peer_score::PeerStatus::Promoted(_) => FragmentPolicy::Prioritized,
            _ => FragmentPolicy::Regular,
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_peer_score::{PeerStatus, Score};
    use zerocopy::FromBytes;

    use super::*;

    struct DummyScore;

    impl IdentityScore for DummyScore {
        type Identity = u64;

        fn score(&self, _identity: &Self::Identity) -> FragmentPolicy {
            FragmentPolicy::Regular
        }
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.max_message_size, 128 * 1024);
        assert_eq!(config.max_priority_messages, 10_000);
        assert_eq!(config.max_regular_messages, 1_000);
        assert_eq!(
            config.max_messages_per_identity,
            MAX_CONCURRENT_MESSAGES_PER_IDENTITY
        );
        assert_eq!(
            config.max_bytes_per_identity,
            MAX_CONCURRENT_BYTES_PER_IDENTITY
        );
        assert_eq!(config.message_timeout, Duration::from_millis(100));
    }

    #[test]
    fn test_dummy_score() {
        let score = DummyScore;
        assert_eq!(score.score(&0), FragmentPolicy::Regular);
    }

    struct PeerScoreMock(PeerStatus);

    impl monad_peer_score::IdentityScore for PeerScoreMock {
        type Identity = u64;

        fn score(&self, _identity: &Self::Identity) -> PeerStatus {
            self.0
        }
    }

    #[test]
    fn test_peer_score_mapping() {
        let promoted = PeerScoreMock(PeerStatus::Promoted(Score::try_from(1.0).unwrap()));
        assert_eq!(
            IdentityScore::score(&promoted, &0),
            FragmentPolicy::Prioritized
        );

        let unknown = PeerScoreMock(PeerStatus::Unknown);
        assert_eq!(IdentityScore::score(&unknown, &0), FragmentPolicy::Regular);
    }

    #[test]
    fn test_max_payload_for_mtu() {
        assert_eq!(max_payload_for_mtu(1500), 1432);
        assert_eq!(max_payload_for_mtu(1400), 1332);
        assert_eq!(max_payload_for_mtu(576), 508);
    }

    #[test]
    #[should_panic(expected = "max_message_size must be > 0")]
    fn test_invalid_max_message_size() {
        let config = Config {
            max_message_size: 0,
            ..Config::default()
        };
        let _ = config.build(DummyScore);
    }

    #[test]
    #[should_panic(expected = "max_messages_per_identity must be > 0")]
    fn test_invalid_max_messages_per_identity() {
        let config = Config {
            max_messages_per_identity: 0,
            ..Config::default()
        };
        let _ = config.build(DummyScore);
    }

    #[test]
    #[should_panic(expected = "message_timeout must be > 0")]
    fn test_invalid_message_timeout() {
        let config = Config {
            message_timeout: Duration::from_millis(0),
            ..Config::default()
        };
        let _ = config.build(DummyScore);
    }

    #[test]
    #[should_panic(expected = "max_bytes_per_identity must be > 0")]
    fn test_invalid_max_bytes_per_identity() {
        let config = Config {
            max_bytes_per_identity: 0,
            ..Config::default()
        };
        let _ = config.build(DummyScore);
    }

    #[test]
    #[should_panic(expected = "max_fragment_payload must be > LEANUDP_HEADER_SIZE")]
    fn test_invalid_max_fragment_payload() {
        let config = Config {
            max_fragment_payload: LEANUDP_HEADER_SIZE,
            ..Config::default()
        };
        let _ = config.build(DummyScore);
    }

    #[test]
    fn test_policy_default() {
        assert_eq!(FragmentPolicy::default(), FragmentPolicy::Regular);
    }

    #[test]
    fn test_header_size() {
        assert_eq!(LEANUDP_HEADER_SIZE, 8);
        assert_eq!(std::mem::size_of::<PacketHeader>(), PacketHeader::SIZE);
    }

    #[test]
    fn test_header_fragment_types() {
        let start = PacketHeader::new(1, 0, FragmentType::Start);
        assert_eq!(start.fragment_type(), FragmentType::Start);
        assert_eq!(u8::from(start.fragment_type()), PacketHeader::START_FLAG);
        assert_eq!(
            FragmentType::from(PacketHeader::START_FLAG),
            FragmentType::Start
        );
        assert_eq!(start.version(), LEANUDP_PROTOCOL_VERSION);

        let end = PacketHeader::new(1, 5, FragmentType::End);
        assert_eq!(end.fragment_type(), FragmentType::End);
        assert_eq!(u8::from(end.fragment_type()), PacketHeader::END_FLAG);
        assert_eq!(
            FragmentType::from(PacketHeader::END_FLAG),
            FragmentType::End
        );

        let complete = PacketHeader::new(1, 0, FragmentType::Complete);
        assert_eq!(complete.fragment_type(), FragmentType::Complete);
        assert_eq!(
            u8::from(complete.fragment_type()),
            PacketHeader::START_FLAG | PacketHeader::END_FLAG
        );
        assert_eq!(
            FragmentType::from(PacketHeader::START_FLAG | PacketHeader::END_FLAG),
            FragmentType::Complete
        );

        let middle = PacketHeader::new(1, 2, FragmentType::Middle);
        assert_eq!(middle.fragment_type(), FragmentType::Middle);
        assert_eq!(u8::from(middle.fragment_type()), 0);
        assert_eq!(FragmentType::from(0), FragmentType::Middle);
    }

    #[test]
    fn test_header_roundtrip() {
        let original = PacketHeader::new(0x00ABCDEF, 65535, FragmentType::Complete);
        let bytes = original.as_bytes();
        let parsed = PacketHeader::read_from_bytes(bytes).unwrap();
        assert_eq!(parsed.version(), LEANUDP_PROTOCOL_VERSION);
        assert_eq!(parsed.msg_id(), 0x00ABCDEF);
        assert_eq!(parsed.seq_num(), 65535);
        assert_eq!(parsed.fragment_type(), FragmentType::Complete);
    }

    #[test]
    fn test_msg_id_field() {
        let header = PacketHeader::new(0x123456, 42, FragmentType::Middle);
        assert_eq!(header.msg_id(), 0x123456);
        assert_eq!(header.seq_num(), 42);
    }

    #[test]
    fn test_msg_id_full_width() {
        let header = PacketHeader::new(0x01234567, 0, FragmentType::Complete);
        assert_eq!(header.msg_id(), 0x01234567);
    }
}
