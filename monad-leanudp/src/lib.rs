mod decoder;
mod encoder;
pub mod metrics;

use std::{hash::Hash, time::Duration};

pub use decoder::{
    Clock, DecodeError, DecodeOutcome, Decoder, DecoderMetrics, Packet, SystemClock,
};
pub use encoder::{max_payload_for_mtu, EncodeError, Encoder, EncoderMetrics};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, LE, U16, U32};

pub const LEANUDP_HEADER_SIZE: usize = PacketHeader::SIZE;
pub(crate) const LEANUDP_PROTOCOL_VERSION: u8 = 1;
/// Default in-flight messages per identity.
pub const MAX_CONCURRENT_MESSAGES_PER_IDENTITY: usize = 20;

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
    const KNOWN_FLAGS_MASK: u8 = Self::START_FLAG | Self::END_FLAG;

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

    #[inline]
    pub(crate) fn has_known_flags(&self) -> bool {
        self.flags & !Self::KNOWN_FLAGS_MASK == 0
    }

    #[inline]
    pub(crate) fn has_valid_fragment_layout(&self) -> bool {
        match (self.seq_num(), self.fragment_type()) {
            (0, FragmentType::Start | FragmentType::Complete) => true,
            (0, FragmentType::Middle | FragmentType::End) => false,
            (_, FragmentType::Middle | FragmentType::End) => true,
            (_, FragmentType::Start | FragmentType::Complete) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub max_message_size: usize,
    pub max_priority_messages: usize,
    pub max_regular_messages: usize,
    pub max_messages_per_identity: usize,
    pub message_timeout: Duration,
    /// Max fragment size on wire (including LeanUDP header). Defaults to 1440.
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
            max_priority_messages: 8_192,
            max_regular_messages: 4_096,
            max_messages_per_identity: MAX_CONCURRENT_MESSAGES_PER_IDENTITY,
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
