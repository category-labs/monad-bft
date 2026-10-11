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

mod acknowledgement;
mod decoder;
mod encoder;
pub mod metrics;
mod pool;
mod retransmit;

use std::{hash::Hash, time::Duration};

pub use acknowledgement::{Acknowledgement, FragmentBitmap, ACK_BITMAP_BYTES};
pub use decoder::{Clock, DecodeError, DecodeOutcome, Decoder, Packet, SystemClock};
pub use encoder::{max_payload_for_mtu, EncodeError, Encoder, FragmentIter};
pub use retransmit::{RetryConfig, RetrySender};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, LE, U16};

pub const LEANUDP_HEADER_SIZE: usize = PacketHeader::SIZE;
pub(crate) const LEANUDP_PROTOCOL_VERSION: u8 = 1;
/// Default in-flight messages per identity.
pub const MAX_CONCURRENT_MESSAGES_PER_IDENTITY: usize = 100;
/// Default in-flight messages reserved for each dedicated identity.
pub const MAX_CONCURRENT_MESSAGES_PER_DEDICATED_IDENTITY: usize = 10;

pub const MESSAGE_LIFETIME: Duration = Duration::from_secs(2);
pub const ACK_DELAY: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FragmentType {
    Start,
    Middle,
    End,
    Complete,
}

impl From<FragmentType> for u8 {
    #[inline]
    fn from(fragment_type: FragmentType) -> Self {
        match fragment_type {
            FragmentType::Start | FragmentType::Middle => 0,
            FragmentType::Complete | FragmentType::End => PacketHeader::END_FLAG,
        }
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct PacketHeader {
    version: u8,
    msg_id: U16<LE>,
    seq_num: U16<LE>,
    // Bit 0 marks the final fragment; bit 1 marks an acknowledgement.
    flags: u8,
}

impl PacketHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
    const END_FLAG: u8 = 0x01;
    const ACK_FLAG: u8 = 0x02;
    const KNOWN_FLAGS_MASK: u8 = Self::END_FLAG | Self::ACK_FLAG;

    #[inline]
    pub(crate) fn new(msg_id: u16, seq_num: u16, fragment_type: FragmentType) -> Self {
        Self {
            version: LEANUDP_PROTOCOL_VERSION,
            msg_id: U16::new(msg_id),
            seq_num: U16::new(seq_num),
            flags: fragment_type.into(),
        }
    }

    pub fn is_ack(&self) -> bool {
        self.flags & Self::ACK_FLAG != 0
    }

    pub(crate) fn acknowledgement(msg_id: u16) -> Self {
        Self {
            version: LEANUDP_PROTOCOL_VERSION,
            msg_id: U16::new(msg_id),
            seq_num: U16::new(0),
            flags: Self::ACK_FLAG,
        }
    }

    #[inline]
    pub(crate) fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    pub fn msg_id(&self) -> u16 {
        self.msg_id.get()
    }

    #[inline]
    pub(crate) fn seq_num(&self) -> u16 {
        self.seq_num.get()
    }

    #[inline]
    pub(crate) fn fragment_type(&self) -> FragmentType {
        match (self.seq_num(), self.flags & Self::END_FLAG != 0) {
            (0, true) => FragmentType::Complete,
            (0, false) => FragmentType::Start,
            (_, true) => FragmentType::End,
            (_, false) => FragmentType::Middle,
        }
    }

    #[inline]
    pub(crate) fn has_known_flags(&self) -> bool {
        self.flags & !Self::KNOWN_FLAGS_MASK == 0
            && !(self.is_ack() && self.flags & Self::END_FLAG != 0)
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub max_message_size: usize,
    pub retry: RetryConfig,
    pub max_priority_messages: usize,
    pub max_regular_messages: usize,
    pub max_messages_per_identity: usize,
    pub max_messages_per_dedicated_identity: usize,
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
            self.max_messages_per_dedicated_identity > 0,
            "max_messages_per_dedicated_identity must be > 0"
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
            max_message_size: 512 * 1024,
            retry: RetryConfig::default(),
            // with the default config, the pool is capped at about 1.5 gib.
            max_priority_messages: 2_048,
            max_regular_messages: 1_024,
            max_messages_per_identity: MAX_CONCURRENT_MESSAGES_PER_IDENTITY,
            max_messages_per_dedicated_identity: MAX_CONCURRENT_MESSAGES_PER_DEDICATED_IDENTITY,
            message_timeout: MESSAGE_LIFETIME,
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
