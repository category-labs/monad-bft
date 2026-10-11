// Copyright (C) 2025 Category Labs, Inc.
// SPDX-License-Identifier: GPL-3.0-or-later

use bytes::{BufMut, Bytes, BytesMut};
use zerocopy::IntoBytes;

use crate::{encoder::MAX_FRAGMENTS, DecodeError, Packet, PacketHeader, LEANUDP_PROTOCOL_VERSION};

pub const ACK_BITMAP_BYTES: usize = 64;
const _: () = assert!(ACK_BITMAP_BYTES * 8 == MAX_FRAGMENTS);

/// A snapshot of fragments currently retained by the receiver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FragmentBitmap([u8; ACK_BITMAP_BYTES]);

impl Default for FragmentBitmap {
    fn default() -> Self {
        Self([0; ACK_BITMAP_BYTES])
    }
}

impl FragmentBitmap {
    pub fn insert(&mut self, seq: u16) {
        let seq = usize::from(seq);
        assert!(seq < MAX_FRAGMENTS);
        self.0[seq / 8] |= 1 << (seq % 8);
    }

    pub fn contains(&self, seq: u16) -> bool {
        let seq = usize::from(seq);
        seq < MAX_FRAGMENTS && self.0[seq / 8] & (1 << (seq % 8)) != 0
    }

    pub fn is_complete(&self, fragments: usize) -> bool {
        fragments <= MAX_FRAGMENTS && (0..fragments).all(|seq| self.contains(seq as u16))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Acknowledgement {
    pub msg_id: u16,
    pub received: FragmentBitmap,
}

impl Acknowledgement {
    pub fn encode(&self) -> Bytes {
        let mut packet = BytesMut::with_capacity(PacketHeader::SIZE + ACK_BITMAP_BYTES);
        packet.put_slice(PacketHeader::acknowledgement(self.msg_id).as_bytes());
        packet.put_slice(&self.received.0);
        packet.freeze()
    }

    /// Validates the header and returns `None` for a data packet.
    pub fn from_packet(packet: &Packet) -> Result<Option<Self>, DecodeError> {
        if packet.header.version() != LEANUDP_PROTOCOL_VERSION {
            return Err(DecodeError::UnsupportedVersion {
                version: packet.header.version(),
                expected: LEANUDP_PROTOCOL_VERSION,
            });
        }
        if !packet.header.has_known_flags() {
            return Err(DecodeError::InvalidHeader);
        }
        if !packet.header.is_ack() {
            return Ok(None);
        }
        let received = packet
            .payload
            .as_ref()
            .try_into()
            .map_err(|_| DecodeError::InvalidHeader)?;
        Ok(Some(Self {
            msg_id: packet.header.msg_id(),
            received: FragmentBitmap(received),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acknowledgement_roundtrip_and_validation() {
        let mut received = FragmentBitmap::default();
        for seq in [0, 7, 8, 511] {
            received.insert(seq);
        }
        let ack = Acknowledgement {
            msg_id: u16::MAX,
            received,
        };
        let bytes = ack.encode();
        assert_eq!(bytes.len(), 70);
        assert_eq!(&bytes[..6], &[1, 255, 255, 0, 0, 2]);
        assert_eq!(
            Acknowledgement::from_packet(&Packet::try_from(bytes.clone()).unwrap()),
            Ok(Some(ack))
        );
        assert!(received.contains(511));
        assert!(!received.contains(510));
        assert!(received.is_complete(1));
        assert!(!received.is_complete(2));
        for len in [6, 69, 71] {
            let mut malformed = bytes.to_vec();
            malformed.resize(len, 0);
            assert_eq!(
                Acknowledgement::from_packet(&Packet::try_from(Bytes::from(malformed)).unwrap()),
                Err(DecodeError::InvalidHeader)
            );
        }
        for flags in [3, 0x82] {
            let mut malformed = bytes.to_vec();
            malformed[5] = flags;
            assert_eq!(
                Acknowledgement::from_packet(&Packet::try_from(Bytes::from(malformed)).unwrap()),
                Err(DecodeError::InvalidHeader)
            );
        }
    }
}
