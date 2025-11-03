use std::convert::TryFrom;

use bytes::Bytes;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, LE, U32, U64};

use crate::{common::*, errors::MessageError};

/// Trait for messages that have MAC1 and MAC2 fields
pub trait MacMessage: IntoBytes {
    fn mac1(&self) -> &MacTag;
    fn mac2(&self) -> &MacTag;
    fn mac1_input(&self) -> &[u8];
    fn mac2_input(&self) -> &[u8];
}

pub const TYPE_HANDSHAKE_INITIATION: u8 = 1;
pub const TYPE_HANDSHAKE_RESPONSE: u8 = 2;
pub const TYPE_COOKIE_REPLY: u8 = 3;
pub const TYPE_DATA: u8 = 4;

pub const TIMESTAMP_SIZE: usize = 12;

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone)]
pub struct HandshakeInitiation {
    pub message_type: u8,
    pub reserved: [u8; 3],
    pub sender_index: U32<LE>,
    pub ephemeral_public: [u8; monad_secp::COMPRESSED_PUBLIC_KEY_SIZE],
    pub encrypted_static: [u8; monad_secp::COMPRESSED_PUBLIC_KEY_SIZE],
    pub encrypted_static_tag: [u8; CIPHER_TAG_SIZE],
    pub encrypted_timestamp: [u8; TIMESTAMP_SIZE],
    pub encrypted_timestamp_tag: [u8; CIPHER_TAG_SIZE],
    pub mac1: MacTag,
    pub mac2: MacTag,
}

impl Default for HandshakeInitiation {
    fn default() -> Self {
        unsafe {
            let mut msg: Self = core::mem::zeroed();
            msg.message_type = TYPE_HANDSHAKE_INITIATION;
            msg
        }
    }
}

impl MacMessage for HandshakeInitiation {
    fn mac1(&self) -> &MacTag {
        &self.mac1
    }

    fn mac2(&self) -> &MacTag {
        &self.mac2
    }

    fn mac1_input(&self) -> &[u8] {
        self.as_bytes()[..Self::MAC1_OFFSET].as_ref()
    }

    fn mac2_input(&self) -> &[u8] {
        self.as_bytes()[..Self::MAC2_OFFSET].as_ref()
    }
}

impl HandshakeInitiation {
    pub const SIZE: usize = 4
        + 4
        + PUBLIC_KEY_SIZE
        + PUBLIC_KEY_SIZE
        + CIPHER_TAG_SIZE
        + TIMESTAMP_SIZE
        + CIPHER_TAG_SIZE
        + MAC_TAG_SIZE
        + MAC_TAG_SIZE;

    pub const MAC1_OFFSET: usize = 4
        + 4
        + PUBLIC_KEY_SIZE
        + PUBLIC_KEY_SIZE
        + CIPHER_TAG_SIZE
        + TIMESTAMP_SIZE
        + CIPHER_TAG_SIZE;

    pub const MAC2_OFFSET: usize = Self::MAC1_OFFSET + MAC_TAG_SIZE;
}

impl<'a> TryFrom<&'a [u8]> for &'a HandshakeInitiation {
    type Error = MessageError;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != HandshakeInitiation::SIZE {
            return Err(MessageError::BufferTooSmall {
                required: HandshakeInitiation::SIZE,
                actual: bytes.len(),
            });
        }
        HandshakeInitiation::ref_from_bytes(bytes).map_err(|_| MessageError::InvalidHeader)
    }
}

impl<'a> TryFrom<&'a mut [u8]> for &'a mut HandshakeInitiation {
    type Error = MessageError;

    fn try_from(bytes: &'a mut [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != HandshakeInitiation::SIZE {
            return Err(MessageError::BufferTooSmall {
                required: HandshakeInitiation::SIZE,
                actual: bytes.len(),
            });
        }
        HandshakeInitiation::mut_from_bytes(bytes).map_err(|_| MessageError::InvalidHeader)
    }
}

impl From<HandshakeInitiation> for Bytes {
    fn from(msg: HandshakeInitiation) -> Self {
        Bytes::copy_from_slice(msg.as_bytes())
    }
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone)]
pub struct HandshakeResponse {
    pub message_type: u8,
    pub reserved: [u8; 3],
    pub sender_index: U32<LE>,
    pub receiver_index: U32<LE>,
    pub ephemeral_public: [u8; monad_secp::COMPRESSED_PUBLIC_KEY_SIZE],
    pub encrypted_nothing_tag: [u8; CIPHER_TAG_SIZE],
    pub mac1: MacTag,
    pub mac2: MacTag,
}

impl Default for HandshakeResponse {
    fn default() -> Self {
        unsafe {
            let mut msg: Self = core::mem::zeroed();
            msg.message_type = TYPE_HANDSHAKE_RESPONSE;
            msg
        }
    }
}

impl MacMessage for HandshakeResponse {
    fn mac1(&self) -> &MacTag {
        &self.mac1
    }

    fn mac2(&self) -> &MacTag {
        &self.mac2
    }

    fn mac1_input(&self) -> &[u8] {
        self.as_bytes()[..Self::MAC1_OFFSET].as_ref()
    }

    fn mac2_input(&self) -> &[u8] {
        self.as_bytes()[..Self::MAC2_OFFSET].as_ref()
    }
}

impl HandshakeResponse {
    pub const SIZE: usize =
        4 + 4 + 4 + PUBLIC_KEY_SIZE + CIPHER_TAG_SIZE + MAC_TAG_SIZE + MAC_TAG_SIZE;

    pub const MAC1_OFFSET: usize = 4 + 4 + 4 + PUBLIC_KEY_SIZE + CIPHER_TAG_SIZE;

    pub const MAC2_OFFSET: usize = Self::MAC1_OFFSET + MAC_TAG_SIZE;
}

impl<'a> TryFrom<&'a [u8]> for &'a HandshakeResponse {
    type Error = MessageError;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != HandshakeResponse::SIZE {
            return Err(MessageError::BufferTooSmall {
                required: HandshakeResponse::SIZE,
                actual: bytes.len(),
            });
        }
        HandshakeResponse::ref_from_bytes(bytes).map_err(|_| MessageError::InvalidHeader)
    }
}

impl<'a> TryFrom<&'a mut [u8]> for &'a mut HandshakeResponse {
    type Error = MessageError;

    fn try_from(bytes: &'a mut [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != HandshakeResponse::SIZE {
            return Err(MessageError::BufferTooSmall {
                required: HandshakeResponse::SIZE,
                actual: bytes.len(),
            });
        }
        HandshakeResponse::mut_from_bytes(bytes).map_err(|_| MessageError::InvalidHeader)
    }
}

impl From<HandshakeResponse> for Bytes {
    fn from(msg: HandshakeResponse) -> Self {
        Bytes::copy_from_slice(msg.as_bytes())
    }
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone)]
pub struct CookieReply {
    pub message_type: u8,
    pub reserved: [u8; 3],
    pub receiver_index: U32<LE>,
    pub nonce: CipherNonce,
    pub encrypted_cookie: [u8; 16],
    pub encrypted_cookie_tag: [u8; CIPHER_TAG_SIZE],
}

impl Default for CookieReply {
    fn default() -> Self {
        unsafe {
            let mut msg: Self = core::mem::zeroed();
            msg.message_type = TYPE_COOKIE_REPLY;
            msg
        }
    }
}

impl CookieReply {
    pub const SIZE: usize = 4 + 4 + 16 + 16 + CIPHER_TAG_SIZE;
}

impl<'a> TryFrom<&'a [u8]> for &'a CookieReply {
    type Error = MessageError;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != CookieReply::SIZE {
            return Err(MessageError::BufferTooSmall {
                required: CookieReply::SIZE,
                actual: bytes.len(),
            });
        }
        CookieReply::ref_from_bytes(bytes).map_err(|_| MessageError::InvalidHeader)
    }
}

impl<'a> TryFrom<&'a mut [u8]> for &'a mut CookieReply {
    type Error = MessageError;

    fn try_from(bytes: &'a mut [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != CookieReply::SIZE {
            return Err(MessageError::BufferTooSmall {
                required: CookieReply::SIZE,
                actual: bytes.len(),
            });
        }
        CookieReply::mut_from_bytes(bytes).map_err(|_| MessageError::InvalidHeader)
    }
}

impl From<CookieReply> for Bytes {
    fn from(msg: CookieReply) -> Self {
        Bytes::copy_from_slice(msg.as_bytes())
    }
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone)]
pub struct DataPacketHeader {
    pub message_type: u8,
    pub reserved: [u8; 3],
    pub receiver_index: U32<LE>,
    pub counter: U64<LE>,
    pub tag: [u8; CIPHER_TAG_SIZE],
}

impl Default for DataPacketHeader {
    fn default() -> Self {
        unsafe {
            let mut msg: Self = core::mem::zeroed();
            msg.message_type = TYPE_DATA;
            msg
        }
    }
}

impl DataPacketHeader {
    pub const SIZE: usize = 4 + 4 + 8 + CIPHER_TAG_SIZE;
}

impl<'a> TryFrom<&'a [u8]> for &'a DataPacketHeader {
    type Error = MessageError;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < DataPacketHeader::SIZE {
            return Err(MessageError::BufferTooSmall {
                required: DataPacketHeader::SIZE,
                actual: bytes.len(),
            });
        }
        DataPacketHeader::ref_from_bytes(&bytes[..DataPacketHeader::SIZE])
            .map_err(|_| MessageError::InvalidDataPacketHeader)
    }
}

impl<'a> TryFrom<&'a mut [u8]> for &'a mut DataPacketHeader {
    type Error = MessageError;

    fn try_from(bytes: &'a mut [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < DataPacketHeader::SIZE {
            return Err(MessageError::BufferTooSmall {
                required: DataPacketHeader::SIZE,
                actual: bytes.len(),
            });
        }
        let (header_bytes, _) = bytes.split_at_mut(DataPacketHeader::SIZE);
        DataPacketHeader::mut_from_bytes(header_bytes)
            .map_err(|_| MessageError::InvalidDataPacketHeader)
    }
}

impl From<DataPacketHeader> for Bytes {
    fn from(header: DataPacketHeader) -> Self {
        Bytes::copy_from_slice(header.as_bytes())
    }
}

pub struct DataPacket<'a> {
    header: &'a DataPacketHeader,
    plaintext: &'a mut [u8],
}

impl<'a> DataPacket<'a> {
    pub const HEADER_SIZE: usize = DataPacketHeader::SIZE;

    pub fn new(header: &'a DataPacketHeader, plaintext: &'a mut [u8]) -> Self {
        Self { header, plaintext }
    }

    pub fn header(&self) -> &DataPacketHeader {
        self.header
    }

    pub fn data(&self) -> &[u8] {
        self.plaintext
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        self.plaintext
    }
}

pub struct Plaintext<'a>(DataPacket<'a>);

impl<'a> Plaintext<'a> {
    pub const HEADER_SIZE: usize = DataPacket::HEADER_SIZE;

    pub fn new(data_packet: DataPacket<'a>) -> Self {
        Self(data_packet)
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.data()
    }
}

impl<'a> AsRef<[u8]> for Plaintext<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> TryFrom<&'a mut [u8]> for DataPacket<'a> {
    type Error = MessageError;

    fn try_from(bytes: &'a mut [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < DataPacketHeader::SIZE {
            return Err(MessageError::BufferTooSmall {
                required: DataPacketHeader::SIZE,
                actual: bytes.len(),
            });
        }

        let (header_bytes, plaintext) = bytes.split_at_mut(DataPacketHeader::SIZE);
        let header = DataPacketHeader::ref_from_bytes(header_bytes)
            .map_err(|_| MessageError::InvalidDataPacketHeader)?;

        Ok(DataPacket { header, plaintext })
    }
}

pub enum ControlPacket<'a> {
    HandshakeInitiation(&'a mut HandshakeInitiation),
    HandshakeResponse(&'a mut HandshakeResponse),
    CookieReply(&'a mut CookieReply),
    Keepalive(DataPacket<'a>),
}

pub enum Packet<'a> {
    Control(ControlPacket<'a>),
    Data(DataPacket<'a>),
}

impl<'a> TryFrom<&'a mut [u8]> for Packet<'a> {
    type Error = MessageError;

    fn try_from(bytes: &'a mut [u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            return Err(MessageError::BufferTooSmall {
                required: 1,
                actual: 0,
            });
        }

        match bytes[0] {
            TYPE_HANDSHAKE_INITIATION => {
                let msg = <&mut HandshakeInitiation>::try_from(bytes)?;
                Ok(Packet::Control(ControlPacket::HandshakeInitiation(msg)))
            }
            TYPE_HANDSHAKE_RESPONSE => {
                let msg = <&mut HandshakeResponse>::try_from(bytes)?;
                Ok(Packet::Control(ControlPacket::HandshakeResponse(msg)))
            }
            TYPE_COOKIE_REPLY => {
                let msg = <&mut CookieReply>::try_from(bytes)?;
                Ok(Packet::Control(ControlPacket::CookieReply(msg)))
            }
            TYPE_DATA => {
                let data_packet = DataPacket::try_from(bytes)?;
                if data_packet.data().is_empty() {
                    Ok(Packet::Control(ControlPacket::Keepalive(data_packet)))
                } else {
                    Ok(Packet::Data(data_packet))
                }
            }
            _ => Err(MessageError::InvalidHeader),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use super::*;
    use crate::errors::MessageError;

    #[test]
    fn test_handshake_initiation_default() {
        let msg = HandshakeInitiation::default();
        assert_eq!(msg.message_type, TYPE_HANDSHAKE_INITIATION);
    }

    #[test]
    fn test_handshake_initiation_mac1_input() {
        let msg = HandshakeInitiation::default();
        let mac1_input = msg.mac1_input();
        assert_eq!(mac1_input.len(), HandshakeInitiation::MAC1_OFFSET);
    }

    #[test]
    fn test_handshake_initiation_mac2_input() {
        let msg = HandshakeInitiation::default();
        let mac2_input = msg.mac2_input();
        assert_eq!(mac2_input.len(), HandshakeInitiation::MAC2_OFFSET);
    }

    #[test]
    fn test_handshake_initiation_from_bytes() {
        let mut bytes = [0u8; HandshakeInitiation::SIZE];
        bytes[0] = TYPE_HANDSHAKE_INITIATION;

        let msg = <&HandshakeInitiation>::try_from(&bytes[..]).unwrap();
        assert_eq!(msg.message_type, TYPE_HANDSHAKE_INITIATION);
    }

    #[test]
    fn test_handshake_initiation_from_bytes_invalid_size() {
        let bytes = [0u8; HandshakeInitiation::SIZE - 1];
        let result = <&HandshakeInitiation>::try_from(&bytes[..]);
        assert!(matches!(result, Err(MessageError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_handshake_initiation_from_mut_bytes_invalid_size() {
        let mut bytes = [0u8; HandshakeInitiation::SIZE - 1];
        let result = <&mut HandshakeInitiation>::try_from(&mut bytes[..]);
        assert!(matches!(result, Err(MessageError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_handshake_response_default() {
        let msg = HandshakeResponse::default();
        assert_eq!(msg.message_type, TYPE_HANDSHAKE_RESPONSE);
    }

    #[test]
    fn test_handshake_response_mac1_input() {
        let msg = HandshakeResponse::default();
        let mac1_input = msg.mac1_input();
        assert_eq!(mac1_input.len(), HandshakeResponse::MAC1_OFFSET);
    }

    #[test]
    fn test_handshake_response_mac2_input() {
        let msg = HandshakeResponse::default();
        let mac2_input = msg.mac2_input();
        assert_eq!(mac2_input.len(), HandshakeResponse::MAC2_OFFSET);
    }

    #[test]
    fn test_handshake_response_from_bytes() {
        let mut bytes = [0u8; HandshakeResponse::SIZE];
        bytes[0] = TYPE_HANDSHAKE_RESPONSE;

        let msg = <&HandshakeResponse>::try_from(&bytes[..]).unwrap();
        assert_eq!(msg.message_type, TYPE_HANDSHAKE_RESPONSE);
    }

    #[test]
    fn test_handshake_response_from_bytes_invalid_size() {
        let bytes = [0u8; HandshakeResponse::SIZE - 1];
        let result = <&HandshakeResponse>::try_from(&bytes[..]);
        assert!(matches!(result, Err(MessageError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_handshake_response_from_mut_bytes() {
        let mut bytes = [0u8; HandshakeResponse::SIZE];
        bytes[0] = TYPE_HANDSHAKE_RESPONSE;

        let msg = <&mut HandshakeResponse>::try_from(&mut bytes[..]).unwrap();
        assert_eq!(msg.message_type, TYPE_HANDSHAKE_RESPONSE);
    }

    #[test]
    fn test_handshake_response_from_mut_bytes_invalid_size() {
        let mut bytes = [0u8; HandshakeResponse::SIZE - 1];
        let result = <&mut HandshakeResponse>::try_from(&mut bytes[..]);
        assert!(matches!(result, Err(MessageError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_cookie_reply_default() {
        let msg = CookieReply::default();
        assert_eq!(msg.message_type, TYPE_COOKIE_REPLY);
    }

    #[test]
    fn test_cookie_reply_from_bytes() {
        let mut bytes = [0u8; CookieReply::SIZE];
        bytes[0] = TYPE_COOKIE_REPLY;

        let msg = <&CookieReply>::try_from(&bytes[..]).unwrap();
        assert_eq!(msg.message_type, TYPE_COOKIE_REPLY);
    }

    #[test]
    fn test_cookie_reply_from_bytes_invalid_size() {
        let bytes = [0u8; CookieReply::SIZE - 1];
        let result = <&CookieReply>::try_from(&bytes[..]);
        assert!(matches!(result, Err(MessageError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_cookie_reply_from_mut_bytes() {
        let mut bytes = [0u8; CookieReply::SIZE];
        bytes[0] = TYPE_COOKIE_REPLY;

        let msg = <&mut CookieReply>::try_from(&mut bytes[..]).unwrap();
        assert_eq!(msg.message_type, TYPE_COOKIE_REPLY);
    }

    #[test]
    fn test_cookie_reply_from_mut_bytes_invalid_size() {
        let mut bytes = [0u8; CookieReply::SIZE - 1];
        let result = <&mut CookieReply>::try_from(&mut bytes[..]);
        assert!(matches!(result, Err(MessageError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_data_packet_header_default() {
        let msg = DataPacketHeader::default();
        assert_eq!(msg.message_type, TYPE_DATA);
    }

    #[test]
    fn test_data_packet_header_from_bytes() {
        let mut bytes = [0u8; DataPacketHeader::SIZE];
        bytes[0] = TYPE_DATA;

        let msg = <&DataPacketHeader>::try_from(&bytes[..]).unwrap();
        assert_eq!(msg.message_type, TYPE_DATA);
    }

    #[test]
    fn test_data_packet_header_from_bytes_with_extra() {
        let mut bytes = [0u8; DataPacketHeader::SIZE + 100];
        bytes[0] = TYPE_DATA;

        let msg = <&DataPacketHeader>::try_from(&bytes[..]).unwrap();
        assert_eq!(msg.message_type, TYPE_DATA);
    }

    #[test]
    fn test_data_packet_header_from_bytes_invalid_size() {
        let bytes = [0u8; DataPacketHeader::SIZE - 1];
        let result = <&DataPacketHeader>::try_from(&bytes[..]);
        assert!(matches!(result, Err(MessageError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_data_packet_header_from_mut_bytes() {
        let mut bytes = [0u8; DataPacketHeader::SIZE];
        bytes[0] = TYPE_DATA;

        let msg = <&mut DataPacketHeader>::try_from(&mut bytes[..]).unwrap();
        assert_eq!(msg.message_type, TYPE_DATA);
    }

    #[test]
    fn test_data_packet_header_from_mut_bytes_with_extra() {
        let mut bytes = [0u8; DataPacketHeader::SIZE + 100];
        bytes[0] = TYPE_DATA;

        let msg = <&mut DataPacketHeader>::try_from(&mut bytes[..]).unwrap();
        assert_eq!(msg.message_type, TYPE_DATA);
    }

    #[test]
    fn test_data_packet_header_from_mut_bytes_invalid_size() {
        let mut bytes = [0u8; DataPacketHeader::SIZE - 1];
        let result = <&mut DataPacketHeader>::try_from(&mut bytes[..]);
        assert!(matches!(result, Err(MessageError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_data_packet_from_bytes() {
        let mut bytes = [0u8; DataPacketHeader::SIZE + 100];
        bytes[0] = TYPE_DATA;

        let packet = DataPacket::try_from(&mut bytes[..]).unwrap();
        assert_eq!(packet.header.message_type, TYPE_DATA);
        assert_eq!(packet.plaintext.len(), 100);
    }

    #[test]
    fn test_data_packet_from_bytes_no_payload() {
        let mut bytes = [0u8; DataPacketHeader::SIZE];
        bytes[0] = TYPE_DATA;

        let packet = DataPacket::try_from(&mut bytes[..]).unwrap();
        assert_eq!(packet.header.message_type, TYPE_DATA);
        assert_eq!(packet.plaintext.len(), 0);
    }

    #[test]
    fn test_data_packet_from_bytes_invalid_size() {
        let mut bytes = [0u8; DataPacketHeader::SIZE - 1];
        let result = DataPacket::try_from(&mut bytes[..]);
        assert!(matches!(result, Err(MessageError::BufferTooSmall { .. })));
    }
}
