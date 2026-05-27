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

use alloy_rlp::{encode_list, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use bytes::{Bytes, BytesMut};
use monad_compress::{util::BoundedWriter, zstd::ZstdCompression, CompressionAlgo};
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_peer_discovery::PeerDiscoveryMessage;
use thiserror::Error;

use super::raptorcast_secondary::group_message::FullNodesGroupMessage;

const SERIALIZE_VERSION: u32 = 1;
pub const MAX_MESSAGE_SIZE: usize = 3 * 1024 * 1024;

enum CompressionVersion {
    UncompressedVersion,
    DefaultZSTDVersion,
}

impl Encodable for CompressionVersion {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            CompressionVersion::UncompressedVersion => {
                out.put_u8(1);
            }
            CompressionVersion::DefaultZSTDVersion => {
                out.put_u8(2);
            }
        }
    }
}

impl Decodable for CompressionVersion {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        match u8::decode(buf)? {
            1 => Ok(Self::UncompressedVersion),
            2 => Ok(Self::DefaultZSTDVersion),
            _ => Err(alloy_rlp::Error::Custom("unexpected compression version")),
        }
    }
}

#[derive(RlpEncodable, RlpDecodable)]
struct NetworkMessageVersion {
    pub serialize_version: u32,
    pub compression_version: CompressionVersion,
}

impl NetworkMessageVersion {
    pub fn version() -> Self {
        Self {
            serialize_version: SERIALIZE_VERSION,
            compression_version: CompressionVersion::DefaultZSTDVersion,
        }
    }
}

const MESSAGE_TYPE_APP: u8 = 1;
const MESSAGE_TYPE_PEER_DISC: u8 = 2;
const MESSAGE_TYPE_GROUP: u8 = 3;

pub enum OutboundRouterMessage<OM, ST: CertificateSignatureRecoverable> {
    AppMessage(OM),
    PeerDiscoveryMessage(PeerDiscoveryMessage<ST>),
    FullNodesGroup(FullNodesGroupMessage<ST>),
}

#[derive(Debug, Error)]
pub enum SerializeError {
    #[error("final message too large: {0} bytes exceeds maximum of {1} bytes")]
    FinalMsgTooLarge(usize, usize),
    #[error("inner rlp message too large: {0} bytes exceeds maximum of {1} bytes")]
    InnerMsgTooLarge(usize, usize),
}

impl<OM: Encodable, ST: CertificateSignatureRecoverable> OutboundRouterMessage<OM, ST> {
    pub fn try_serialize(self) -> Result<Bytes, SerializeError> {
        self.try_serialize_with_config(NetworkMessageVersion::version(), MAX_MESSAGE_SIZE)
    }

    fn try_serialize_with_config(
        &self,
        version: NetworkMessageVersion,
        max_message_size: usize,
    ) -> Result<Bytes, SerializeError> {
        let mut buf = BytesMut::new();
        match self {
            Self::AppMessage(app_message) => {
                match version.compression_version {
                    CompressionVersion::UncompressedVersion => {
                        // encode as uncompressed message
                        let enc: [&dyn Encodable; 3] = [&version, &MESSAGE_TYPE_APP, &app_message];
                        encode_list::<_, dyn Encodable>(&enc, &mut buf);
                    }
                    CompressionVersion::DefaultZSTDVersion => {
                        let mut rlp_encoded_msg = BytesMut::new();
                        app_message.encode(&mut rlp_encoded_msg);
                        if rlp_encoded_msg.len() > max_message_size {
                            return Err(SerializeError::InnerMsgTooLarge(
                                rlp_encoded_msg.len(),
                                max_message_size,
                            ));
                        };
                        let message_len = u32::try_from(rlp_encoded_msg.len()).map_err(|_| {
                            SerializeError::InnerMsgTooLarge(
                                rlp_encoded_msg.len(),
                                max_message_size,
                            )
                        })?;

                        let mut compressed_writer = BoundedWriter::new(message_len);
                        match ZstdCompression::default()
                            .compress(&rlp_encoded_msg, &mut compressed_writer)
                        {
                            Ok(_) => {
                                let compressed_app_message: Bytes = compressed_writer.into();
                                let enc: [&dyn Encodable; 4] = [
                                    &version,
                                    &MESSAGE_TYPE_APP,
                                    &message_len,
                                    &compressed_app_message,
                                ];
                                encode_list::<_, dyn Encodable>(&enc, &mut buf);
                            }
                            Err(err) => {
                                tracing::warn!(
                                    ?err,
                                    "compression failed, falling back to uncompressed"
                                );
                                let uncompressed_version = NetworkMessageVersion {
                                    serialize_version: version.serialize_version,
                                    compression_version: CompressionVersion::UncompressedVersion,
                                };
                                let enc: [&dyn Encodable; 3] =
                                    [&uncompressed_version, &MESSAGE_TYPE_APP, &app_message];
                                encode_list::<_, dyn Encodable>(&enc, &mut buf);
                            }
                        }
                    }
                }
            }
            Self::PeerDiscoveryMessage(peer_disc_message) => {
                // encode as uncompressed message
                let enc: [&dyn Encodable; 3] =
                    [&version, &MESSAGE_TYPE_PEER_DISC, &peer_disc_message];
                encode_list::<_, dyn Encodable>(&enc, &mut buf);
            }
            Self::FullNodesGroup(group_message) => {
                // encode as uncompressed message
                let enc: [&dyn Encodable; 3] = [&version, &MESSAGE_TYPE_GROUP, &group_message];
                encode_list::<_, dyn Encodable>(&enc, &mut buf);
            }
        };
        if buf.len() > max_message_size {
            return Err(SerializeError::FinalMsgTooLarge(
                buf.len(),
                max_message_size,
            ));
        }
        Ok(buf.into())
    }
}

pub enum InboundRouterMessage<M, ST: CertificateSignatureRecoverable> {
    AppMessage(M),
    PeerDiscoveryMessage(PeerDiscoveryMessage<ST>),
    FullNodesGroup(FullNodesGroupMessage<ST>),
}

#[derive(Debug, Error, PartialEq)]
pub enum DeserializeError {
    #[error("rlp decode error: {0}")]
    RlpDecode(#[from] alloy_rlp::Error),
    #[error("extra data after header")]
    ExtraDataAfterHeader,
    #[error("extra data in payload")]
    ExtraDataInPayload,
    #[error("message size {size} exceeds maximum allowed size {max}")]
    MessageTooLarge { size: usize, max: usize },
    #[error("decompression error: {0}")]
    DecompressionError(String),
    #[error("unexpected decompressed message length. expected: {expected}, actual: {actual}")]
    UnexpectedDecompressedLength { expected: usize, actual: usize },
    #[error("unknown message type")]
    UnknownMessageType,
}

impl<M: Decodable, ST: CertificateSignatureRecoverable> InboundRouterMessage<M, ST> {
    pub fn try_deserialize(data: &Bytes) -> Result<Self, DeserializeError> {
        Self::try_deserialize_with_config(data, false)
    }

    pub fn try_deserialize_with_config(
        data: &Bytes,
        allow_zstd: bool,
    ) -> Result<Self, DeserializeError> {
        if data.len() > MAX_MESSAGE_SIZE {
            return Err(DeserializeError::MessageTooLarge {
                size: data.len(),
                max: MAX_MESSAGE_SIZE,
            });
        }
        let mut data_ref = data.as_ref();
        let mut payload = Header::decode_bytes(&mut data_ref, true)?;
        if !data_ref.is_empty() {
            return Err(DeserializeError::ExtraDataAfterHeader);
        }
        let version = NetworkMessageVersion::decode(&mut payload)?;
        let message_type = u8::decode(&mut payload)?;
        let result = match message_type {
            MESSAGE_TYPE_APP => {
                match version.compression_version {
                    CompressionVersion::UncompressedVersion => {
                        // decode as uncompressed message
                        let app_message = M::decode(&mut payload)?;
                        Ok(Self::AppMessage(app_message))
                    }
                    CompressionVersion::DefaultZSTDVersion => {
                        if !allow_zstd {
                            return Err(DeserializeError::DecompressionError(
                                "zstd compression is disabled".to_string(),
                            ));
                        }
                        let decompressed_message_len = u32::decode(&mut payload)?;
                        if decompressed_message_len as usize > MAX_MESSAGE_SIZE {
                            return Err(DeserializeError::MessageTooLarge {
                                size: decompressed_message_len as usize,
                                max: MAX_MESSAGE_SIZE,
                            });
                        }
                        let compressed_app_message = Bytes::decode(&mut payload)?;

                        // decompress message
                        let mut decompressed_writer = BoundedWriter::new(decompressed_message_len);
                        ZstdCompression::default()
                            .decompress(&compressed_app_message, &mut decompressed_writer)
                            .map_err(|e| DeserializeError::DecompressionError(e.to_string()))?;
                        let decompressed_app_message: Bytes = decompressed_writer.into();

                        if decompressed_app_message.len() < decompressed_message_len as usize {
                            return Err(DeserializeError::UnexpectedDecompressedLength {
                                expected: decompressed_message_len as usize,
                                actual: decompressed_app_message.len(),
                            });
                        }

                        let app_message = M::decode(&mut decompressed_app_message.as_ref())?;
                        Ok(Self::AppMessage(app_message))
                    }
                }
            }
            MESSAGE_TYPE_PEER_DISC => {
                let peer_disc_message = PeerDiscoveryMessage::decode(&mut payload)?;
                Ok(Self::PeerDiscoveryMessage(peer_disc_message))
            }
            MESSAGE_TYPE_GROUP => {
                let group_message = FullNodesGroupMessage::decode(&mut payload)?;
                Ok(Self::FullNodesGroup(group_message))
            }
            _ => Err(DeserializeError::UnknownMessageType),
        };
        if !payload.is_empty() {
            return Err(DeserializeError::ExtraDataInPayload);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use monad_secp::SecpSignature;
    use rstest::*;

    use super::*;

    #[derive(RlpEncodable, RlpDecodable)]
    struct TestMessage {
        value: u64,
    }

    #[derive(RlpEncodable, RlpDecodable, Clone)]
    struct SliceMessage {
        data: Vec<u8>,
    }

    fn prepare_data_with_extra_after_header() -> (Bytes, DeserializeError) {
        let msg = TestMessage { value: 42 };
        let outbound_msg = OutboundRouterMessage::<TestMessage, SecpSignature>::AppMessage(msg);

        let data = outbound_msg.try_serialize().unwrap();
        let data_bytes = data.to_vec();
        let mut buf = BytesMut::from(data_bytes.as_slice());
        buf.extend_from_slice(b"extra_data");

        (buf.freeze(), DeserializeError::ExtraDataAfterHeader)
    }

    fn prepare_data_with_extra_in_payload() -> (Bytes, DeserializeError) {
        let msg = TestMessage { value: 42 };
        let version = NetworkMessageVersion::version();

        let mut inner_buf = BytesMut::new();
        version.encode(&mut inner_buf);
        MESSAGE_TYPE_APP.encode(&mut inner_buf);
        msg.encode(&mut inner_buf);
        inner_buf.extend_from_slice(b"extra");

        let mut buf = BytesMut::new();
        Header {
            list: true,
            payload_length: inner_buf.len(),
        }
        .encode(&mut buf);
        buf.extend_from_slice(&inner_buf);

        (buf.freeze(), DeserializeError::ExtraDataInPayload)
    }

    fn prepare_valid_data() -> Bytes {
        let msg = TestMessage { value: 42 };
        let outbound_msg = OutboundRouterMessage::<TestMessage, SecpSignature>::AppMessage(msg);
        outbound_msg.try_serialize().unwrap()
    }

    fn prepare_oversized_message() -> (Bytes, DeserializeError) {
        let oversized_data = vec![0u8; MAX_MESSAGE_SIZE + 1];
        (
            Bytes::from(oversized_data),
            DeserializeError::MessageTooLarge {
                size: MAX_MESSAGE_SIZE + 1,
                max: MAX_MESSAGE_SIZE,
            },
        )
    }

    fn prepare_unknown_message_type() -> (Bytes, DeserializeError) {
        let version = NetworkMessageVersion::version();

        let mut inner_buf = BytesMut::new();
        version.encode(&mut inner_buf);
        99u8.encode(&mut inner_buf);

        let mut buf = BytesMut::new();
        Header {
            list: true,
            payload_length: inner_buf.len(),
        }
        .encode(&mut buf);
        buf.extend_from_slice(&inner_buf);

        (buf.freeze(), DeserializeError::UnknownMessageType)
    }

    #[rstest]
    #[case::extra_after_header(prepare_data_with_extra_after_header())]
    #[case::extra_in_payload(prepare_data_with_extra_in_payload())]
    #[case::oversized_message(prepare_oversized_message())]
    #[case::unknown_message_type(prepare_unknown_message_type())]
    fn test_deserialize_validation_errors(#[case] test_case: (Bytes, DeserializeError)) {
        let (data, expected_err) = test_case;
        let result = InboundRouterMessage::<TestMessage, SecpSignature>::try_deserialize(&data);
        match result {
            Err(actual_err) => assert_eq!(actual_err, expected_err),
            Ok(_) => panic!("expected error {:?} but got success", expected_err),
        }
    }

    #[test]
    fn test_deserialize_validation_success() {
        let data = prepare_valid_data();
        let result = InboundRouterMessage::<TestMessage, SecpSignature>::try_deserialize(&data);
        match result.unwrap() {
            InboundRouterMessage::AppMessage(decoded_msg) => {
                assert_eq!(decoded_msg.value, 42);
            }
            _ => panic!("expected AppMessage variant"),
        }
    }

    #[test]
    fn test_serialized_compressed_with_random_bytes() {
        let random_bytes = SliceMessage {
            data: vec![0x42, 0x73, 0x19, 0xAB, 0xCD, 0xEF],
        };
        let msg = OutboundRouterMessage::<_, SecpSignature>::AppMessage(random_bytes.clone());
        let mut version = NetworkMessageVersion::version();
        version.compression_version = CompressionVersion::DefaultZSTDVersion;

        let serialized = msg
            .try_serialize_with_config(version, MAX_MESSAGE_SIZE)
            .unwrap();
        let deserialized =
            InboundRouterMessage::<SliceMessage, SecpSignature>::try_deserialize_with_config(
                &serialized,
                true,
            );
        assert!(deserialized.is_ok());

        match deserialized.unwrap() {
            InboundRouterMessage::AppMessage(msg) => {
                assert_eq!(msg.data, random_bytes.data);
            }
            _ => panic!("expected AppMessage"),
        }
    }

    #[test]
    fn test_compressed_message_rejected_by_default() {
        let data = vec![0x42; 1000];
        let compressible_msg = SliceMessage { data };
        let msg = OutboundRouterMessage::<_, SecpSignature>::AppMessage(compressible_msg);
        let mut version = NetworkMessageVersion::version();
        version.compression_version = CompressionVersion::DefaultZSTDVersion;

        let serialized = msg
            .try_serialize_with_config(version, MAX_MESSAGE_SIZE)
            .unwrap();

        let deserialized =
            InboundRouterMessage::<SliceMessage, SecpSignature>::try_deserialize(&serialized);

        assert!(deserialized.is_err());
        match deserialized {
            Err(DeserializeError::DecompressionError(msg)) => {
                assert_eq!(msg, "zstd compression is disabled");
            }
            _ => panic!("expected DecompressionError with 'zstd compression is disabled' message"),
        }
    }

    #[test]
    fn test_serialize_exceeds_3mb_limit() {
        let large_data = vec![0u8; MAX_MESSAGE_SIZE];
        let large_msg = SliceMessage { data: large_data };
        let msg = OutboundRouterMessage::<_, SecpSignature>::AppMessage(large_msg);

        let result = msg.try_serialize();
        assert!(result.is_err());

        if let Err(SerializeError::FinalMsgTooLarge(size, max)) = result {
            assert!(size > max);
            assert_eq!(max, MAX_MESSAGE_SIZE);
        } else {
            panic!("expected FinalMsgTooLarge error");
        }
    }

    #[test]
    fn test_outbound_message_serialize_snapshot() {
        let msg = TestMessage { value: 42 };
        let outbound_msg = OutboundRouterMessage::<TestMessage, SecpSignature>::AppMessage(msg);

        let serialized = outbound_msg.try_serialize().unwrap();
        let hex_encoded = hex::encode(&serialized);

        insta::assert_snapshot!(hex_encoded);
    }

    #[test]
    fn test_inbound_message_deserialize_snapshot() {
        let msg = TestMessage { value: 12345 };
        let outbound_msg = OutboundRouterMessage::<TestMessage, SecpSignature>::AppMessage(msg);

        let serialized = outbound_msg.try_serialize().unwrap();
        let hex_encoded_original = hex::encode(&serialized);

        let deserialized =
            InboundRouterMessage::<TestMessage, SecpSignature>::try_deserialize(&serialized)
                .unwrap();

        match deserialized {
            InboundRouterMessage::AppMessage(decoded_msg) => {
                assert_eq!(decoded_msg.value, 12345);

                let reserialize_msg =
                    OutboundRouterMessage::<TestMessage, SecpSignature>::AppMessage(decoded_msg);
                let reserialized = reserialize_msg.try_serialize().unwrap();
                let hex_encoded_roundtrip = hex::encode(&reserialized);

                assert_eq!(hex_encoded_original, hex_encoded_roundtrip);
                insta::assert_snapshot!(hex_encoded_roundtrip);
            }
            _ => panic!("expected AppMessage variant"),
        }
    }
}
