use alloy_rlp::{encode_list, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use bytes::{Bytes, BytesMut};
use monad_compress::{util::BoundedWriter, zstd::ZstdCompression, CompressionAlgo};
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_peer_discovery::PeerDiscoveryMessage;

use super::raptorcast_secondary::group_message::FullNodesGroupMessage;

const SERIALIZE_VERSION: u32 = 1;

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
            compression_version: CompressionVersion::UncompressedVersion,
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

impl<OM: Encodable, ST: CertificateSignatureRecoverable> OutboundRouterMessage<OM, ST> {
    pub fn serialize(self) -> Bytes {
        let version = NetworkMessageVersion::version();
        self.serialize_with_version(version)
    }

    fn serialize_with_version(&self, version: NetworkMessageVersion) -> Bytes {
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
                        let message_len = rlp_encoded_msg.len() as u32;

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

        buf.into()
    }
}

pub enum InboundRouterMessage<M, ST: CertificateSignatureRecoverable> {
    AppMessage(M),
    PeerDiscoveryMessage(PeerDiscoveryMessage<ST>),
    FullNodesGroup(FullNodesGroupMessage<ST>),
}

#[derive(Debug)]
pub struct DeserializeError(pub String);

impl From<alloy_rlp::Error> for DeserializeError {
    fn from(err: alloy_rlp::Error) -> Self {
        DeserializeError(format!("rlp decode error: {:?}", err))
    }
}

impl<M: Decodable, ST: CertificateSignatureRecoverable> InboundRouterMessage<M, ST> {
    pub fn try_deserialize(data: &Bytes) -> Result<Self, DeserializeError> {
        let mut data_ref = data.as_ref();
        let mut payload =
            Header::decode_bytes(&mut data_ref, true).map_err(DeserializeError::from)?;
        if !data_ref.is_empty() {
            return Err(DeserializeError("extra data after header".into()));
        }
        let version =
            NetworkMessageVersion::decode(&mut payload).map_err(DeserializeError::from)?;
        let message_type = u8::decode(&mut payload).map_err(DeserializeError::from)?;
        let result = match message_type {
            MESSAGE_TYPE_APP => {
                match version.compression_version {
                    CompressionVersion::UncompressedVersion => {
                        // decode as uncompressed message
                        let app_message =
                            M::decode(&mut payload).map_err(DeserializeError::from)?;
                        Ok(Self::AppMessage(app_message))
                    }
                    CompressionVersion::DefaultZSTDVersion => {
                        let decompressed_message_len =
                            u32::decode(&mut payload).map_err(DeserializeError::from)?;
                        let compressed_app_message =
                            Bytes::decode(&mut payload).map_err(DeserializeError::from)?;

                        // decompress message
                        let mut decompressed_writer = BoundedWriter::new(decompressed_message_len);
                        ZstdCompression::default()
                            .decompress(&compressed_app_message, &mut decompressed_writer)
                            .map_err(|err| {
                                DeserializeError(format!("decompression error: {:?}", err))
                            })?;
                        let decompressed_app_message: Bytes = decompressed_writer.into();

                        if decompressed_app_message.len() < decompressed_message_len as usize {
                            return Err(DeserializeError(format!(
                                "unexpected decompressed message length. expected: {}, actual: {}",
                                decompressed_message_len,
                                decompressed_app_message.len()
                            )));
                        }

                        let app_message = M::decode(&mut decompressed_app_message.as_ref())
                            .map_err(DeserializeError::from)?;
                        Ok(Self::AppMessage(app_message))
                    }
                }
            }
            MESSAGE_TYPE_PEER_DISC => {
                let peer_disc_message =
                    PeerDiscoveryMessage::decode(&mut payload).map_err(DeserializeError::from)?;
                Ok(Self::PeerDiscoveryMessage(peer_disc_message))
            }
            MESSAGE_TYPE_GROUP => {
                let group_message =
                    FullNodesGroupMessage::decode(&mut payload).map_err(DeserializeError::from)?;
                Ok(Self::FullNodesGroup(group_message))
            }
            _ => Err(DeserializeError("unknown message type".into())),
        };
        if !payload.is_empty() {
            return Err(DeserializeError("extra data in payload".into()));
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

    fn prepare_data_with_extra_after_header() -> Bytes {
        let msg = TestMessage { value: 42 };
        let outbound_msg = OutboundRouterMessage::<TestMessage, SecpSignature>::AppMessage(msg);

        let data = outbound_msg.serialize();
        let data_bytes = data.to_vec();
        let mut buf = BytesMut::from(data_bytes.as_slice());
        buf.extend_from_slice(b"extra_data");

        buf.freeze()
    }

    fn prepare_data_with_extra_in_payload() -> Bytes {
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

        buf.freeze()
    }

    fn prepare_valid_data() -> Bytes {
        let msg = TestMessage { value: 42 };
        let outbound_msg = OutboundRouterMessage::<TestMessage, SecpSignature>::AppMessage(msg);
        outbound_msg.serialize()
    }

    enum Expected {
        Error(&'static str),
        Success { value: u64 },
    }

    #[rstest]
    #[case::extra_after_header(
        prepare_data_with_extra_after_header(),
        Expected::Error("extra data after header")
    )]
    #[case::extra_in_payload(
        prepare_data_with_extra_in_payload(),
        Expected::Error("extra data in payload")
    )]
    #[case::valid_message(prepare_valid_data(), Expected::Success { value: 42 })]
    fn test_deserialize_validation(#[case] data: Bytes, #[case] expected: Expected) {
        let result = InboundRouterMessage::<TestMessage, SecpSignature>::try_deserialize(&data);
        match (result, expected) {
            (Err(DeserializeError(msg)), Expected::Error(expected_msg)) => {
                assert!(
                    msg.contains(expected_msg),
                    "error message '{}' should contain '{}'",
                    msg,
                    expected_msg
                );
            }
            (Ok(InboundRouterMessage::AppMessage(decoded_msg)), Expected::Success { value }) => {
                assert_eq!(decoded_msg.value, value);
            }
            (Ok(_), Expected::Success { .. }) => panic!("expected AppMessage variant"),
            (Err(_), Expected::Success { .. }) => panic!("expected successful deserialization"),
            (Ok(_), Expected::Error(expected_msg)) => {
                panic!("expected error containing '{}'", expected_msg)
            }
        }
    }

    #[test]
    fn test_serialized_compressed_with_random_bytes() {
        let random_bytes = SliceMessage {
            data: vec![0x42, 0x73, 0x19, 0xAB, 0xCD, 0xEF],
        };
        let msg = OutboundRouterMessage::<_, SecpSignature>::AppMessage(random_bytes.clone());
        let mut compressed_version = NetworkMessageVersion::version();
        compressed_version.compression_version = CompressionVersion::DefaultZSTDVersion;

        let serialized = msg.serialize_with_version(compressed_version);
        let deserialized =
            InboundRouterMessage::<SliceMessage, SecpSignature>::try_deserialize(&serialized);
        assert!(deserialized.is_ok());

        match deserialized.unwrap() {
            InboundRouterMessage::AppMessage(msg) => {
                assert_eq!(msg.data, random_bytes.data);
            }
            _ => panic!("expected AppMessage"),
        }
    }
}
