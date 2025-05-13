use alloy_rlp::{encode_list, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use bytes::{Bytes, BytesMut};
use monad_compress::{zstd::ZstdCompression, CompressionAlgo};
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_peer_discovery::PeerDiscoveryMessage;

const SERIALIZE_VERSION: u32 = 1;
// compression versions
const UNCOMPRESSED_VERSION: u32 = 1;
const DEFAULT_ZSTD_VERSION: u32 = 2;

#[derive(RlpEncodable, RlpDecodable)]
struct NetworkMessageVersion {
    pub serialize_version: u32,
    pub compression_version: u32,
}

impl NetworkMessageVersion {
    pub fn version() -> Self {
        Self {
            serialize_version: SERIALIZE_VERSION,
            compression_version: UNCOMPRESSED_VERSION,
        }
    }
}

#[derive(Clone)]
pub enum OutboundRouterMessage<OM, ST: CertificateSignatureRecoverable> {
    AppMessage(OM),
    PeerDiscoveryMessage(PeerDiscoveryMessage<ST>),
}

#[derive(Debug)]
pub struct SerializeError(pub String);

impl<OM: Encodable, ST: CertificateSignatureRecoverable> OutboundRouterMessage<OM, ST> {
    pub fn try_serialize(self) -> Result<Bytes, SerializeError> {
        let mut buf = BytesMut::new();

        let version = NetworkMessageVersion::version();
        match self {
            Self::AppMessage(app_message) => {
                if version.compression_version == UNCOMPRESSED_VERSION {
                    // encode as uncompressed message
                    let enc: [&dyn Encodable; 3] = [&version, &1u8, &app_message];
                    encode_list::<_, dyn Encodable>(&enc, &mut buf);
                } else if version.compression_version == DEFAULT_ZSTD_VERSION {
                    // compress message
                    let mut rlp_encoded_msg = BytesMut::new();
                    app_message.encode(&mut rlp_encoded_msg);

                    let mut compressed_app_message = Vec::new();
                    ZstdCompression::default()
                        .compress(&rlp_encoded_msg, &mut compressed_app_message)
                        .map_err(|err| SerializeError(format!("compression error: {:?}", err)))?;
                    let compressed_app_message = Bytes::from(compressed_app_message);

                    // encode as compressed message
                    let enc: [&dyn Encodable; 3] = [&version, &1u8, &compressed_app_message];
                    encode_list::<_, dyn Encodable>(&enc, &mut buf);
                } else {
                    unreachable!()
                };
            }
            Self::PeerDiscoveryMessage(peer_disc_message) => {
                // TODO: decide whether it should be compressed
                // encode as uncompressed message
                let enc: [&dyn Encodable; 3] = [&version, &2u8, &peer_disc_message];
                encode_list::<_, dyn Encodable>(&enc, &mut buf);
            }
        };

        Ok(buf.into())
    }
}

pub enum InboundRouterMessage<M, ST: CertificateSignatureRecoverable> {
    AppMessage(M),
    PeerDiscoveryMessage(PeerDiscoveryMessage<ST>),
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
        let mut payload =
            Header::decode_bytes(&mut data.as_ref(), true).map_err(DeserializeError::from)?;

        let version =
            NetworkMessageVersion::decode(&mut payload).map_err(DeserializeError::from)?;
        let message_type = u8::decode(&mut payload).map_err(DeserializeError::from)?;
        match message_type {
            1 => {
                match version.compression_version {
                    UNCOMPRESSED_VERSION => {
                        // decode as uncompressed message
                        let app_message =
                            M::decode(&mut payload).map_err(DeserializeError::from)?;
                        Ok(Self::AppMessage(app_message))
                    }
                    DEFAULT_ZSTD_VERSION => {
                        let compressed_app_message =
                            Bytes::decode(&mut payload).map_err(DeserializeError::from)?;

                        // decompress message
                        let mut decompressed_app_message = Vec::new();
                        ZstdCompression::default()
                            .decompress(&compressed_app_message, &mut decompressed_app_message)
                            .map_err(|err| {
                                DeserializeError(format!("decompression error: {:?}", err))
                            })?;

                        let app_message = M::decode(&mut decompressed_app_message.as_ref())
                            .map_err(DeserializeError::from)?;
                        Ok(Self::AppMessage(app_message))
                    }
                    _ => Err(DeserializeError("unknown compression version".into())),
                }
            }
            2 => {
                // TODO: decide if peer discovery message should be compressed
                let peer_disc_message =
                    PeerDiscoveryMessage::decode(&mut payload).map_err(DeserializeError::from)?;
                Ok(Self::PeerDiscoveryMessage(peer_disc_message))
            }
            _ => Err(DeserializeError("unknown message type".into())),
        }
    }
}
