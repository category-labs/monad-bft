// Contains the common methods used for serializing and deserializing messages

use alloy_rlp::{encode_list, Decodable, Encodable, RlpDecodable, RlpEncodable};
use bytes::{BufMut, Bytes, BytesMut};
use monad_compress::{zstd::ZstdCompression, CompressionAlgo};

pub const SERIALIZE_VERSION: u32 = 1;
// compression versions
pub const UNCOMPRESSED_VERSION: u32 = 1;
pub const DEFAULT_ZSTD_VERSION: u32 = 2;

#[derive(RlpEncodable, RlpDecodable)]
pub struct NetworkMessageVersion {
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

#[derive(Debug)]
pub struct SerializeError(pub String);

pub(crate) fn try_serialize_rlp_message<M: Encodable>(
    msg_type: u8,
    rlp_message: M,
    version: NetworkMessageVersion,
    out_buf: &mut dyn BufMut,
) -> Result<(), SerializeError> {
    if version.compression_version == UNCOMPRESSED_VERSION {
        // encode as uncompressed message
        let enc: [&dyn Encodable; 3] = [&version, &msg_type, &rlp_message];
        encode_list::<_, dyn Encodable>(&enc, out_buf);
        Ok(())
    } else if version.compression_version == DEFAULT_ZSTD_VERSION {
        // Serialize the message into a temporary buffer,
        // compress, then encode the compressed message.
        let mut rlp_encoded_msg = BytesMut::new();
        rlp_message.encode(&mut rlp_encoded_msg);

        let mut compressed_message = Vec::new();
        ZstdCompression::default()
            .compress(&rlp_encoded_msg, &mut compressed_message)
            .map_err(|err| SerializeError(format!("compression error: {:?}", err)))?;
        let compressed_message = Bytes::from(compressed_message);

        // encode as compressed message
        let enc: [&dyn Encodable; 3] = [&version, &msg_type, &compressed_message];
        encode_list::<_, dyn Encodable>(&enc, out_buf);
        Ok(())
    } else {
        unreachable!() // Logic error, invalid compression version?
    }
}

#[derive(Debug)]
pub struct DeserializeError(pub String);

impl From<alloy_rlp::Error> for DeserializeError {
    fn from(err: alloy_rlp::Error) -> Self {
        DeserializeError(format!("rlp decode error: {:?}", err))
    }
}

pub(crate) fn try_deserialize_rlp_message<M: Decodable>(
    version: NetworkMessageVersion,
    payload: &mut &[u8],
) -> Result<M, DeserializeError> {
    match version.compression_version {
        UNCOMPRESSED_VERSION => {
            // decode as uncompressed message
            let decoded_msg = M::decode(payload).map_err(DeserializeError::from)?;
            Ok(decoded_msg)
        }
        DEFAULT_ZSTD_VERSION => {
            let compressed_app_message = Bytes::decode(payload).map_err(DeserializeError::from)?;
            // decompress message
            let mut decompressed_app_message = Vec::new();
            ZstdCompression::default()
                .decompress(&compressed_app_message, &mut decompressed_app_message)
                .map_err(|err| DeserializeError(format!("decompression error: {:?}", err)))?;

            let decoded_msg = M::decode(&mut decompressed_app_message.as_ref())
                .map_err(DeserializeError::from)?;
            Ok(decoded_msg)
        }
        _ => Err(DeserializeError("unknown compression version".into())),
    }
}
