use monad_proto::{
    error::ProtoError,
    proto::compress::{
        proto_compression_algorithm, ProtoBrotliCompression, ProtoCompressionAlgorithm,
        ProtoDeflateCompression, ProtoLz4Compression,
    },
};

use crate::compress::CompressionAlgorithm;

impl TryFrom<monad_proto::proto::compress::ProtoCompressionAlgorithm> for CompressionAlgorithm {
    type Error = ProtoError;

    fn try_from(
        algorithm: monad_proto::proto::compress::ProtoCompressionAlgorithm,
    ) -> Result<Self, Self::Error> {
        match algorithm
            .compression
            .ok_or(Self::Error::MissingRequiredField(
                "ProtoCompressionAlgorithm.compression".to_owned(),
            ))? {
            proto_compression_algorithm::Compression::Brotli(ProtoBrotliCompression { .. }) => {
                Ok(CompressionAlgorithm::BrotliCompression {})
            }
            proto_compression_algorithm::Compression::Deflate(ProtoDeflateCompression {
                ..
            }) => Ok(CompressionAlgorithm::DeflateCompression {}),
            proto_compression_algorithm::Compression::Lz4(ProtoLz4Compression { .. }) => {
                Ok(CompressionAlgorithm::Lz4 {})
            }
        }
    }
}
impl From<&CompressionAlgorithm> for ProtoCompressionAlgorithm {
    fn from(algorithm: &CompressionAlgorithm) -> Self {
        match algorithm {
            CompressionAlgorithm::Lz4 { .. } => ProtoCompressionAlgorithm {
                compression: Some(proto_compression_algorithm::Compression::Lz4(
                    ProtoLz4Compression {},
                )),
            },
            CompressionAlgorithm::BrotliCompression { .. } => ProtoCompressionAlgorithm {
                compression: Some(proto_compression_algorithm::Compression::Brotli(
                    ProtoBrotliCompression {},
                )),
            },
            CompressionAlgorithm::DeflateCompression { .. } => ProtoCompressionAlgorithm {
                compression: Some(proto_compression_algorithm::Compression::Deflate(
                    ProtoDeflateCompression {},
                )),
            },
        }
    }
}
