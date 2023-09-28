use std::fmt;

#[derive(Debug, Clone)]
pub enum MempoolProtoError {
    DecodeError(prost::DecodeError),
    RlpDecodeError(reth_rlp::DecodeError),
    InvalidSignature(reth_primitives::TransactionSigned),
    MissingRequiredField(String),
    TooLarge(usize),
}

impl fmt::Display for MempoolProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DecodeError(err) => write!(f, "Decoder error: {}", err),
            Self::RlpDecodeError(err) => write!(f, "RlpDecoder error: {}", err),
            Self::InvalidSignature(tx) => write!(f, "InvalidSignature for tx: {:?}", tx),
            Self::MissingRequiredField(s) => write!(f, "Missing required field: {}", s),
            Self::TooLarge(size) => write!(f, "Too large: {} bytes", size),
        }
    }
}

impl std::error::Error for MempoolProtoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Self::DecodeError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<prost::DecodeError> for MempoolProtoError {
    fn from(value: prost::DecodeError) -> Self {
        Self::DecodeError(value)
    }
}

impl From<reth_rlp::DecodeError> for MempoolProtoError {
    fn from(value: reth_rlp::DecodeError) -> Self {
        Self::RlpDecodeError(value)
    }
}
