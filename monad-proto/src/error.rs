use monad_crypto::secp256k1;
use std::fmt;
#[derive(Debug)]
pub enum ProtoError {
    DecodeError(prost::DecodeError),
    WrongHashLen(String),
    MissingRequiredField(String),
    InvalidNodeId(String),
    Secp256k1Error(secp256k1::Error),
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DecodeError(err) => write!(f, "Decoder error: {}", err),
            Self::WrongHashLen(s) => write!(f, "Wrong hash len: {}", s),
            Self::MissingRequiredField(s) => write!(f, "Missing required field: {}", s),
            Self::InvalidNodeId(s) => write!(f, "Invalid NodeId: {}", s),
            Self::Secp256k1Error(err) => write!(f, "Crypto error: {}", err),
        }
    }
}

impl std::error::Error for ProtoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Self::DecodeError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<prost::DecodeError> for ProtoError {
    fn from(value: prost::DecodeError) -> Self {
        Self::DecodeError(value)
    }
}

impl From<secp256k1::Error> for ProtoError {
    fn from(value: secp256k1::Error) -> Self {
        Self::Secp256k1Error(value)
    }
}
