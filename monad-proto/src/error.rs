use std::fmt;
#[derive(Debug, Clone)]
pub enum ProtoError {
    DecodeError(prost::DecodeError),
    WrongHashLen(String),
    MissingRequiredField(String),
    InvalidNodeId(String),
    CryptoError(String),
    SignatureHashMismatch(String),
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DecodeError(err) => write!(f, "Decoder error: {}", err),
            Self::WrongHashLen(s) => write!(f, "Wrong hash len: {}", s),
            Self::MissingRequiredField(s) => write!(f, "Missing required field: {}", s),
            Self::InvalidNodeId(s) => write!(f, "Invalid NodeId: {}", s),
            Self::CryptoError(err) => write!(f, "Crypto error: {}", err),
            Self::SignatureHashMismatch(s) => write!(f, "QC Signature hash mismatch: {}", s),
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
