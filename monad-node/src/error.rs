use clap::error::ErrorKind;
use monad_consensus_types::signature_collection::SignatureCollection;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NodeSetupError {
    #[error(transparent)]
    ClapError(#[from] clap::Error),

    #[error("{msg}")]
    Custom { kind: ErrorKind, msg: String },

    #[error(transparent)]
    EnvLoggerError(log::SetLoggerError),

    #[error(transparent)]
    FromHexError(#[from] hex::FromHexError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    Secp256k1(#[from] monad_crypto::secp256k1::Error),

    #[error(transparent)]
    Bls12_381(#[from] monad_crypto::bls12_381::BlsError),

    #[error(transparent)]
    SignatureCollectionError(
        #[from]
        monad_consensus_types::signature_collection::SignatureCollectionError<
            <crate::SignatureCollectionType as SignatureCollection>::NodeIdPubKey,
            <crate::SignatureCollectionType as SignatureCollection>::SignatureType,
        >,
    ),

    #[error(transparent)]
    TomlDeError(#[from] toml::de::Error),

    #[error(transparent)]
    TraceError(#[from] opentelemetry_api::trace::TraceError),
}

impl NodeSetupError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            NodeSetupError::ClapError(e) => e.kind(),
            NodeSetupError::Custom { kind, msg: _ } => kind.to_owned(),
            NodeSetupError::EnvLoggerError(_) => ErrorKind::Io,
            NodeSetupError::FromHexError(_) => ErrorKind::ValueValidation,
            NodeSetupError::IoError(_) => ErrorKind::Io,
            NodeSetupError::Secp256k1(_) => ErrorKind::ValueValidation,
            NodeSetupError::Bls12_381(_) => ErrorKind::ValueValidation,
            NodeSetupError::SignatureCollectionError(_) => ErrorKind::ValueValidation,
            NodeSetupError::TomlDeError(_) => ErrorKind::ValueValidation,
            NodeSetupError::TraceError(_) => ErrorKind::ValueValidation,
        }
    }
}
