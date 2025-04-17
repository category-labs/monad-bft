use clap::error::ErrorKind;
use monad_consensus_types::signature_collection::SignatureCollection;
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_sdk::trace::TraceError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NodeSetupError {
    #[error(transparent)]
    Bls12_381(#[from] monad_bls::BlsError),

    #[error(transparent)]
    ChainConfigError(#[from] monad_chain_config::ChainConfigError),

    #[error(transparent)]
    ClapError(#[from] clap::Error),

    #[error("{msg}")]
    Custom { kind: ErrorKind, msg: String },

    #[error(transparent)]
    FromHexError(#[from] hex::FromHexError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    MetricsError(#[from] ExporterBuildError),

    #[error(transparent)]
    RayonPoolBuildError(#[from] rayon::ThreadPoolBuildError),

    #[error(transparent)]
    Secp256k1(#[from] monad_secp::Error),

    #[error(transparent)]
    SetGlobalDefaultError(#[from] tracing::subscriber::SetGlobalDefaultError),

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
    TraceError(#[from] TraceError),
}

impl NodeSetupError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            NodeSetupError::ClapError(e) => e.kind(),
            NodeSetupError::Custom { kind, msg: _ } => kind.to_owned(),
            NodeSetupError::IoError(_)
            | NodeSetupError::RayonPoolBuildError(_)
            | NodeSetupError::SetGlobalDefaultError(_) => ErrorKind::Io,
            NodeSetupError::ChainConfigError(_)
            | NodeSetupError::FromHexError(_)
            | NodeSetupError::Secp256k1(_)
            | NodeSetupError::Bls12_381(_)
            | NodeSetupError::SignatureCollectionError(_)
            | NodeSetupError::TomlDeError(_)
            | NodeSetupError::TraceError(_)
            | NodeSetupError::MetricsError(_) => ErrorKind::ValueValidation,
        }
    }
}
