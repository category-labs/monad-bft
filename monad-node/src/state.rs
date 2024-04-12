use std::{path::PathBuf, time::Duration};

use base64::Engine;
use clap::{error::ErrorKind, FromArgMatches};
use log::info;
use monad_bls::BlsKeyPair;
use monad_secp::KeyPair;
use opentelemetry_otlp::WithExportConfig;
use zeroize::Zeroize;

use crate::{
    cli::Cli,
    config::{GenesisConfig, NodeConfig},
    error::NodeSetupError,
    mode::RunModeCommand,
};

pub struct NodeState {
    pub node_config: NodeConfig,
    pub genesis_config: GenesisConfig,

    pub secp256k1_identity: KeyPair,
    pub gossip_identity: KeyPair,
    pub bls12_381_identity: BlsKeyPair,

    pub wal_path: PathBuf,
    pub execution_ledger_path: PathBuf,
    pub mempool_ipc_path: PathBuf,
    pub blockdb_path: PathBuf,
    pub otel_endpoint: Option<String>,
    pub record_metrics_interval: Option<Duration>,

    pub run_mode: RunModeCommand,
}

impl NodeState {
    pub fn setup(cmd: &mut clap::Command) -> Result<Self, NodeSetupError> {
        let cli = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut())?;

        let secp_key = load_secp256k1_keypair(&cli.secp_identity)?;
        info!(
            "Loaded secp256k1 key from {:?}, pubkey=0x{}",
            &cli.secp_identity,
            hex::encode(secp_key.pubkey().bytes_compressed())
        );
        // FIXME this is somewhat jank.. is there a better way?
        let gossip_key = load_secp256k1_keypair(&cli.secp_identity)?;
        info!(
            "Loaded gossip key from {:?}, pubkey=0x{}",
            &cli.secp_identity,
            hex::encode(gossip_key.pubkey().bytes_compressed())
        );
        let bls_key = load_bls12_381_keypair(&cli.bls_identity)?;
        info!(
            "Loaded bls12_381 key from {:?}, pubkey=0x{}",
            &cli.bls_identity,
            hex::encode(bls_key.pubkey().compress())
        );

        let node_config: NodeConfig = toml::from_str(&std::fs::read_to_string(cli.node_config)?)?;
        let genesis_config: GenesisConfig =
            toml::from_str(&std::fs::read_to_string(cli.genesis_config)?)?;

        let run_mode = cli.run_mode.unwrap_or_default();

        Ok(Self {
            node_config,
            genesis_config,

            secp256k1_identity: secp_key,
            gossip_identity: gossip_key,
            bls12_381_identity: bls_key,

            wal_path: cli.wal_path,
            execution_ledger_path: cli.execution_ledger_path,
            blockdb_path: cli.blockdb_path,
            mempool_ipc_path: cli.mempool_ipc_path,
            otel_endpoint: cli.otel_endpoint,
            record_metrics_interval: cli
                .record_metrics_interval_seconds
                .and_then(|s| Some(Duration::from_secs(s))),

            run_mode,
        })
    }
}

fn load_secp256k1_keypair(path: &PathBuf) -> Result<KeyPair, NodeSetupError> {
    let mut b64 = std::fs::read_to_string(path)?;
    let mut secret = Vec::with_capacity(32);
    let result = base64::engine::general_purpose::STANDARD.decode_vec(b64.trim_end(), &mut secret);
    b64.zeroize();
    if result.is_ok() && secret.len() == 32 {
        return Ok(KeyPair::from_bytes(&mut secret)?);
    }
    Err(NodeSetupError::Custom {
        kind: ErrorKind::ValueValidation,
        msg: "secp secret must be base64-encoded 32 bytes".to_owned(),
    })
}

fn load_bls12_381_keypair(path: &PathBuf) -> Result<BlsKeyPair, NodeSetupError> {
    let mut b64 = std::fs::read_to_string(path)?;
    let mut secret = Vec::with_capacity(32);
    let result = base64::engine::general_purpose::STANDARD.decode_vec(b64.trim_end(), &mut secret);
    b64.zeroize();
    if result.is_ok() && secret.len() == 32 {
        return Ok(BlsKeyPair::from_bytes(&mut secret)?);
    }
    Err(NodeSetupError::Custom {
        kind: ErrorKind::ValueValidation,
        msg: "bls secret must be base64-encoded 32 bytes".to_owned(),
    })
}

pub fn build_otel_provider(
    otel_endpoint: &str,
    service_name: String,
) -> Result<opentelemetry::sdk::trace::TracerProvider, NodeSetupError> {
    let exporter = opentelemetry_otlp::SpanExporterBuilder::Tonic(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(otel_endpoint),
    )
    .build_span_exporter()?;

    let provider_builder = opentelemetry::sdk::trace::TracerProvider::builder()
        .with_config(opentelemetry::sdk::trace::config().with_resource(
            opentelemetry::sdk::Resource::new(vec![opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name,
            )]),
        ))
        .with_batch_exporter(exporter, opentelemetry::runtime::Tokio);

    Ok(provider_builder.build())
}
