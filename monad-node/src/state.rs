use std::path::PathBuf;

use clap::{error::ErrorKind, FromArgMatches};
use monad_consensus_types::voting::ValidatorMapping;
use monad_crypto::secp256k1::KeyPair;
use monad_types::NodeId;
use opentelemetry::trace::{Span, TraceContextExt, Tracer, TracerProvider};
use opentelemetry_otlp::WithExportConfig;

use crate::{
    cli::Cli,
    config::{NodeBootstrapConfig, NodeConfig},
    error::NodeSetupError,
    genesis::GenesisState,
};

pub struct NodeState {
    pub config: NodeConfig,
    pub genesis: GenesisState,
    pub val_mapping: ValidatorMapping<KeyPair>,

    pub identity: KeyPair,
    pub certkey: KeyPair,

    pub execution_ledger_path: PathBuf,
    pub mempool_ipc_path: Option<PathBuf>,
    pub otel_context: Option<opentelemetry::Context>,
}

impl NodeState {
    pub fn setup(cmd: &mut clap::Command) -> Result<Self, NodeSetupError> {
        let cli = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut())?;

        let config: NodeConfig = toml::from_str(&std::fs::read_to_string(cli.config)?)?;

        let val_mapping = build_validator_mapping(&config.bootstrap);

        let genesis = GenesisState::setup(cli.genesis, &val_mapping)?;

        let identity = load_secp256k1_keypair_from_ec_pem(cli.identity)?;
        let certkey = load_secp256k1_keypair_from_ec_pem(cli.certkey)?;

        let otel_context = if let Some(otel_endpoint) = cli.otel_endpoint {
            Some(build_otel_context(otel_endpoint)?)
        } else {
            None
        };

        Ok(Self {
            config,
            genesis,
            val_mapping,

            identity,
            certkey,

            execution_ledger_path: cli.execution_ledger_path,
            mempool_ipc_path: cli.mempool_ipc_path,
            otel_context,
        })
    }
}

fn build_validator_mapping(bootstrap_config: &NodeBootstrapConfig) -> ValidatorMapping<KeyPair> {
    let voting_identities = bootstrap_config
        .peers
        .iter()
        .map(|peer| (NodeId(peer.pubkey), peer.certkey))
        .collect::<Vec<_>>();

    ValidatorMapping::new(voting_identities)
}

fn load_secp256k1_keypair_from_ec_pem(path: PathBuf) -> Result<KeyPair, NodeSetupError> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);

    let mut ec_private_keys = rustls_pemfile::ec_private_keys(&mut reader)?.into_iter();

    let Some(private_key_bytes) = ec_private_keys.next() else {
        return Err(NodeSetupError::Custom {
            kind: ErrorKind::ValueValidation,
            msg: "pem file does not contain \"EC PRIVATE KEY\" tag".to_owned(),
        });
    };

    if ec_private_keys.next().is_some() {
        return Err(NodeSetupError::Custom {
            kind: ErrorKind::ValueValidation,
            msg: "pem file contains more than one \"EC PRIVATE KEY\" tag".to_owned(),
        });
    }

    Ok(KeyPair::from_der(private_key_bytes)?)
}

fn build_otel_context(otel_endpoint: String) -> Result<opentelemetry::Context, NodeSetupError> {
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
                "monad-coordinator",
            )]),
        ))
        .with_batch_exporter(exporter, opentelemetry::runtime::Tokio);

    let provider = provider_builder.build();

    let tracer = provider.tracer("opentelemetry");
    let span = tracer.start("exec");

    Ok(opentelemetry::Context::default().with_remote_span_context(span.span_context().clone()))
}
