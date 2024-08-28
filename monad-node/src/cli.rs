use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Cli {
    /// Set the bls12_381 secret key path
    #[arg(long)]
    pub bls_identity: PathBuf,

    /// Set the secp256k1 key path
    #[arg(long)]
    pub secp_identity: PathBuf,

    /// Set the node config path
    #[arg(long)]
    pub node_config: PathBuf,

    /// Set the forkpoint config path
    #[arg(long)]
    pub forkpoint_config: PathBuf,

    /// Set the genesis config path
    #[arg(long)]
    pub genesis_path: PathBuf,

    /// Set the path where the write-ahead log will be stored
    #[arg(long)]
    pub wal_path: PathBuf,

    /// Set the path where the execution ledger will be stored
    #[arg(long)]
    pub execution_ledger_path: PathBuf,

    /// Set the path where the bft block headers will be stored
    #[arg(long)]
    pub bft_block_header_path: PathBuf,

    /// Set the path where the bft block payloads will be stored
    #[arg(long)]
    pub bft_block_payload_path: PathBuf,

    /// Set a custom monad mempool ipc path
    #[arg(long)]
    pub mempool_ipc_path: PathBuf,

    /// Set the monad triedb path
    #[arg(long)]
    pub triedb_path: PathBuf,

    /// Set a custom monad control panel ipc path
    #[arg(long)]
    pub control_panel_ipc_path: PathBuf,

    /// Set a custom monad statesync ipc path
    #[arg(long)]
    pub statesync_ipc_path: PathBuf,

    /// Set the opentelemetry OTLP exporter endpoint
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// Set the password for decrypting keystore file
    /// Default to empty string
    #[arg(long)]
    pub keystore_password: Option<String>,

    /// Turn otel trace collection on
    #[arg(long, requires = "otel_endpoint")]
    pub record_otel_traces: bool,

    /// Set the time interval for metrics collection
    #[arg(long, requires = "otel_endpoint")]
    pub record_metrics_interval_seconds: Option<u64>,
}
