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

    /// Set the validators config path
    #[arg(long)]
    pub validators_path: PathBuf,

    /// Set devnet chain config override path
    #[arg(long)]
    pub devnet_chain_config_override: Option<PathBuf>,

    /// Set the genesis config path
    #[arg(long)]
    pub genesis_path: PathBuf,

    /// Set the path where the write-ahead log will be stored
    #[arg(long)]
    pub wal_path: PathBuf,

    /// Set the path where consensus blocks will be stored
    #[arg(long)]
    pub ledger_path: PathBuf,

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

    /// Set the sq_thread_cpu for statesync client. None means SQPOLL mode is
    /// disabled
    #[arg(long)]
    pub statesync_sq_thread_cpu: Option<u32>,

    /// Set the opentelemetry OTLP exporter endpoint
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// Set the password for decrypting keystore file
    /// Default to empty string
    #[arg(long)]
    pub keystore_password: Option<String>,

    /// Set the time interval for metrics collection
    #[arg(long, requires = "otel_endpoint")]
    pub record_metrics_interval_seconds: Option<u64>,
}
