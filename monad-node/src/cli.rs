use std::path::PathBuf;

use clap::Parser;

use crate::mode::RunModeCommand;

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

    // Set the path where the write-ahead log will be stored
    #[arg(long)]
    pub wal_path: PathBuf,

    // Set the path where the execution ledger will be stored
    #[arg(long)]
    pub execution_ledger_path: PathBuf,
    //
    // Set the path where the blockdb will be stored
    #[arg(long)]
    pub blockdb_path: PathBuf,

    /// Set a custom monad mempool ipc path
    #[arg(long)]
    pub mempool_ipc_path: PathBuf,

    /// Set the monad triedb path
    #[arg(long)]
    pub triedb_path: PathBuf,

    /// Set a custom monad control panel ipc path
    #[arg(long)]
    pub control_panel_ipc_path: PathBuf,

    /// Set the opentelemetry OTLP exporter endpoint
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// Set the password for decrypting keystore file
    /// Default to empty string
    #[arg(long)]
    pub keystore_password: Option<String>,

    /// Default to run in prod mode
    /// Passing "testmode" enables test only arguments
    #[command(subcommand)]
    pub run_mode: Option<RunModeCommand>,

    #[arg(long, requires = "otel_endpoint")]
    pub record_metrics_interval_seconds: Option<u64>,
}
