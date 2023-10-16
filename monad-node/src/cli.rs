use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Cli {
    /// Set the certificate key path
    #[arg(long)]
    pub certkey: PathBuf,

    /// Set the node config path
    #[arg(long)]
    pub config: PathBuf,

    // Set the path where the execution ledger will be stored
    #[arg(long)]
    pub execution_ledger_path: PathBuf,

    /// Set the genesis config path
    #[arg(long)]
    pub genesis: PathBuf,

    /// Set the identity key path
    #[arg(long)]
    pub identity: PathBuf,

    /// Set a custom monad mempool ipc path
    #[arg(long)]
    pub mempool_ipc_path: Option<PathBuf>,

    /// Set the opentelemetry OTLP exporter endpoint
    #[arg(long)]
    pub otel_endpoint: Option<String>,
}
