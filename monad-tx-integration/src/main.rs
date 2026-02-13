use std::{net::SocketAddr, path::PathBuf};

use clap::{Args, Parser, Subcommand};

mod channel_input;
mod committer;
mod message;
mod multi_submit;
mod node;
mod router;
mod rpc;
mod stats;
mod submit;

#[derive(Parser)]
#[command(name = "monad-tx-integration")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Node(NodeArgs),
    Submit(SubmitArgs),
    MultiSubmit(MultiSubmitArgs),
}

#[derive(Args)]
pub struct NodeArgs {
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:0")]
    rpc_listen: String,

    #[arg(long, default_value = "1000")]
    commit_interval_ms: u64,

    #[arg(long, default_value = "node-stats.jsonl")]
    stats_file: PathBuf,

    #[arg(long, default_value = "1000")]
    num_accounts: usize,

    #[arg(long, default_value = "0")]
    duration_secs: u64,

    #[arg(long, default_value = "30")]
    ema_half_life_secs: u64,

    #[arg(long, default_value = "100.0")]
    promotion_threshold: f64,

    #[arg(long, default_value = "10")]
    max_sessions_per_ip: usize,

    #[arg(long, default_value = "10000")]
    low_watermark_sessions: usize,

    #[arg(long, default_value = "5000")]
    proposal_tx_limit: usize,

    #[arg(long, default_value = "200000000")]
    proposal_gas_limit: u64,

    #[arg(long, default_value = "2000000")]
    proposal_byte_limit: u64,

    /// IP rate limit window in ms for handshakes (0 = disabled)
    #[arg(long, default_value = "10000")]
    ip_rate_limit_ms: u64,

    /// Soft tx expiry in seconds (used when pool exceeds watermark)
    #[arg(long, default_value = "3600")]
    soft_tx_expiry_secs: u64,

    /// Hard tx expiry in seconds (used under normal conditions)
    #[arg(long, default_value = "7200")]
    hard_tx_expiry_secs: u64,
}

#[derive(Args)]
pub struct SubmitArgs {
    #[arg(long)]
    node_addr: SocketAddr,

    /// RPC address to query committed nonces (enables nonce gap recovery)
    #[arg(long)]
    rpc_addr: Option<String>,

    #[arg(long, default_value = "100")]
    tps: u64,

    #[arg(long, default_value = "0")]
    count: u64,

    #[arg(long, default_value = "0")]
    sender_index: usize,

    #[arg(long)]
    stats_file: Option<PathBuf>,

    #[arg(long, default_value = "0")]
    duration_secs: u64,

    /// max_fee_per_gas as a multiplier of MIN_BASE_FEE (e.g. 2 means 2*MIN_BASE_FEE)
    #[arg(long, default_value = "2")]
    max_fee_multiplier: u64,

    /// max_priority_fee_per_gas as a multiplier of MIN_BASE_FEE (e.g. 1 means 1*MIN_BASE_FEE)
    #[arg(long, default_value = "0")]
    priority_fee_multiplier: u64,

    /// Nonce sync interval in ms (how often to query committed nonce via RPC)
    #[arg(long, default_value = "1000")]
    nonce_sync_ms: u64,

    /// Number of transactions to batch per ForwardTxs message
    #[arg(long, default_value = "32")]
    batch_size: usize,

    /// Percentage of transactions to send with corrupted signatures (0-100)
    #[arg(long, default_value = "0")]
    invalid_sig_pct: u8,

    /// Skip a nonce every N transactions to create nonce gaps (0 = disabled)
    #[arg(long, default_value = "0")]
    nonce_gap_interval: u64,

    /// Send replacement transactions (same nonce 0, incrementing priority fee)
    #[arg(long, default_value_t = false)]
    replacement_mode: bool,
}

#[derive(Args)]
pub struct MultiSubmitArgs {
    #[arg(long)]
    node_addr: SocketAddr,

    /// RPC address to query committed nonces (enables nonce gap recovery)
    #[arg(long)]
    rpc_addr: Option<String>,

    #[arg(long, default_value = "5")]
    num_identities: usize,

    #[arg(long, default_value = "100")]
    tps_per_identity: u64,

    #[arg(long, default_value = "0")]
    sender_index_base: usize,

    #[arg(long, default_value = "30")]
    duration_secs: u64,

    /// max_fee_per_gas as a multiplier of MIN_BASE_FEE
    #[arg(long, default_value = "2")]
    max_fee_multiplier: u64,

    /// max_priority_fee_per_gas as a multiplier of MIN_BASE_FEE
    #[arg(long, default_value = "0")]
    priority_fee_multiplier: u64,

    /// Number of transactions to batch per ForwardTxs message
    #[arg(long, default_value = "32")]
    batch_size: usize,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Node(args) => node::run(args).await,
        Command::Submit(args) => submit::run(args).await,
        Command::MultiSubmit(args) => multi_submit::run(args).await,
    }
}
