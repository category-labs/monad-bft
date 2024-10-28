#![allow(async_fn_in_trait)]

use std::env;

use alloy_rpc_client::ClientBuilder;
use clap::{Parser, Subcommand, ValueEnum};
use prelude::*;
use serde::Deserialize;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;

// pub mod complex;
pub mod generators;
pub mod prelude;
pub mod run;
pub mod shared;
pub mod workers;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Config {
    #[arg(long, global = true, default_value = "http://localhost:8545")]
    pub rpc_url: Url,

    #[arg(long, global = true, default_value = "1000")]
    pub tps: u64,

    #[arg(
        long,
        global = true,
        default_values_t = [
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_string(),
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d".to_string(),
            "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a".to_string(),
            "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6".to_string(),
            "0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a".to_string(),
            "0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba".to_string(),
        ]
    )]
    pub root_private_keys: Vec<String>,

    #[arg(long, global = true, default_value = "10101")]
    pub recipient_seed: u64,

    #[arg(long, global = true, default_value = "10101")]
    pub sender_seed: u64,

    #[arg(long, global = true, default_value = "100000")]
    pub recipients: usize,

    #[arg(long, global = true)]
    pub senders: Option<usize>,

    #[arg(long, global = true, default_value = "5.")]
    pub refresh_delay_secs: f64,

    #[arg(long, global = true, default_value = "false")]
    pub erc20_balance_of: bool,

    #[command(subcommand)]
    pub generator_config: GeneratorConfig,

    #[arg(long, global = true)]
    sender_group_size: Option<usize>,

    #[clap(long, global = true)]
    tx_per_sender: Option<usize>,

    #[clap(long, global = true)]
    erc20_contract: Option<String>,
}

impl Config {
    pub fn tx_per_sender(&self) -> usize {
        use GeneratorConfig::*;
        if let Some(x) = self.tx_per_sender {
            return x;
        }
        match self.generator_config {
            FewToMany { .. } => 500,
            ManyToMany { .. }
            | Duplicates
            | RandomPriorityFee
            | HighCallData
            | NonDeterministicStorage
            | StorageDeletes => 10,
            NullGen => 0,
        }
    }

    pub fn sender_group_size(&self) -> usize {
        use GeneratorConfig::*;
        if let Some(x) = self.sender_group_size {
            return x;
        }
        match self.generator_config {
            FewToMany { .. } => 100,
            ManyToMany { .. }
            | Duplicates
            | RandomPriorityFee
            | HighCallData
            | NonDeterministicStorage
            | StorageDeletes => 100,
            NullGen => 10,
        }
    }

    pub fn senders(&self) -> usize {
        use GeneratorConfig::*;
        if let Some(x) = self.senders {
            return x;
        }
        match self.generator_config {
            FewToMany { .. } => 1000,
            ManyToMany { .. }
            | Duplicates
            | RandomPriorityFee
            | HighCallData
            | NonDeterministicStorage
            | StorageDeletes => 2500,
            NullGen => 100,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum GeneratorConfig {
    FewToMany {
        #[clap(long, default_value = "erc20")]
        tx_type: TxType,
    },
    ManyToMany {
        #[clap(long, default_value = "erc20")]
        tx_type: TxType,
    },
    Duplicates,
    RandomPriorityFee,
    HighCallData,
    NonDeterministicStorage,
    StorageDeletes,
    NullGen,
}

#[derive(Deserialize, Clone, Copy, Debug, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TxType {
    ERC20,
    Native,
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging()?;

    let config = Config::parse();
    let client: ReqwestClient = ClientBuilder::default().http(config.rpc_url.clone());

    info!("Config: {config:?}");

    let time_to_send_txs_from_all_senders =
        (config.tx_per_sender() * config.senders()) as f64 / config.tps as f64;
    if time_to_send_txs_from_all_senders < config.refresh_delay_secs {
        warn!(
            time_to_send_txs_from_all_senders,
            refresh_delay = config.refresh_delay_secs,
            "Not enough senders for given tps to prevent stall during refresh"
        );
    }

    run::run(client, config).await
}

fn setup_logging() -> Result<()> {
    use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Layer};
    let trace_layer = fmt::layer()
        .with_writer(std::fs::File::create("trace.log")?)
        .with_filter(EnvFilter::new("txgen=trace"));

    let debug_layer = fmt::layer()
        .with_writer(std::fs::File::create("debug.log")?)
        .with_filter(EnvFilter::new("txgen=debug"));

    let rust_log = env::var("RUST_LOG").unwrap_or("info".into());

    // log high signal aggregations to stdio
    let stdio_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_filter(EnvFilter::new(format!("txgen={rust_log}")));

    // set up subscriber with all layers
    tracing_subscriber::registry()
        .with(trace_layer)
        .with(debug_layer)
        .with(stdio_layer)
        .try_init()
        .map_err(Into::into)
}
