#![allow(async_fn_in_trait)]

use std::env;

use alloy_rpc_client::ClientBuilder;
use clap::Parser;
use prelude::*;
use tracing_subscriber::util::SubscriberInitExt;

pub mod cli;
pub mod generators;
pub mod prelude;
pub mod run;
pub mod shared;
pub mod workers;

#[tokio::main]
async fn main() {
    let config = cli::Config::parse();

    if let Err(e) = setup_logging(config.trace_log_file, config.debug_log_file) {
        error!("Error setting up logging: {e:?}");
    }

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

    if let Err(e) = run::run(client, config).await {
        error!("Fatal error: {e:?}");
    }
}

fn setup_logging(trace_log_file: bool, debug_log_file: bool) -> Result<()> {
    use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Layer};

    let trace_layer = if trace_log_file {
        Some(
            fmt::layer()
                .with_writer(std::fs::File::create("trace.log")?)
                .with_filter(EnvFilter::new("txgen=trace")),
        )
    } else {
        None
    };

    let debug_layer = if debug_log_file {
        Some(
            fmt::layer()
                .with_writer(std::fs::File::create("debug.log")?)
                .with_filter(EnvFilter::new("txgen=debug")),
        )
    } else {
        None
    };

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
