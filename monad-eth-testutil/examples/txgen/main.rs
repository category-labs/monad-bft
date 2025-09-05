// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#![allow(async_fn_in_trait, clippy::too_many_arguments)]

use std::env;

use alloy_rpc_client::ClientBuilder;
use clap::Parser;
use prelude::*;
use tracing_subscriber::util::SubscriberInitExt;

pub mod cli;
pub mod config;
pub mod generators;
pub mod prelude;
pub mod run;
pub mod shared;
pub mod workers;

#[tokio::main]
async fn main() {
    let cli_config = cli::CliConfig::parse();
    let config = if let Some(config_file) = cli_config.config_file {
        config::Config::from_file(&config_file).expect("Failed to load configuration")
    } else {
        cli_config.into()
    };

    if let Err(e) = setup_logging(config.trace_log_file, config.debug_log_file) {
        error!("Error setting up logging: {e:?}");
    }

    let rpc_urls = config.rpc_urls().expect("Invalid RPC URLs");
    let clients = rpc_urls
        .into_iter()
        .map(|url| ClientBuilder::default().http(url))
        .collect();

    info!("Config: {config:?}");

    // Check if the time to send txs from all senders is less than the refresh delay
    for workload_group in &config.workload_groups {
        for traffic_gen in &workload_group.traffic_gens {
            let time_to_send_txs_from_all_senders = (traffic_gen.tx_per_sender()
                * traffic_gen.senders()) as f64
                / traffic_gen.tps as f64;

            if time_to_send_txs_from_all_senders < config.refresh_delay_secs {
                warn!(
                    workload_group = workload_group.name,
                    traffic_gen = ?traffic_gen.gen_mode,
                    time_to_send_txs_from_all_senders,
                    refresh_delay = config.refresh_delay_secs,
                    "Not enough senders for given tps to prevent stall during refresh"
                );
            }
        }
    }

    if let Err(e) = run::run(clients, config).await {
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
