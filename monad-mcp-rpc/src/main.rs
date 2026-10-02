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

use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
use monad_mcp_rpc::{RpcSection, run};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    about = "Tx rpc beside an MCP validator: http in, udp to the leader out, ledger-confirmed"
)]
struct Args {
    /// toml file with an [rpc] table [default: --node-config]; flags override it
    #[arg(long)]
    config: Option<PathBuf>,
    /// listen address [default: 127.0.0.1:8080]
    #[arg(long)]
    http_addr: Option<String>,
    /// the colocated validator's node toml
    #[arg(long)]
    node_config: Option<PathBuf>,
    /// [default: the node's ledger.dir]
    #[arg(long)]
    ledger_dir: Option<PathBuf>,
    /// added to the node's proposing lead and delta [default: one slot interval]
    #[arg(long)]
    lead_margin_ms: Option<u64>,
    /// [default: 3000]
    #[arg(long)]
    resend_after_ms: Option<u64>,
    /// [default: 5]
    #[arg(long)]
    max_attempts: Option<u32>,
    /// [default: 10000]
    #[arg(long)]
    max_pending: Option<usize>,
    /// [default: 10000]
    #[arg(long)]
    retain_finished: Option<usize>,
    /// [default: 200]
    #[arg(long)]
    poll_interval_ms: Option<u64>,
    /// rtt matrix from deploy/latency.sh, relative to the node config's dir;
    /// unpinned txs go to the nearest proposer [default: by tenure]
    #[arg(long)]
    latency: Option<PathBuf>,
    /// tenure a nearest proposer must have left [default: 2]
    #[arg(long)]
    min_tenure_slots: Option<u64>,
}

#[actix_web::main]
async fn main() -> ExitCode {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    match serve(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("monad-mcp-rpc: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let file = match args.config.as_ref().or(args.node_config.as_ref()) {
        Some(path) => RpcSection::load(path)?,
        None => RpcSection::default(),
    };
    let flags = RpcSection {
        http_addr: args.http_addr,
        node_config: args.node_config,
        ledger_dir: args.ledger_dir,
        lead_margin_ms: args.lead_margin_ms,
        resend_after_ms: args.resend_after_ms,
        max_attempts: args.max_attempts,
        max_pending: args.max_pending,
        retain_finished: args.retain_finished,
        poll_interval_ms: args.poll_interval_ms,
        latency: args.latency,
        min_tenure_slots: args.min_tenure_slots,
    };
    run(file.merge(flags).build()?).await?;
    Ok(())
}
