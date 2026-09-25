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

use std::path::PathBuf;

use clap::Parser;
use monad_mcp_explorer::{ExplorerConfig, index::IndexConfig, loader::LoaderConfig, run};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(about = "Block explorer for the MCP ledger")]
struct Args {
    // the node's `ledger.dir`; only `<ledger_dir>/blocks` is read.
    #[arg(long)]
    ledger_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8090")]
    http_addr: String,
    // base url of monad-mcp-rpc, used by the page's send panel.
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    rpc_url: String,
    #[arg(long, default_value_t = IndexConfig::default().retain_slots, value_parser = positive)]
    retain_slots: usize,
    #[arg(long, default_value_t = IndexConfig::default().max_txs)]
    max_txs: usize,
    #[arg(long, default_value_t = 250)]
    tail_interval_ms: u64,
}

fn positive(s: &str) -> Result<usize, String> {
    match s.parse::<usize>() {
        Ok(0) => Err("must be at least 1".into()),
        Ok(n) => Ok(n),
        Err(e) => Err(e.to_string()),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let args = Args::parse();
    let loader = LoaderConfig {
        tail_interval: std::time::Duration::from_millis(args.tail_interval_ms),
        ..LoaderConfig::default()
    };
    run(ExplorerConfig {
        ledger_dir: args.ledger_dir,
        http_addr: args.http_addr,
        rpc_url: args.rpc_url.trim_end_matches('/').to_owned(),
        index: IndexConfig {
            retain_slots: args.retain_slots,
            max_txs: args.max_txs,
        },
        loader,
    })
    .await
}
