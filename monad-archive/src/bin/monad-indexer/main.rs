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

use clap::Parser;
use monad_archive::{
    cli::set_source_and_sink_metrics, model::logs_index::LogsIndexArchiver, prelude::*,
    workers::index_worker::index_worker,
};
use tracing::{info, Level};

use crate::migrate_logs::run_migrate_logs;

mod cli;
mod migrate_logs;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args, "Cli Arguments: ");

    match args.command {
        Some(cli::Commands::MigrateLogs) => run_migrate_logs(args).await,
        None => run_indexer(args).await,
    }
}

async fn run_indexer(args: cli::Cli) -> Result<()> {
    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-indexer",
        args.otel_replica_name_override
            .unwrap_or_else(|| args.archive_sink.replica_name()),
        Duration::from_secs(15),
    )?;
    set_source_and_sink_metrics(&args.archive_sink, &args.block_data_source, &metrics);

    let block_data_reader = args.block_data_source.build(&metrics).await?;
    let tx_index_archiver = args
        .archive_sink
        .build_index_archive(&metrics, args.max_inline_encoded_len)
        .await?;

    let log_index_archiver = match &tx_index_archiver.index_store {
        KVStoreErased::MongoDbStorage(_storage) => {
            info!("Building log index archiver...");
            Some(
                LogsIndexArchiver::from_tx_index_archiver(&tx_index_archiver, 50, false)
                    .await
                    .wrap_err("Failed to create log index reader")?,
            )
        }
        _ => None,
    };
    info!("Log index archiver: {:?}", log_index_archiver.is_some());

    // for testing
    if args.reset_index {
        tx_index_archiver.update_latest_indexed(0).await?;
    }

    // tokio main should not await futures directly, so we spawn a worker
    tokio::spawn(index_worker(
        block_data_reader,
        tx_index_archiver,
        log_index_archiver,
        args.max_blocks_per_iteration,
        args.max_concurrent_blocks,
        metrics,
        args.start_block,
        args.stop_block,
        Duration::from_millis(500),
    ))
    .await
    .map_err(Into::into)
}
