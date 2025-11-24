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
use eyre::bail;
use monad_archive::{
    cli::set_source_and_sink_metrics, model::logs_index::LogsIndexArchiver, prelude::*,
    workers::index_worker::index_worker,
};
use tracing::{info, Level};

use crate::{migrate_capped::migrate_to_uncapped, migrate_logs::run_migrate_logs};

mod cli;
mod migrate_capped;
mod migrate_logs;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let mut args = cli::Cli::parse();
    info!(?args, "Cli Arguments: ");

    match std::mem::take(&mut args.command) {
        Some(cli::Commands::MigrateLogs) => run_migrate_logs(args).await,
        Some(cli::Commands::MigrateCapped {
            db_name,
            coll_name,
            batch_size,
            free_factor,
        }) => run_migrate_capped(db_name, coll_name, batch_size, free_factor, args).await,
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
    // Optional fallback
    let fallback_block_data_source = match args.fallback_block_data_source {
        Some(source) => Some(source.build(&metrics).await?),
        None => None,
    };
    let tx_index_archiver = args
        .archive_sink
        .build_index_archive(&metrics, args.max_inline_encoded_len)
        .await?;

    let log_index_archiver = match &tx_index_archiver.index_store {
        KVStoreErased::MongoDbStorage(_storage) => {
            if args.enable_logs_indexing {
                info!("Building log index archiver...");
                Some(
                    LogsIndexArchiver::from_tx_index_archiver(&tx_index_archiver, 50, false)
                        .await
                        .wrap_err("Failed to create log index reader")?,
                )
            } else {
                info!("eth_getLogs indexing is disabled");
                None
            }
        }
        _ => None,
    };

    // for testing
    if args.reset_index {
        tx_index_archiver.update_latest_indexed(0, false).await?;
    }

    // tokio main should not await futures directly, so we spawn a worker
    tokio::spawn(index_worker(
        block_data_reader,
        fallback_block_data_source,
        tx_index_archiver,
        log_index_archiver,
        args.max_blocks_per_iteration,
        args.max_concurrent_blocks,
        metrics,
        args.start_block,
        args.stop_block,
        Duration::from_millis(500),
        args.async_backfill,
    ))
    .await
    .map_err(Into::into)
}

async fn run_migrate_capped(
    db_name: String,
    coll_name: String,
    batch_size: u32,
    free_factor: f64,
    args: cli::Cli,
) -> Result<()> {
    let metrics = Metrics::none();

    let tx_index_archiver = args
        .archive_sink
        .build_index_archive(&metrics, args.max_inline_encoded_len)
        .await?;

    let mongodb_storage = match &tx_index_archiver.index_store {
        KVStoreErased::MongoDbStorage(storage) => storage,
        _ => bail!("migrate_capped requires MongoDB storage"),
    };

    let client = &mongodb_storage.client;
    migrate_to_uncapped(client, &db_name, &coll_name, batch_size, free_factor).await
}
