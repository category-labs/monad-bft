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

use eyre::bail;
use monad_archive::{
    cli::set_source_and_sink_metrics, model::logs_index::LogsIndexArchiver, prelude::*,
};

mod index_worker;

use index_worker::index_worker;
use tracing::{info, Level};

use crate::{
    migrate_bft_archive::BftBlockIndex, migrate_capped::migrate_to_uncapped,
    migrate_logs::run_migrate_logs,
};

mod cli;
mod migrate_bft_archive;
mod migrate_capped;
mod migrate_logs;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    match cli::Cli::parse() {
        cli::ParsedCli::Command { command, args } => match command {
            cli::Commands::MigrateLogs {
                start_block,
                stop_block,
            } => run_migrate_logs(args.into_cli()?, start_block, stop_block).await,
            cli::Commands::MigrateCapped {
                db_name,
                coll_name,
                batch_size,
                free_factor,
            } => {
                let max_inline_encoded_len = args.max_inline_encoded_len;
                let archive_sink = match args.archive_sink {
                    Some(archive_sink) => archive_sink,
                    None => bail!("archive_sink must be provided"),
                };
                run_migrate_capped(
                    db_name,
                    coll_name,
                    batch_size,
                    free_factor,
                    archive_sink,
                    max_inline_encoded_len,
                )
                .await
            }
            cli::Commands::SetStartBlock {
                block,
                async_backfill,
            } => {
                let archive_sink = match args.archive_sink {
                    Some(archive_sink) => archive_sink,
                    None => bail!("archive_sink must be provided"),
                };
                run_set_start_block(block, archive_sink, async_backfill).await
            }
            cli::Commands::MigrateBftIndex {
                source,
                sink,
                concurrency,
                batch_size,
                no_copy_data,
            } => run_migrate_bft_index(source, sink, concurrency, batch_size, !no_copy_data).await,
        },
        cli::ParsedCli::Daemon(args) => {
            info!(?args, "Cli Arguments: ");
            run_indexer(args).await
        }
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
    archive_sink: monad_archive::cli::ArchiveArgs,
    max_inline_encoded_len: usize,
) -> Result<()> {
    let metrics = Metrics::none();

    let tx_index_archiver = archive_sink
        .build_index_archive(&metrics, max_inline_encoded_len)
        .await?;

    let mongodb_storage = match &tx_index_archiver.index_store {
        KVStoreErased::MongoDbStorage(storage) => storage,
        _ => bail!("migrate_capped requires MongoDB storage"),
    };

    let client = &mongodb_storage.client;
    migrate_to_uncapped(client, &db_name, &coll_name, batch_size, free_factor).await
}

async fn run_set_start_block(
    block: u64,
    archive_sink: monad_archive::cli::ArchiveArgs,
    async_backfill: bool,
) -> Result<()> {
    let metrics = Metrics::none();
    let archive = archive_sink.build_block_data_archive(&metrics).await?;

    let latest_kind = if async_backfill {
        LatestKind::IndexedAsyncBackfill
    } else {
        LatestKind::Indexed
    };

    archive.update_latest(block, latest_kind).await?;

    let key_name = match latest_kind {
        LatestKind::Indexed => "latest_indexed",
        LatestKind::IndexedAsyncBackfill => "latest_indexed_async_backfill",
        _ => unreachable!(),
    };

    println!("Set latest marker: key=\"{key_name}\", block={block}");
    Ok(())
}

async fn run_migrate_bft_index(
    source: monad_archive::cli::ArchiveArgs,
    sink: monad_archive::cli::ArchiveArgs,
    concurrency: usize,
    batch_size: usize,
    copy_data: bool,
) -> Result<()> {
    let metrics = Metrics::none();
    let source_archive = source.build_block_data_archive(&metrics).await?;
    let sink_archive = sink.build_block_data_archive(&metrics).await?;

    let indexer = BftBlockIndex::new(source_archive.store, sink_archive.store);
    indexer
        .index_bft_headers(concurrency, batch_size, copy_data)
        .await
}
