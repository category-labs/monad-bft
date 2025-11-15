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

#![allow(async_fn_in_trait)]

use monad_archive::{
    cli::set_source_and_sink_metrics,
    prelude::*,
    workers::{
        bft_archive_worker::bft_block_archive_worker, block_archive_worker::archive_worker,
        file_checkpointer::file_checkpoint_worker,
    },
};
use tracing::Level;

mod cli;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args, "Cli Arguments: ");

    let metrics = Metrics::new(
        args.otel_endpoint.clone(),
        "monad-archiver",
        args.otel_replica_name_override
            .clone()
            .unwrap_or_else(|| args.archive_sink.replica_name()),
        Duration::from_secs(15),
    )?;
    set_source_and_sink_metrics(&args.archive_sink, &args.block_data_source, &metrics);

    let archive_writer = args.archive_sink.build_block_data_archive(&metrics).await?;
    let block_data_source = args.block_data_source.build(&metrics).await?;

    // Optional fallback
    let fallback_block_data_source = match args.fallback_block_data_source {
        Some(source) => Some(source.build(&metrics).await?),
        None => None,
    };

    // Confirm connectivity
    if !args.skip_connectivity_check {
        block_data_source
            .get_latest(LatestKind::Uploaded)
            .await
            .wrap_err("Cannot connect to block data source")?;
        archive_writer
            .get_latest(LatestKind::Uploaded)
            .await
            .wrap_err("Cannot connect to archive sink")?;
    }

    if let Some(path) = args.bft_block_path {
        info!("Spawning bft block archive worker...");
        tokio::spawn(bft_block_archive_worker(
            archive_writer.store.clone(),
            path,
            Duration::from_secs(args.bft_block_poll_freq_secs),
            metrics.clone(),
            Some(Duration::from_secs(args.bft_block_min_age_secs)),
        ));
    }

    if let Some(path) = args.forkpoint_path {
        info!("Spawning forkpoint checkpoint worker...");
        tokio::spawn(file_checkpoint_worker(
            archive_writer.store.clone(),
            path,
            "forkpoint".to_owned(),
            Duration::from_secs(args.forkpoint_checkpoint_freq_secs),
        ));
    }

    for path in args.additional_files_to_checkpoint {
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let file_name = file_name.to_owned();
        info!("Spawning {} checkpoint worker...", &file_name,);
        tokio::spawn(file_checkpoint_worker(
            archive_writer.store.clone(),
            path,
            file_name,
            Duration::from_secs(args.additional_checkpoint_freq_secs),
        ));
    }

    tokio::spawn(archive_worker(
        block_data_source,
        fallback_block_data_source,
        archive_writer,
        args.max_blocks_per_iteration,
        args.max_concurrent_blocks,
        args.start_block,
        args.stop_block,
        args.unsafe_skip_bad_blocks,
        metrics,
    ))
    .await
    .map_err(Into::into)
}
