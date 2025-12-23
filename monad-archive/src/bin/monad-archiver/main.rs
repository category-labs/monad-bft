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

use monad_archive::{cli::set_source_and_sink_metrics, prelude::*};

mod bft_archive_worker;
mod block_archive_worker;
mod file_checkpointer;
mod generic_folder_archiver;

use bft_archive_worker::bft_block_archive_worker;
use block_archive_worker::{archive_worker, ArchiveWorkerOpts};
use cli::{Commands, ParsedCli};
use file_checkpointer::file_checkpoint_worker;
use generic_folder_archiver::recursive_dir_archiver;
use tokio::task::JoinHandle;
use tracing::Level;

mod cli;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let parsed = cli::Cli::parse();

    // Handle subcommands
    if let ParsedCli::Command(cmd) = parsed {
        return handle_command(cmd).await;
    }

    let ParsedCli::Daemon(args) = parsed else {
        unreachable!()
    };
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

    let mut worker_handles: Vec<JoinHandle<Result<()>>> = Vec::new();

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
        let handle = tokio::spawn(bft_block_archive_worker(
            archive_writer.store.clone(),
            path,
            Duration::from_secs(args.bft_block_poll_freq_secs),
            metrics.clone(),
            Some(Duration::from_secs(args.bft_block_min_age_secs)),
            args.bft_block_upload_concurrency,
        ));
        worker_handles.push(handle);
    }

    if let Some(path) = args.forkpoint_path {
        info!("Spawning forkpoint checkpoint worker...");
        let handle = tokio::spawn(file_checkpoint_worker(
            archive_writer.store.clone(),
            path,
            "forkpoint".to_owned(),
            Duration::from_secs(args.forkpoint_checkpoint_freq_secs),
        ));
        worker_handles.push(handle);
    }

    for path in args.additional_files_to_checkpoint {
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let file_name = file_name.to_owned();
        info!("Spawning {} checkpoint worker...", &file_name,);
        worker_handles.push(tokio::spawn(file_checkpoint_worker(
            archive_writer.store.clone(),
            path,
            file_name,
            Duration::from_secs(args.additional_checkpoint_freq_secs),
        )));
    }

    for path in args.additional_dirs_to_archive {
        info!(
            "Spawning {} folder archive worker...",
            &path.file_name().unwrap().to_string_lossy()
        );
        let handle = tokio::spawn(recursive_dir_archiver(
            archive_writer.store.clone(),
            path,
            Duration::from_millis((args.additional_dirs_archive_freq_secs * 1000.0) as u64),
            args.additional_dirs_exclude_prefix.clone(),
            metrics.clone(),
            Some(Duration::from_secs(1)),
            Duration::from_secs(60 * 60), // 1 hour hot TTL
        ));
        worker_handles.push(handle);
    }

    let archive_worker_opts = ArchiveWorkerOpts {
        max_blocks_per_iteration: args.max_blocks_per_iteration,
        max_concurrent_blocks: args.max_concurrent_blocks,
        stop_block: args.stop_block,
        unsafe_skip_bad_blocks: args.unsafe_skip_bad_blocks,
        require_traces: args.require_traces,
        traces_only: args.traces_only,
        async_backfill: args.async_backfill,
    };

    if !args.unsafe_disable_normal_archiving {
        tokio::spawn(archive_worker(
            block_data_source,
            fallback_block_data_source,
            archive_writer,
            archive_worker_opts,
            metrics,
        ))
        .await?;
    } else {
        info!("Normal archiving disabled, only running auxiliary workers");
    }

    for handle in worker_handles {
        handle.await??;
    }

    Ok(())
}

async fn handle_command(cmd: Commands) -> Result<()> {
    match cmd {
        Commands::SetStartBlock {
            block,
            archive_sink,
            async_backfill,
        } => {
            let metrics = Metrics::none();
            let archive = archive_sink.build_block_data_archive(&metrics).await?;

            let latest_kind = if async_backfill {
                LatestKind::UploadedAsyncBackfill
            } else {
                LatestKind::Uploaded
            };

            archive.update_latest(block, latest_kind).await?;

            let key_name = match latest_kind {
                LatestKind::Uploaded => "latest",
                LatestKind::UploadedAsyncBackfill => "latest_uploaded_async_backfill",
                _ => unreachable!(),
            };

            println!("Set latest marker: key=\"{key_name}\", block={block}");
            Ok(())
        }
    }
}
