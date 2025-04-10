#![allow(async_fn_in_trait)]

use clap::Parser;
use monad_archive::{
    prelude::*,
    workers::{
        bft_block_archiver::bft_block_archive_worker, block_archive_worker::archive_worker,
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
        args.otel_endpoint,
        "monad-archiver",
        args.archive_sink.replica_name(),
        Duration::from_secs(15),
    )?;

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
        ));
    }

    if let Some(path) = args.wal_path {
        info!("Spawning wal checkpoint worker...");
        tokio::spawn(file_checkpoint_worker(
            archive_writer.store.clone(),
            path,
            "wal".to_owned(),
            Duration::from_secs(args.wal_checkpoint_freq_secs),
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
