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

use std::{fmt, future::Future};

use monad_archive::{cli::set_source_and_sink_metrics, kvstore::WritePolicy, prelude::*};
use opentelemetry::KeyValue;

mod block_archive_worker;
mod file_checkpointer;
mod generic_folder_archiver;
mod ledger_archiver_and_indexer;

use block_archive_worker::{archive_worker, ArchiveWorkerOpts};
use cli::{Commands, ParsedCli};
use file_checkpointer::file_checkpoint_worker;
use generic_folder_archiver::recursive_dir_archiver;
use tokio::task::JoinSet;
use tracing::Level;

mod cli;

const SUPERVISOR_BASE_DELAY: Duration = Duration::from_millis(200);
const SUPERVISOR_MAX_DELAY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerFailure {
    Retryable,
    Fatal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnCleanExit {
    Restart,
    Return,
}

#[derive(Debug)]
struct FatalWorkerError {
    code: &'static str,
}

impl FatalWorkerError {
    const fn new(code: &'static str) -> Self {
        Self { code }
    }
}

impl fmt::Display for FatalWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fatal worker error: {}", self.code)
    }
}

impl std::error::Error for FatalWorkerError {}

fn fatal_worker_err(code: &'static str) -> Report {
    Report::new(FatalWorkerError::new(code))
}

fn worker_attrs(
    worker: &'static str,
    stage: &'static str,
    operation: &'static str,
) -> [KeyValue; 3] {
    [
        KeyValue::new("worker", worker),
        KeyValue::new("stage", stage),
        KeyValue::new("operation", operation),
    ]
}

fn classify_worker_error(err: &Report) -> WorkerFailure {
    let msg = format!("{err:#}");
    let fatal_markers = [
        "fatal invariant violation",
        "finalized chain mismatch",
        "Marker key collision",
    ];

    if err
        .chain()
        .any(|cause| cause.downcast_ref::<FatalWorkerError>().is_some())
        || fatal_markers.iter().any(|marker| msg.contains(marker))
    {
        WorkerFailure::Fatal
    } else {
        WorkerFailure::Retryable
    }
}

fn jittered_backoff_delay(base_delay: Duration, max_delay: Duration, attempts: u64) -> Duration {
    let base_ms = base_delay.as_millis().max(1) as u64;
    let max_ms = max_delay.as_millis().max(base_ms as u128) as u64;
    let exponent = attempts.saturating_sub(1).min(16);
    let exp_ms = base_ms.saturating_mul(1_u64 << exponent).min(max_ms);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let jitter_pct = (nanos % 41) as i32 - 20;
    let jittered_ms = if jitter_pct >= 0 {
        exp_ms.saturating_add(exp_ms.saturating_mul(jitter_pct as u64) / 100)
    } else {
        exp_ms.saturating_sub(exp_ms.saturating_mul((-jitter_pct) as u64) / 100)
    };
    Duration::from_millis(jittered_ms.max(1))
}

async fn run_supervised_worker<F, Fut>(
    worker_name: &'static str,
    metrics: Metrics,
    on_clean_exit: OnCleanExit,
    mut make_worker: F,
) -> Result<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let mut restart_attempts = 0_u64;
    metrics.periodic_gauge_with_attrs(
        MetricNames::WORKER_HEALTH_STATUS,
        1,
        vec![
            KeyValue::new("worker", worker_name),
            KeyValue::new("stage", "supervisor"),
        ],
    );

    loop {
        let run_result = make_worker().await;
        match run_result {
            Ok(()) => match on_clean_exit {
                OnCleanExit::Return => {
                    info!(worker = worker_name, "Worker completed successfully");
                    return Ok(());
                }
                OnCleanExit::Restart => {
                    warn!(
                        worker = worker_name,
                        "Worker exited unexpectedly; restarting"
                    );
                }
            },
            Err(err) => match classify_worker_error(&err) {
                WorkerFailure::Fatal => {
                    metrics.periodic_gauge_with_attrs(
                        MetricNames::WORKER_HEALTH_STATUS,
                        0,
                        vec![
                            KeyValue::new("worker", worker_name),
                            KeyValue::new("stage", "supervisor"),
                        ],
                    );
                    return Err(
                        err.wrap_err(format!("Worker {worker_name} failed with fatal error"))
                    );
                }
                WorkerFailure::Retryable => {
                    error!(
                        ?err,
                        worker = worker_name,
                        "Worker failed with retryable error"
                    );
                }
            },
        }

        restart_attempts = restart_attempts.saturating_add(1);
        let backoff = jittered_backoff_delay(
            SUPERVISOR_BASE_DELAY,
            SUPERVISOR_MAX_DELAY,
            restart_attempts,
        );
        let restart_attrs = worker_attrs(worker_name, "supervisor", "restart");
        metrics.counter_with_attrs(MetricNames::WORKER_RESTARTS, 1, &restart_attrs);
        metrics.counter_with_attrs(MetricNames::WORKER_RETRY_ATTEMPTS, 1, &restart_attrs);
        metrics.periodic_gauge_with_attrs(
            MetricNames::WORKER_HEALTH_STATUS,
            0,
            vec![
                KeyValue::new("worker", worker_name),
                KeyValue::new("stage", "supervisor"),
            ],
        );
        metrics.gauge_with_attrs(
            MetricNames::WORKER_STALLED_SECONDS,
            backoff.as_secs(),
            &restart_attrs,
        );
        tokio::time::sleep(backoff).await;
        metrics.periodic_gauge_with_attrs(
            MetricNames::WORKER_HEALTH_STATUS,
            1,
            vec![
                KeyValue::new("worker", worker_name),
                KeyValue::new("stage", "supervisor"),
            ],
        );
    }
}

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

    let mut workers: JoinSet<Result<()>> = JoinSet::new();

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
        let model =
            monad_archive::model::bft_ledger::BftBlockModel::new(archive_writer.store.clone());
        let poll_frequency = Duration::from_secs(args.bft_block_poll_freq_secs);
        let worker_metrics = metrics.clone();
        workers.spawn(run_supervised_worker(
            "bft_archive_worker",
            metrics.clone(),
            OnCleanExit::Restart,
            move || {
                ledger_archiver_and_indexer::bft_archive_worker(
                    model.clone(),
                    path.clone(),
                    poll_frequency,
                    worker_metrics.clone(),
                )
            },
        ));
    }

    if let Some(path) = args.forkpoint_path {
        info!("Spawning forkpoint checkpoint worker...");
        let poll_frequency = Duration::from_secs(args.forkpoint_checkpoint_freq_secs);
        let store = archive_writer.store.clone();
        let blob_prefix = "forkpoint".to_owned();
        workers.spawn(run_supervised_worker(
            "forkpoint_checkpoint_worker",
            metrics.clone(),
            OnCleanExit::Restart,
            move || {
                file_checkpoint_worker(
                    store.clone(),
                    path.clone(),
                    blob_prefix.clone(),
                    poll_frequency,
                )
            },
        ));
    }

    for path in args.additional_files_to_checkpoint {
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let file_name = file_name.to_owned();
        info!("Spawning {} checkpoint worker...", &file_name,);
        let store = archive_writer.store.clone();
        let poll_frequency = Duration::from_secs(args.additional_checkpoint_freq_secs);
        workers.spawn(run_supervised_worker(
            "additional_checkpoint_worker",
            metrics.clone(),
            OnCleanExit::Restart,
            move || {
                file_checkpoint_worker(
                    store.clone(),
                    path.clone(),
                    file_name.clone(),
                    poll_frequency,
                )
            },
        ));
    }

    for path in args.additional_dirs_to_archive {
        info!(
            "Spawning {} folder archive worker...",
            &path.file_name().unwrap().to_string_lossy()
        );
        let store = archive_writer.store.clone();
        let poll_frequency =
            Duration::from_millis((args.additional_dirs_archive_freq_secs * 1000.0) as u64);
        let exclude_prefix = args.additional_dirs_exclude_prefix.clone();
        let worker_metrics = metrics.clone();
        workers.spawn(run_supervised_worker(
            "additional_dir_archiver_worker",
            metrics.clone(),
            OnCleanExit::Restart,
            move || {
                recursive_dir_archiver(
                    store.clone(),
                    path.clone(),
                    poll_frequency,
                    exclude_prefix.clone(),
                    worker_metrics.clone(),
                    Some(Duration::from_secs(1)),
                    Duration::from_secs(60 * 60), // 1 hour hot TTL
                )
            },
        ));
    }

    let archive_worker_opts = ArchiveWorkerOpts {
        max_blocks_per_iteration: args.max_blocks_per_iteration,
        max_concurrent_blocks: args.max_concurrent_blocks,
        stop_block: args.stop_block,
        unsafe_skip_bad_blocks: args.unsafe_skip_bad_blocks,
        require_traces: args.require_traces,
        traces_only: args.traces_only,
        async_backfill: args.async_backfill,
        blocks_write_policy: if args.unsafe_allow_overwrite || args.unsafe_allow_blocks_overwrite {
            WritePolicy::AllowOverwrite
        } else {
            WritePolicy::NoClobber
        },
        receipts_write_policy: if args.unsafe_allow_overwrite
            || args.unsafe_allow_receipts_overwrite
        {
            WritePolicy::AllowOverwrite
        } else {
            WritePolicy::NoClobber
        },
        traces_write_policy: if args.unsafe_allow_overwrite || args.unsafe_allow_traces_overwrite {
            WritePolicy::AllowOverwrite
        } else {
            WritePolicy::NoClobber
        },
    };

    if !args.unsafe_disable_normal_archiving {
        let block_data_source = block_data_source.clone();
        let fallback_block_data_source = fallback_block_data_source.clone();
        let archive_writer = archive_writer.clone();
        let worker_metrics = metrics.clone();
        let archive_worker_stop_block = archive_worker_opts.stop_block;
        workers.spawn(run_supervised_worker(
            "archive_worker",
            metrics.clone(),
            OnCleanExit::Return,
            move || {
                let block_data_source = block_data_source.clone();
                let fallback_block_data_source = fallback_block_data_source.clone();
                let archive_writer = archive_writer.clone();
                let archive_worker_opts = archive_worker_opts.clone();
                let worker_metrics = worker_metrics.clone();
                async move {
                    archive_worker(
                        block_data_source,
                        fallback_block_data_source,
                        archive_writer,
                        archive_worker_opts,
                        worker_metrics,
                    )
                    .await?;

                    if archive_worker_stop_block.is_some() {
                        Ok(())
                    } else {
                        Err(fatal_worker_err("archive_worker_unexpected_exit"))
                    }
                }
            },
        ));
    } else {
        info!("Normal archiving disabled, only running auxiliary workers");
    }

    if workers.is_empty() {
        info!("No workers were configured to run");
        return Ok(());
    }

    while let Some(joined) = workers.join_next().await {
        match joined {
            Ok(Ok(())) => {
                warn!("A worker exited cleanly; continuing to monitor remaining workers");
            }
            Ok(Err(err)) => return Err(err),
            Err(join_err) => return Err(join_err.into()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_worker_error_marks_typed_fatal() {
        let err = fatal_worker_err("test_fatal");
        assert_eq!(classify_worker_error(&err), WorkerFailure::Fatal);
    }

    #[test]
    fn classify_worker_error_marks_other_errors_retryable() {
        let err = eyre::eyre!("temporary failure");
        assert_eq!(classify_worker_error(&err), WorkerFailure::Retryable);
    }

    #[test]
    fn classify_worker_error_marks_legacy_fatal_marker() {
        let err = eyre::eyre!("fatal invariant violation: finalized chain mismatch");
        assert_eq!(classify_worker_error(&err), WorkerFailure::Fatal);
    }
}
