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

//! Monad Archive Checker System
//!
//! This module implements a blockchain data consistency checker that verifies
//! blocks, receipts, and traces across multiple archival replicas. It detects
//! and reports inconsistencies, missing data, and corrupted blocks.

use clap::Parser;
use eyre::Result;
use model::CheckerModel;
use monad_archive::{cli::get_aws_config, prelude::*};
use tracing_subscriber::EnvFilter;

mod checker;
mod cli;
mod fault_fixer;
mod inspector;
mod model;
mod rechecker_v2;

/// Number of blocks to check per iteration and the number of blocks per stored object.
/// This value determines the granularity of fault detection and storage.
/// A chunk is the smallest unit of blocks that can be rechecked or have faults cleared.
pub const CHUNK_SIZE: u64 = 1000;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive("monad_archive_checker=debug".parse()?)
                .from_env_lossy(),
        )
        .init();

    // Parse command line arguments
    let args = cli::Cli::parse();
    info!("Starting monad-archive-checker with mode: {:?}", args.mode);

    if let Some(max_compute_threads) = args.max_compute_threads.as_ref() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(*max_compute_threads)
            .build_global()?;
    }

    // Initialize metrics
    info!(
        "Initializing metrics with endpoint: {:?}",
        args.otel_endpoint
    );
    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad_archive_checker",
        "",
        Duration::from_secs(15),
    )?;

    // Get AWS configuration
    info!("Configuring AWS with region: {:?}", args.region);
    let aws_config = get_aws_config(args.region.clone(), 30).await;

    // Initialize S3 bucket
    info!("Initializing S3 bucket: {}", args.bucket);
    let s3 = Bucket::new(args.bucket.clone(), &aws_config, metrics.clone());

    match args.mode {
        cli::Mode::Checker(checker_args) => {
            info!(
                "Starting in checker mode with min_lag_from_tip: {}",
                checker_args.min_lag_from_tip
            );

            // Create the checker model and synchonize replicas to check
            let init_replicas = checker_args.init_replicas.map(|list| {
                info!("Using initial replicas: {:?}", list);
                list.into_iter().collect()
            });

            info!("Initializing checker model");
            let model = CheckerModel::new(s3, &metrics, init_replicas).await?;
            let recheck_freq = Duration::from_secs_f64(checker_args.recheck_freq_min * 60.);
            info!("Recheck frequency set to {:?}", recheck_freq);

            // Start the checker worker task
            let checker_handle = tokio::spawn(checker::checker_worker(
                model.clone(),
                checker_args.min_lag_from_tip,
                metrics.clone(),
                checker_args.concurrency,
            ));

            // Start the rechecker worker if enabled
            let rechecker_handle = if !checker_args.disable_rechecker {
                info!("Starting rechecker worker (runs alongside checker)");
                tokio::spawn(rechecker_v2::rechecker_v2_worker(
                    recheck_freq,
                    model.clone(),
                    metrics.clone(),
                    checker_args.concurrency,
                ))
            } else {
                // Dummy task for type compatibility
                tokio::spawn(async { Ok(()) })
            };

            checker_handle.await??;
            rechecker_handle.await??;
        }
        cli::Mode::Rechecker(rechecker_args) => {
            info!(
                "Starting in rechecker mode with recheck_freq_min: {}, dry_run: {}, worker: {}, start: {:?}, end: {:?}, force_recheck: {}",
                rechecker_args.recheck_freq_min, rechecker_args.dry_run, rechecker_args.worker, rechecker_args.start, rechecker_args.end, rechecker_args.force_recheck
            );
            let model = CheckerModel::new(s3, &metrics, None).await?;
            let recheck_freq = Duration::from_secs_f64(rechecker_args.recheck_freq_min * 60.);

            tokio::spawn(rechecker_v2::rechecker_v2_standalone(
                recheck_freq,
                model,
                metrics,
                rechecker_args.dry_run,
                rechecker_args.start,
                rechecker_args.end,
                rechecker_args.force_recheck,
                rechecker_args.worker,
                rechecker_args.concurrency,
            ))
            .await??;
        }
        cli::Mode::FaultFixer(fixer_args) => {
            info!(
                "Starting in fault fixer mode [commit_changes: {}, verify: {}]",
                fixer_args.commit_changes, fixer_args.verify
            );

            let model = CheckerModel::new(s3, &metrics, None).await?;

            info!("Running fault fixer on replicas: {:?}", fixer_args.replicas);

            let (total_fixed, total_failed) = fault_fixer::run_fixer(
                &model,
                &metrics,
                !fixer_args.commit_changes,
                fixer_args.verify,
                fixer_args.replicas,
                fixer_args.start,
                fixer_args.end,
                fixer_args.concurrency,
            )
            .await?;

            if !fixer_args.commit_changes {
                info!(
                    "DRY RUN SUMMARY: Would fix {} faults ({} would fail)",
                    total_fixed, total_failed
                );
            } else {
                info!(
                    "SUMMARY: Fixed {} faults ({} failed)",
                    total_fixed, total_failed
                );
            }
        }
        cli::Mode::Inspector(inspector_args) => {
            info!("Starting in inspector mode");

            let model = CheckerModel::new(s3, &metrics, None).await?;

            match inspector_args.command {
                cli::InspectorCommand::Status => {
                    info!("Displaying checker status");
                    inspector::status(&model).await?;
                }
                cli::InspectorCommand::ListFaults {
                    summary,
                    start,
                    end,
                    replica,
                } => {
                    info!("Listing all fault ranges (summary: {})", summary);
                    inspector::list_fault_ranges(&model, summary, start, end, replica).await?;
                }
                cli::InspectorCommand::InspectBlock {
                    block_num,
                    format,
                    raw,
                } => {
                    info!("Inspecting block {} with format {:?}", block_num, format);
                    inspector::inspect_block(&model, block_num, format, raw).await?;
                }
            }
        }
    }

    info!("monad-archive-checker completed successfully");
    Ok(())
}
