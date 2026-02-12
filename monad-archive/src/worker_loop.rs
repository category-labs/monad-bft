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

use std::{ops::RangeInclusive, time::Duration};

use eyre::Result;
use futures::StreamExt;
use tokio::time::sleep;
use tracing::{error, info, warn};

pub struct ProcessRangeConfig {
    pub concurrency: usize,
    pub max_retries: u32,
    pub skip_failures: bool,
}

pub struct WorkerLoopConfig {
    pub max_blocks_per_iteration: u64,
    pub stop_block: Option<u64>,
    pub poll_interval: Duration,
    pub process: ProcessRangeConfig,
}

pub trait BlockRangeWorker {
    async fn get_checkpoint(&self) -> Result<u64>;

    async fn get_source_head(&self) -> Result<u64>;

    async fn process_block(&self, block_num: u64) -> Result<()>;

    async fn checkpoint(&self, block_num: u64);

    fn report_metrics(&self, _start: u64, _end: u64, _source_head: u64) {}
}

pub async fn process_block_range(
    worker: &(impl BlockRangeWorker + Sync),
    range: RangeInclusive<u64>,
    config: &ProcessRangeConfig,
) -> Option<u64> {
    let start = std::time::Instant::now();
    let mut failed_blocks: Vec<u64> = Vec::new();

    for attempt in 0..=config.max_retries {
        let blocks: Vec<u64> = if attempt == 0 {
            range.clone().collect()
        } else {
            info!(
                attempt,
                count = failed_blocks.len(),
                "Retrying failed blocks",
            );
            std::mem::take(&mut failed_blocks)
        };

        futures::stream::iter(blocks)
            .map(|block_num| async move {
                match worker.process_block(block_num).await {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        error!(block_num, "Failed to process block: {e:?}");
                        if config.skip_failures {
                            Ok(())
                        } else {
                            Err(block_num)
                        }
                    }
                }
            })
            .buffered(config.concurrency)
            .for_each(|result| {
                if let Err(block_num) = result {
                    failed_blocks.push(block_num);
                }
                futures::future::ready(())
            })
            .await;

        if failed_blocks.is_empty() {
            break;
        }
    }

    info!(
        elapsed = start.elapsed().as_millis(),
        start = range.start(),
        end = range.end(),
        "Finished processing range",
    );

    let new_latest = if failed_blocks.is_empty() {
        Some(*range.end())
    } else {
        let min_failed = *failed_blocks.iter().min().unwrap();
        error!(
            count = failed_blocks.len(),
            min_failed, "Blocks still failing after retries",
        );
        if min_failed == *range.start() {
            None
        } else {
            Some(min_failed - 1)
        }
    };

    if let Some(latest) = new_latest {
        worker.checkpoint(latest).await;
    }

    new_latest
}

pub async fn run_worker_loop(worker: impl BlockRangeWorker + Sync, config: WorkerLoopConfig) {
    let checkpoint = worker.get_checkpoint().await.unwrap_or(0);
    let mut start_block = if checkpoint == 0 { 0 } else { checkpoint + 1 };

    loop {
        let source_head = match worker.get_source_head().await {
            Ok(n) => n,
            Err(e) => {
                warn!("Error getting source head: {e:?}");
                sleep(config.poll_interval).await;
                continue;
            }
        };

        if let Some(stop) = config.stop_block {
            if start_block > stop {
                info!("Reached stop block, exiting.");
                return;
            }
        }

        let end_block = source_head.min(start_block + config.max_blocks_per_iteration - 1);

        if end_block < start_block {
            info!(start_block, end_block, "Nothing to process");
            sleep(config.poll_interval).await;
            continue;
        }

        worker.report_metrics(start_block, end_block, source_head);

        info!(
            start = start_block,
            end = end_block,
            source_head,
            "Processing block range",
        );

        match process_block_range(&worker, start_block..=end_block, &config.process).await {
            Some(latest_done) => start_block = latest_done + 1,
            None => sleep(config.poll_interval).await,
        }
    }
}
