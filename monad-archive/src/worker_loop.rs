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
use tokio::time::sleep;
use tracing::{info, warn};

pub struct WorkerLoopConfig {
    pub max_blocks_per_iteration: u64,
    pub stop_block: Option<u64>,
    pub poll_interval: Duration,
}

pub trait BlockRangeWorker {
    async fn get_checkpoint(&self) -> Result<u64>;

    async fn get_source_head(&self) -> Result<u64>;

    /// Process blocks in the given range. Returns the highest block number
    /// that was successfully processed.
    async fn process_range(&self, range: RangeInclusive<u64>) -> u64;

    fn report_metrics(&self, _start: u64, _end: u64, _source_head: u64) {}
}

pub async fn run_worker_loop(worker: impl BlockRangeWorker, config: WorkerLoopConfig) {
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

        let latest_done = worker.process_range(start_block..=end_block).await;

        start_block = if latest_done == 0 { 0 } else { latest_done + 1 };
    }
}
