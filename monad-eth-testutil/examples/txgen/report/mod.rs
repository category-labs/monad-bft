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

use std::sync::atomic::Ordering;

use chrono::{DateTime, Utc};
use eyre::Context;
use serde::{Deserialize, Serialize};

use crate::prelude::*;

pub mod join;
pub mod stats;

pub use join::*;
pub use stats::*;

impl Report {
    pub fn new(
        config: Config,
        workload_idx: usize,
        start_time: DateTime<Utc>,
        metrics: &Metrics,
    ) -> Self {
        let txs_sent = metrics.total_txs_sent.load(Ordering::Relaxed);
        let txs_committed = metrics.total_committed_txs.load(Ordering::Relaxed);
        let txs_dropped = txs_sent - txs_committed;
        let target_tps = config.workload_groups[workload_idx].traffic_gens[0].tps as usize;
        Self {
            start_time,
            end_time: Utc::now(),
            config,
            workload_idx,
            txs_sent,
            txs_committed,
            txs_dropped,
            target_tps,
            stats: HashMap::new(),
            stats_str: String::new(),
        }
    }

    pub async fn join_stats(&mut self, prom_url: Option<String>) -> Result<()> {
        let report = join_stats(prom_url, self.start_time, self.end_time)
            .await
            .wrap_err("Failed to join stats for Workload Group Report")?;
        self.stats = report.0;
        self.stats_str = report.1;
        Ok(())
    }

    pub fn to_json_file(&self, dir: &std::path::Path) -> Result<()> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(dir)
            .wrap_err_with(|| format!("Failed to create directory {}", dir.display()))?;

        let file_path = dir.join(format!(
            "{}-report-{}-{}.json",
            self.start_time.format("%Y%m%d"),
            self.workload_idx,
            self.end_time.format("%H%M%S")
        ));
        // Open file, truncate if it exists
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&file_path)
            .wrap_err_with(|| format!("Failed to open file {}", file_path.display()))?;
        // Write report to file
        serde_json::to_writer_pretty(file, self)
            .wrap_err_with(|| format!("Failed to write report to file {}", file_path.display()))?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    config: Config,
    workload_idx: usize,
    txs_sent: usize,
    txs_committed: usize,
    txs_dropped: usize,
    target_tps: usize,
    stats: HashMap<String, CounterStatsReport>,
    stats_str: String,
}
