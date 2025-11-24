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

use monad_executor::{ExecutorMetrics, Histogram};

use crate::util::unix_ts_ms_now;

pub const PRIMARY_BROADCAST_LATENCY_P50_MS: &str =
    "monad.bft.raptorcast.udp.primary_broadcast_latency_p50_ms";
pub const PRIMARY_BROADCAST_LATENCY_P90_MS: &str =
    "monad.bft.raptorcast.udp.primary_broadcast_latency_p90_ms";
pub const PRIMARY_BROADCAST_LATENCY_P99_MS: &str =
    "monad.bft.raptorcast.udp.primary_broadcast_latency_p99_ms";
pub const PRIMARY_BROADCAST_LATENCY_COUNT: &str =
    "monad.bft.raptorcast.udp.primary_broadcast_latency_count";

pub const SECONDARY_BROADCAST_LATENCY_P50_MS: &str =
    "monad.bft.raptorcast.udp.secondary_broadcast_latency_p50_ms";
pub const SECONDARY_BROADCAST_LATENCY_P90_MS: &str =
    "monad.bft.raptorcast.udp.secondary_broadcast_latency_p90_ms";
pub const SECONDARY_BROADCAST_LATENCY_P99_MS: &str =
    "monad.bft.raptorcast.udp.secondary_broadcast_latency_p99_ms";
pub const SECONDARY_BROADCAST_LATENCY_COUNT: &str =
    "monad.bft.raptorcast.udp.secondary_broadcast_latency_count";

pub(crate) struct LatencyHistogram {
    histogram: Histogram,
    p50_metric: &'static str,
    p90_metric: &'static str,
    p99_metric: &'static str,
    count_metric: &'static str,
}

impl LatencyHistogram {
    fn new(
        max_ms: u64,
        p50_metric: &'static str,
        p90_metric: &'static str,
        p99_metric: &'static str,
        count_metric: &'static str,
    ) -> Self {
        Self {
            histogram: Histogram::new(max_ms, 3).expect("failed to create latency histogram"),
            p50_metric,
            p90_metric,
            p99_metric,
            count_metric,
        }
    }

    pub(crate) fn record(&mut self, latency_ms: u64, metrics: &mut ExecutorMetrics) {
        if let Err(e) = self.histogram.record(latency_ms) {
            tracing::warn!("failed to record latency: {}", e);
        }

        metrics[self.p50_metric] = self.histogram.p50();
        metrics[self.p90_metric] = self.histogram.p90();
        metrics[self.p99_metric] = self.histogram.p99();
        metrics[self.count_metric] = self.histogram.count();
    }
}

pub struct UdpStateMetrics {
    primary_broadcast: LatencyHistogram,
    secondary_broadcast: LatencyHistogram,
    executor_metrics: ExecutorMetrics,
}

impl UdpStateMetrics {
    pub fn new() -> Self {
        Self {
            primary_broadcast: LatencyHistogram::new(
                10_000,
                PRIMARY_BROADCAST_LATENCY_P50_MS,
                PRIMARY_BROADCAST_LATENCY_P90_MS,
                PRIMARY_BROADCAST_LATENCY_P99_MS,
                PRIMARY_BROADCAST_LATENCY_COUNT,
            ),
            secondary_broadcast: LatencyHistogram::new(
                10_000,
                SECONDARY_BROADCAST_LATENCY_P50_MS,
                SECONDARY_BROADCAST_LATENCY_P90_MS,
                SECONDARY_BROADCAST_LATENCY_P99_MS,
                SECONDARY_BROADCAST_LATENCY_COUNT,
            ),
            executor_metrics: ExecutorMetrics::default(),
        }
    }

    pub fn record_broadcast_latency(
        &mut self,
        mode: crate::util::BroadcastMode,
        message_ts_ms: u64,
    ) {
        let now_ms = unix_ts_ms_now();
        if now_ms < message_ts_ms {
            return;
        }

        let latency_ms = now_ms - message_ts_ms;
        let histogram = match mode {
            crate::util::BroadcastMode::Primary => &mut self.primary_broadcast,
            crate::util::BroadcastMode::Secondary => &mut self.secondary_broadcast,
        };
        histogram.record(latency_ms, &mut self.executor_metrics);
    }

    pub fn executor_metrics(&self) -> &ExecutorMetrics {
        &self.executor_metrics
    }
}

impl Default for UdpStateMetrics {
    fn default() -> Self {
        Self::new()
    }
}
