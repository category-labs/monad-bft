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

use std::time::{Duration, Instant};

use monad_executor::{ExecutorMetrics, Histogram, MetricDef};

use crate::util::unix_ts_ms_now;

monad_executor::metric_consts! {
    pub GAUGE_RAPTORCAST_TOTAL_MESSAGES_RECEIVED {
        name: "monad.raptorcast.total_messages_received",
        help: "Total raptorcast messages received",
    }
    pub GAUGE_RAPTORCAST_TOTAL_RECV_ERRORS {
        name: "monad.raptorcast.total_recv_errors",
        help: "Total raptorcast receive errors",
    }
    pub GAUGE_RAPTORCAST_TOTAL_DESERIALIZE_ERRORS {
        name: "monad.raptorcast.total_deserialize_errors",
        help: "Total raptorcast message deserialization errors",
    }
    pub GAUGE_RAPTORCAST_DECODING_CACHE_SIGNATURE_VERIFICATIONS_RATE_LIMITED {
        name: "monad.raptorcast.decoding_cache.signature_verifications_rate_limited",
        help: "Signature verifications rate limited",
    }
    pub COUNTER_RAPTORCAST_DIRECT_UDP_FORWARD_SENT {
        name: "monad.raptorcast.direct_udp.forward_sent",
        help: "Direct UDP forwards sent",
    }
    pub COUNTER_RAPTORCAST_DIRECT_UDP_FORWARD_FALLBACK {
        name: "monad.raptorcast.direct_udp.forward_fallback",
        help: "Direct UDP forwards that fell back to regular path",
    }
    pub COUNTER_RAPTORCAST_DIRECT_UDP_FORWARD_OVERSIZE {
        name: "monad.raptorcast.direct_udp.forward_oversize",
        help: "Direct UDP forwards rejected due to oversize payload",
    }
    pub COUNTER_RAPTORCAST_CHUNKS_DROPPED_INCOMPATIBLE_VERSION {
        name: "monad.raptorcast.chunks_dropped_incompatible_version",
        help: "Chunks dropped due to incompatible raptorcast version",
    }
    pub COUNTER_RAPTORCAST_V0_PRIMARY_CHUNKS_ACCEPTED {
        name: "monad.raptorcast.v0_primary_chunks_accepted",
        help: "V0 (regular) primary raptorcast chunks accepted",
    }
    pub COUNTER_RAPTORCAST_V1_PRIMARY_CHUNKS_ACCEPTED {
        name: "monad.raptorcast.v1_primary_chunks_accepted",
        help: "V1 (deterministic) primary raptorcast chunks accepted",
    }
    pub GAUGE_RAPTORCAST_DETERMINISTIC_ROLLOUT_STAGE {
        name: "monad.raptorcast.deterministic_rollout_stage",
        help: "Current deterministic raptorcast rollout stage (0=always_v0, 1=accept_both_publish_v0, 2=accept_both_publish_v1, 3=always_v1)",
    }
    pub PRIMARY_BROADCAST_LATENCY_P99_MS {
        name: "monad.bft.raptorcast.udp.primary_broadcast_latency_p99_ms",
        help: "P99 primary UDP broadcast latency in ms (30s rolling window)",
    }
    pub PRIMARY_BROADCAST_LATENCY_COUNT {
        name: "monad.bft.raptorcast.udp.primary_broadcast_latency_count",
        help: "Primary broadcast latency measurement count (30s rolling window)",
    }
    pub SECONDARY_BROADCAST_LATENCY_P99_MS {
        name: "monad.bft.raptorcast.udp.secondary_broadcast_latency_p99_ms",
        help: "P99 secondary UDP broadcast latency in ms (30s rolling window)",
    }
    pub SECONDARY_BROADCAST_LATENCY_COUNT {
        name: "monad.bft.raptorcast.udp.secondary_broadcast_latency_count",
        help: "Secondary broadcast latency measurement count (30s rolling window)",
    }
}

const HISTOGRAM_CLEAR_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) struct LatencyHistogram {
    histogram: Histogram,
    p99_metric: &'static MetricDef,
    count_metric: &'static MetricDef,
    last_reset: Instant,
}

impl LatencyHistogram {
    fn new(max_ms: u64, p99_metric: &'static MetricDef, count_metric: &'static MetricDef) -> Self {
        Self {
            histogram: Histogram::new(max_ms, 3).expect("failed to create latency histogram"),
            p99_metric,
            count_metric,
            last_reset: Instant::now(),
        }
    }

    pub(crate) fn record(&mut self, latency_ms: u64, metrics: &mut ExecutorMetrics) {
        let now = Instant::now();

        if now.duration_since(self.last_reset) >= HISTOGRAM_CLEAR_INTERVAL {
            self.histogram.clear();
            self.last_reset = now;
        }

        if let Err(e) = self.histogram.record(latency_ms) {
            tracing::warn!("failed to record latency: {}", e);
        }

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
                PRIMARY_BROADCAST_LATENCY_P99_MS,
                PRIMARY_BROADCAST_LATENCY_COUNT,
            ),
            secondary_broadcast: LatencyHistogram::new(
                10_000,
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
            crate::util::BroadcastMode::Unspecified => return,
            crate::util::BroadcastMode::Primary => &mut self.primary_broadcast,
            crate::util::BroadcastMode::Secondary => &mut self.secondary_broadcast,
        };
        histogram.record(latency_ms, &mut self.executor_metrics);
    }

    pub fn executor_metrics(&self) -> &ExecutorMetrics {
        &self.executor_metrics
    }

    pub fn executor_metrics_mut(&mut self) -> &mut ExecutorMetrics {
        &mut self.executor_metrics
    }
}

impl Default for UdpStateMetrics {
    fn default() -> Self {
        Self::new()
    }
}
