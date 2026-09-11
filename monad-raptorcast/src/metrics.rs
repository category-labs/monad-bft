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

use std::time::Duration;

use monad_executor::ExecutorMetrics;

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
    pub COUNTER_RAPTORCAST_SECONDARY_CHUNKS_DROPPED_INCOMPATIBLE_VERSION {
        name: "monad.raptorcast.secondary_chunks_dropped_incompatible_version",
        help: "Secondary raptorcast chunks dropped due to incompatible version",
    }
    pub COUNTER_RAPTORCAST_V0_SECONDARY_CHUNKS_ACCEPTED {
        name: "monad.raptorcast.v0_secondary_chunks_accepted",
        help: "V0 (regular) secondary raptorcast chunks accepted",
    }
    pub COUNTER_RAPTORCAST_V1_SECONDARY_CHUNKS_ACCEPTED {
        name: "monad.raptorcast.v1_secondary_chunks_accepted",
        help: "V1 (deterministic) secondary raptorcast chunks accepted",
    }
    pub GAUGE_RAPTORCAST_DETERMINISTIC_ROLLOUT_STAGE {
        name: "monad.raptorcast.deterministic_rollout_stage",
        help: "Current deterministic raptorcast rollout stage (0=always_v0, 1=accept_both_publish_v0, 2=accept_both_publish_v1, 3=always_v1)",
    }
    pub BROADCAST_LATENCY_SECONDS {
        name: "monad.raptorcast.broadcast_latency_seconds",
        help: "UDP broadcast latency from sender timestamp to completed message decoding in seconds",
    }
}

monad_executor::histogram_labels! {
    struct BroadcastLatency {
        primary => "primary",
        secondary => "secondary",
    }
}

pub(crate) fn init_router_executor_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        GAUGE_RAPTORCAST_TOTAL_MESSAGES_RECEIVED,
        GAUGE_RAPTORCAST_TOTAL_RECV_ERRORS,
        GAUGE_RAPTORCAST_TOTAL_DESERIALIZE_ERRORS,
        COUNTER_RAPTORCAST_DIRECT_UDP_FORWARD_SENT,
        COUNTER_RAPTORCAST_DIRECT_UDP_FORWARD_FALLBACK,
        COUNTER_RAPTORCAST_DIRECT_UDP_FORWARD_OVERSIZE,
    ])
}

pub(crate) fn init_udp_state_executor_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        GAUGE_RAPTORCAST_DECODING_CACHE_SIGNATURE_VERIFICATIONS_RATE_LIMITED,
        COUNTER_RAPTORCAST_CHUNKS_DROPPED_INCOMPATIBLE_VERSION,
        COUNTER_RAPTORCAST_V0_PRIMARY_CHUNKS_ACCEPTED,
        COUNTER_RAPTORCAST_V1_PRIMARY_CHUNKS_ACCEPTED,
        COUNTER_RAPTORCAST_SECONDARY_CHUNKS_DROPPED_INCOMPATIBLE_VERSION,
        COUNTER_RAPTORCAST_V0_SECONDARY_CHUNKS_ACCEPTED,
        COUNTER_RAPTORCAST_V1_SECONDARY_CHUNKS_ACCEPTED,
        GAUGE_RAPTORCAST_DETERMINISTIC_ROLLOUT_STAGE,
    ])
}

pub struct UdpStateMetrics {
    broadcast_latency: BroadcastLatency,
    executor_metrics: ExecutorMetrics,
}

impl UdpStateMetrics {
    pub fn new() -> Self {
        let mut executor_metrics = init_udp_state_executor_metrics();
        let broadcast_latency =
            BroadcastLatency::new(&mut executor_metrics, BROADCAST_LATENCY_SECONDS, "mode");
        Self {
            broadcast_latency,
            executor_metrics,
        }
    }

    pub fn record_broadcast_latency(
        &mut self,
        mode: crate::util::BroadcastMode,
        message_ts_ms: u64,
    ) {
        self.record_broadcast_latency_at(mode, message_ts_ms, unix_ts_ms_now());
    }

    fn record_broadcast_latency_at(
        &self,
        mode: crate::util::BroadcastMode,
        message_ts_ms: u64,
        now_ms: u64,
    ) {
        if now_ms < message_ts_ms {
            return;
        }

        let latency_ms = now_ms - message_ts_ms;
        let histogram = match mode {
            crate::util::BroadcastMode::Unspecified => return,
            crate::util::BroadcastMode::Primary => &self.broadcast_latency.primary,
            crate::util::BroadcastMode::Secondary => &self.broadcast_latency.secondary,
        };
        histogram.observe_duration(Duration::from_millis(latency_ms));
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

#[cfg(test)]
mod tests {
    use monad_executor::ExecutorMetricsChain;

    use super::*;
    use crate::util::BroadcastMode;

    #[test]
    fn broadcast_latency_records_modes_in_seconds_without_a_reset_window() {
        let metrics = UdpStateMetrics::new();
        let registry = ExecutorMetricsChain::from(metrics.executor_metrics())
            .native_histogram_registry(Default::default(), &prometheus::Registry::new())
            .unwrap();
        metrics.record_broadcast_latency_at(BroadcastMode::Primary, 1_000, 1_012);
        metrics.record_broadcast_latency_at(BroadcastMode::Secondary, 1_000, 1_025);
        // Later observations keep the original samples, including zero and long delays.
        metrics.record_broadcast_latency_at(BroadcastMode::Primary, 60_000, 60_000);
        metrics.record_broadcast_latency_at(BroadcastMode::Secondary, 1_000, 61_000);
        // Preserve the existing timestamp and mode filtering.
        metrics.record_broadcast_latency_at(BroadcastMode::Primary, 100_000, 1_000);
        metrics.record_broadcast_latency_at(BroadcastMode::Unspecified, 1_000, 1_005);
        let families = registry.classic_metric_families().unwrap();
        assert_eq!(families.len(), 1);
        assert_eq!(
            families[0].name(),
            "monad_raptorcast_broadcast_latency_seconds"
        );
        for (mode, sum) in [("primary", 0.012), ("secondary", 60.025)] {
            let sample = families[0]
                .get_metric()
                .iter()
                .find(|m| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == "mode" && l.value() == mode)
                })
                .unwrap();
            assert_eq!(sample.get_histogram().sample_count(), 2);
            assert!((sample.get_histogram().sample_sum() - sum).abs() < 1e-12);
        }
    }
}
