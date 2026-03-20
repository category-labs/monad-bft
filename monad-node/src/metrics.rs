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

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use monad_consensus_types::metrics::Metrics as StateMetrics;
use monad_executor::{metric_consts, ExecutorMetrics, ExecutorMetricsChain, Gauge};
use prometheus::Registry;

metric_consts! {
    pub GAUGE_TOTAL_UPTIME_US {
        name: "monad.total_uptime_us",
        help: "Total node uptime in microseconds",
    }
    pub GAUGE_STATE_TOTAL_UPDATE_US {
        name: "monad.state.total_update_us",
        help: "Total time spent updating state in microseconds",
    }
    pub GAUGE_NODE_INFO {
        name: "monad_node_info",
        help: "Node info indicator (always 1)",
    }
}

fn init_node_executor_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs([
        GAUGE_TOTAL_UPTIME_US,
        GAUGE_STATE_TOTAL_UPDATE_US,
        GAUGE_NODE_INFO,
    ])
}

pub struct NodePrometheusMetrics {
    registry: Registry,
    total_uptime: Gauge,
    total_state_update: Gauge,
    node_info: Gauge,
    process_start: Instant,
}

impl NodePrometheusMetrics {
    pub fn new(
        labels: HashMap<String, String>,
        state_metrics: &StateMetrics,
        executor_metrics: ExecutorMetricsChain<'_>,
        process_start: Instant,
    ) -> Result<Self, prometheus::Error> {
        let registry = Registry::new_custom(None, Some(labels))?;
        state_metrics.register(&registry)?;

        executor_metrics.register(&registry)?;

        let mut node_executor_metrics = init_node_executor_metrics();
        node_executor_metrics.set(GAUGE_NODE_INFO, 1);
        node_executor_metrics.set(GAUGE_TOTAL_UPTIME_US, 0);
        node_executor_metrics.set(GAUGE_STATE_TOTAL_UPDATE_US, 0);
        node_executor_metrics.register(&registry)?;

        Ok(Self {
            registry,
            total_uptime: node_executor_metrics.gauge(GAUGE_TOTAL_UPTIME_US),
            total_state_update: node_executor_metrics.gauge(GAUGE_STATE_TOTAL_UPDATE_US),
            node_info: node_executor_metrics.gauge(GAUGE_NODE_INFO),
            process_start,
        })
    }

    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }

    pub fn metric_handles(&self) -> Vec<(&'static str, Gauge, &'static str)> {
        vec![
            (
                GAUGE_TOTAL_UPTIME_US.name,
                self.total_uptime.clone(),
                GAUGE_TOTAL_UPTIME_US.help,
            ),
            (
                GAUGE_STATE_TOTAL_UPDATE_US.name,
                self.total_state_update.clone(),
                GAUGE_STATE_TOTAL_UPDATE_US.help,
            ),
            (
                GAUGE_NODE_INFO.name,
                self.node_info.clone(),
                GAUGE_NODE_INFO.help,
            ),
        ]
    }

    pub fn record_state_update_elapsed(&self, total_state_update_elapsed: &Duration) {
        self.total_state_update
            .set(total_state_update_elapsed.as_micros() as u64);
        self.node_info.set(1);
    }

    pub fn refresh_dynamic_metrics(&self) {
        self.total_uptime
            .set(self.process_start.elapsed().as_micros() as u64);
        self.node_info.set(1);
    }
}
