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
use monad_executor::{
    ExecutorMetricHandle, ExecutorMetrics, ExecutorMetricsChain, MetricDef, metric_consts,
};
use prometheus::Registry;

metric_consts! {
    GAUGE_TOTAL_UPTIME_US {
        name: "monad.total_uptime_us",
        help: "Total node uptime in microseconds",
    }
    GAUGE_STATE_TOTAL_UPDATE_US {
        name: "monad.state.total_update_us",
        help: "Total time spent updating state in microseconds",
    }
    GAUGE_NODE_INFO {
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
    state_metrics: HashMap<&'static str, ExecutorMetricHandle>,
    total_uptime: ExecutorMetricHandle,
    total_state_update: ExecutorMetricHandle,
    node_info: ExecutorMetricHandle,
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

        let mut state_executor_metrics = ExecutorMetrics::default();
        let mut state_metric_defs = HashMap::new();
        for (name, value, help) in state_metrics.metrics() {
            let metric: &'static MetricDef = Box::leak(Box::new(MetricDef::new(name, help)));
            state_executor_metrics.set(metric, value);
            state_metric_defs.insert(name, metric);
        }
        state_executor_metrics.register(&registry)?;

        executor_metrics.register(&registry)?;

        let mut node_executor_metrics = init_node_executor_metrics();
        node_executor_metrics.set(GAUGE_NODE_INFO, 1);
        node_executor_metrics.set(GAUGE_TOTAL_UPTIME_US, 0);
        node_executor_metrics.set(GAUGE_STATE_TOTAL_UPDATE_US, 0);
        node_executor_metrics.register(&registry)?;

        Ok(Self {
            registry,
            state_metrics: state_metric_defs
                .into_iter()
                .map(|(name, metric)| (name, state_executor_metrics.handle(metric)))
                .collect(),
            total_uptime: node_executor_metrics.handle(GAUGE_TOTAL_UPTIME_US),
            total_state_update: node_executor_metrics.handle(GAUGE_STATE_TOTAL_UPDATE_US),
            node_info: node_executor_metrics.handle(GAUGE_NODE_INFO),
            process_start,
        })
    }

    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }

    pub fn update(&self, state_metrics: &StateMetrics, total_state_update_elapsed: &Duration) {
        for (name, value, _) in state_metrics.metrics() {
            self.state_metrics
                .get(name)
                .expect("state metric must be initialized")
                .set(value);
        }

        self.total_state_update
            .set(total_state_update_elapsed.as_micros() as u64);
        self.node_info.set(1);
    }

    pub fn refresh_for_scrape(&self) {
        self.total_uptime
            .set(self.process_start.elapsed().as_micros() as u64);
        self.node_info.set(1);
    }
}
