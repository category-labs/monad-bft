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
use prometheus::{Opts, Registry};

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

fn is_valid_prometheus_metric_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || matches!(ch, '_' | ':')
}

fn is_valid_prometheus_metric_char(ch: char) -> bool {
    is_valid_prometheus_metric_start(ch) || ch.is_ascii_digit()
}

fn prometheus_metric_name(metric: &str) -> prometheus::Result<String> {
    let mut name = String::with_capacity(metric.len());
    for (idx, ch) in metric.char_indices() {
        match ch {
            '.' => name.push('_'),
            ch if is_valid_prometheus_metric_char(ch) => name.push(ch),
            _ => {
                return Err(prometheus::Error::Msg(format!(
                    "invalid prometheus metric name {metric:?}: character {ch:?} at byte {idx} is not allowed"
                )));
            }
        }
    }

    let Some(first) = name.chars().next() else {
        return Err(prometheus::Error::Msg(
            "prometheus metric name must not be empty".to_owned(),
        ));
    };
    if !is_valid_prometheus_metric_start(first) {
        return Err(prometheus::Error::Msg(format!(
            "invalid prometheus metric name {metric:?}: first character {first:?} is not allowed"
        )));
    }

    Ok(name)
}

fn init_state_metric_handles(
    state_metrics: &StateMetrics,
) -> Vec<(&'static str, Gauge, &'static str)> {
    state_metrics
        .metrics()
        .into_iter()
        .map(|(name, value, help)| {
            let gauge = Gauge::with_opts(Opts::new(
                prometheus_metric_name(name).expect("state metric definition is valid"),
                help,
            ))
            .expect("state metric definition is valid");
            gauge.set(value);
            (name, gauge, help)
        })
        .collect()
}

pub struct NodePrometheusMetrics {
    registry: Registry,
    state_metrics: Vec<(&'static str, Gauge, &'static str)>,
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
        let state_metric_handles = init_state_metric_handles(state_metrics);
        for (_, gauge, _) in &state_metric_handles {
            registry.register(Box::new(gauge.clone()))?;
        }

        executor_metrics.register(&registry)?;

        let node_executor_metrics = init_node_executor_metrics();
        node_executor_metrics.gauge(GAUGE_NODE_INFO).set(1);
        node_executor_metrics.gauge(GAUGE_TOTAL_UPTIME_US).set(0);
        node_executor_metrics
            .gauge(GAUGE_STATE_TOTAL_UPDATE_US)
            .set(0);
        node_executor_metrics.register(&registry)?;

        Ok(Self {
            registry,
            state_metrics: state_metric_handles,
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
        self.state_metrics
            .iter()
            .map(|(name, gauge, help)| (*name, gauge.clone(), *help))
            .chain([
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
            ])
            .collect()
    }

    pub fn record_state_metrics(&self, state_metrics: &StateMetrics) {
        let state_metric_values: HashMap<_, _> = state_metrics
            .metrics()
            .into_iter()
            .map(|(name, value, _)| (name, value))
            .collect();

        for (name, gauge, _) in &self.state_metrics {
            if let Some(value) = state_metric_values.get(name) {
                gauge.set(*value);
            }
        }
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
#[cfg(test)]
mod tests {
    use super::prometheus_metric_name;

    #[test]
    fn prometheus_metric_name_replaces_dots_only() {
        assert_eq!(
            prometheus_metric_name("monad.state.consensus_events.created_qc").expect("valid name"),
            "monad_state_consensus_events_created_qc"
        );
    }

    #[test]
    fn prometheus_metric_name_rejects_invalid_names() {
        assert!(prometheus_metric_name("monad.state.invalid-name").is_err());
        assert!(prometheus_metric_name("1monad.state.invalid").is_err());
    }
}
