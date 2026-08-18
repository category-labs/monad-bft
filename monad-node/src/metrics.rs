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
    sync::Arc,
    time::{Duration, Instant},
};

use actix_server::Server;
use actix_web::{http::header, web, App, HttpRequest, HttpResponse, HttpServer};
use monad_consensus_types::metrics::Metrics as StateMetrics;
use monad_execution_state_read::NodeCacheStats;
use monad_executor::{metric_consts, ExecutorMetrics, ExecutorMetricsChain, Gauge};
use monad_triedb_utils::{MigrationPhase, StorageStats};
use prometheus::{Encoder, ProtobufEncoder, Registry, TextEncoder};

pub fn default_prometheus_labels(
    service_name: String,
    network_name: String,
    version: Option<&str>,
) -> HashMap<String, String> {
    let mut labels = HashMap::from([
        ("service_name".to_owned(), service_name),
        ("network".to_owned(), network_name),
    ]);
    if let Some(version) = version {
        labels.insert("service_version".to_owned(), version.to_owned());
    }
    labels
}

metric_consts! {
    pub GAUGE_TOTAL_UPTIME_US {
        name: "monad.total_uptime_us",
        help: "Total node uptime in microseconds",
    }
    pub GAUGE_STATE_TOTAL_UPDATE_US {
        name: "monad.state.total_update_us",
        help: "Total time spent updating state in microseconds",
    }
    // Keep this already sanitized so Prometheus and OTel export the same info metric name.
    pub GAUGE_NODE_INFO {
        name: "monad_node_info",
        help: "Node info indicator (always 1)",
    }
}

metric_consts! {
    pub GAUGE_TRIEDB_MIGRATION_PHASE {
        name: "monad.triedb.migration_phase",
        help: "Dual-DB migration phase: 0=legacy (not started), 1=dual-timeline (migrating), 2=page-encoded (complete)",
    }
}

fn init_node_executor_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        GAUGE_TOTAL_UPTIME_US,
        GAUGE_STATE_TOTAL_UPDATE_US,
        GAUGE_NODE_INFO,
    ])
}

pub fn init_triedb_phase_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[GAUGE_TRIEDB_MIGRATION_PHASE])
}

pub fn record_triedb_phase_metrics(metrics: &mut ExecutorMetrics, phase: MigrationPhase) {
    // Map to the published 0/1/2 codes explicitly so the metric's wire contract
    // stays fixed even if the MigrationPhase discriminants change upstream (the
    // enum is defined in monad-triedb).
    let code: u64 = match phase {
        MigrationPhase::Legacy => 0,
        MigrationPhase::DualTimeline => 1,
        MigrationPhase::PageEncoded => 2,
    };
    metrics.gauge(GAUGE_TRIEDB_MIGRATION_PHASE).set(code);
}

metric_consts! {
    pub GAUGE_TRIEDB_DISK_CAPACITY_BYTES {
        name: "monad.triedb.disk_capacity_bytes",
        help: "Total triedb storage-pool capacity in bytes: sum of file sizes (file pools) or raw device sizes (block-device pools).",
    }
    pub GAUGE_TRIEDB_DISK_USED_BYTES {
        name: "monad.triedb.disk_used_bytes",
        help: "triedb storage-pool bytes in use. Block-device pools sum appended bytes per chunk and under-report pool overhead by a few hundred MiB per device; file pools report filesystem-allocated blocks, a high-water mark that does not shrink as chunks are recycled. Treat the trend as the signal, not the absolute value.",
    }
}

pub fn init_triedb_storage_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        GAUGE_TRIEDB_DISK_CAPACITY_BYTES,
        GAUGE_TRIEDB_DISK_USED_BYTES,
    ])
}

pub fn record_triedb_storage_metrics(metrics: &mut ExecutorMetrics, stats: StorageStats) {
    metrics
        .gauge(GAUGE_TRIEDB_DISK_CAPACITY_BYTES)
        .set(stats.disk_capacity_bytes);
    metrics
        .gauge(GAUGE_TRIEDB_DISK_USED_BYTES)
        .set(stats.disk_used_bytes);
}

metric_consts! {
    pub GAUGE_TRIEDB_NODE_CACHE_HITS {
        name: "monad.triedb.node_cache.hits",
        help: "Trie-node LRU lookups served from cache, cumulative since node start. Only the state-read path's cache is reported; RPC runs its own separate cache in another process.",
    }
    pub GAUGE_TRIEDB_NODE_CACHE_MISSES {
        name: "monad.triedb.node_cache.misses",
        help: "Trie-node LRU lookups that fell through to disk, cumulative since node start. Hit rate is hits/(hits+misses); derive it in the query, not here.",
    }
    pub GAUGE_TRIEDB_NODE_CACHE_EVICTIONS {
        name: "monad.triedb.node_cache.evictions",
        help: "Trie nodes dropped to stay within the cache's bounds, cumulative since node start. A rising rate against a flat hit rate means the cache is thrashing and undersized.",
    }
    pub GAUGE_TRIEDB_NODE_CACHE_USED_BYTES {
        name: "monad.triedb.node_cache.used_bytes",
        help: "Bytes of cached trie nodes, against the configured node LRU byte budget.",
    }
    pub GAUGE_TRIEDB_NODE_CACHE_ENTRIES {
        name: "monad.triedb.node_cache.entries",
        help: "Cached trie nodes, against the slot count derived from the byte budget. Compare with used_bytes to see which of the two bounds is binding.",
    }
}

pub fn init_triedb_node_cache_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        GAUGE_TRIEDB_NODE_CACHE_HITS,
        GAUGE_TRIEDB_NODE_CACHE_MISSES,
        GAUGE_TRIEDB_NODE_CACHE_EVICTIONS,
        GAUGE_TRIEDB_NODE_CACHE_USED_BYTES,
        GAUGE_TRIEDB_NODE_CACHE_ENTRIES,
    ])
}

pub fn record_triedb_node_cache_metrics(metrics: &mut ExecutorMetrics, stats: NodeCacheStats) {
    metrics.gauge(GAUGE_TRIEDB_NODE_CACHE_HITS).set(stats.hits);
    metrics
        .gauge(GAUGE_TRIEDB_NODE_CACHE_MISSES)
        .set(stats.misses);
    metrics
        .gauge(GAUGE_TRIEDB_NODE_CACHE_EVICTIONS)
        .set(stats.evictions);
    metrics
        .gauge(GAUGE_TRIEDB_NODE_CACHE_USED_BYTES)
        .set(stats.used_bytes);
    metrics
        .gauge(GAUGE_TRIEDB_NODE_CACHE_ENTRIES)
        .set(stats.entries);
}

fn duration_micros_u64(duration: &Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
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
        let state_metric_handles = state_metrics.metric_handles();
        for (_, gauge, _) in &state_metric_handles {
            registry.register(Box::new(gauge.clone()))?;
        }

        for (_, gauge, _) in executor_metrics.metric_handles() {
            registry.register(Box::new(gauge))?;
        }

        let mut node_executor_metrics = init_node_executor_metrics();
        node_executor_metrics.gauge(GAUGE_NODE_INFO).set(1);
        node_executor_metrics.gauge(GAUGE_TOTAL_UPTIME_US).set(0);
        node_executor_metrics
            .gauge(GAUGE_STATE_TOTAL_UPDATE_US)
            .set(0);
        node_executor_metrics.register(&registry)?;

        Ok(Self {
            registry,
            state_metrics: state_metric_handles,
            total_uptime: node_executor_metrics.gauge(GAUGE_TOTAL_UPTIME_US).clone(),
            total_state_update: node_executor_metrics
                .gauge(GAUGE_STATE_TOTAL_UPDATE_US)
                .clone(),
            node_info: node_executor_metrics.gauge(GAUGE_NODE_INFO).clone(),
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

    pub fn record_state_update_elapsed(&self, total_state_update_elapsed: &Duration) {
        self.total_state_update
            .set(duration_micros_u64(total_state_update_elapsed));
    }

    pub fn refresh_dynamic_metrics(&self) {
        self.total_uptime
            .set(duration_micros_u64(&self.process_start.elapsed()));
    }
}

#[derive(Clone)]
pub struct MetricsServerState {
    registry: Registry,
    before_gather: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl MetricsServerState {
    pub fn new(registry: Registry, before_gather: Option<Arc<dyn Fn() + Send + Sync>>) -> Self {
        Self {
            registry,
            before_gather,
        }
    }
}

fn wants_protobuf(request: &HttpRequest) -> bool {
    // Prometheus negotiates scrape response format with the request Accept header:
    // https://prometheus.io/docs/instrumenting/content_negotiation/
    request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains(prometheus::PROTOBUF_FORMAT))
}

async fn handle_metrics(
    request: HttpRequest,
    state: web::Data<MetricsServerState>,
) -> HttpResponse {
    if let Some(before_gather) = &state.before_gather {
        before_gather();
    }

    let metric_families = state.registry.gather();
    let mut buffer = Vec::new();

    let content_type = if wants_protobuf(&request) {
        let encoder = ProtobufEncoder::new();
        if encoder.encode(&metric_families, &mut buffer).is_err() {
            return HttpResponse::InternalServerError().finish();
        }
        prometheus::PROTOBUF_FORMAT
    } else {
        let encoder = TextEncoder::new();
        if encoder.encode(&metric_families, &mut buffer).is_err() {
            return HttpResponse::InternalServerError().finish();
        }
        prometheus::TEXT_FORMAT
    };

    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, content_type))
        .body(buffer)
}

pub fn start_metrics_server(addr: String, state: MetricsServerState) -> std::io::Result<Server> {
    Ok(HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .route("/metrics", web::get().to(handle_metrics))
    })
    .bind(addr)?
    .workers(1)
    .run())
}

#[cfg(test)]
mod migration_phase_tests {
    use monad_triedb_utils::MigrationPhase;

    use super::{
        init_triedb_phase_metrics, record_triedb_phase_metrics, GAUGE_TRIEDB_MIGRATION_PHASE,
    };

    #[test]
    fn records_phase_code() {
        for (phase, code) in [
            (MigrationPhase::Legacy, 0u64),
            (MigrationPhase::DualTimeline, 1),
            (MigrationPhase::PageEncoded, 2),
        ] {
            let mut metrics = init_triedb_phase_metrics();
            record_triedb_phase_metrics(&mut metrics, phase);
            assert_eq!(metrics.gauge(GAUGE_TRIEDB_MIGRATION_PHASE).get(), code);
        }
    }
}

#[cfg(test)]
mod storage_metrics_tests {
    use monad_triedb_utils::StorageStats;
    use prometheus::{Encoder, Registry, TextEncoder};

    use super::{
        init_triedb_storage_metrics, record_triedb_storage_metrics,
        GAUGE_TRIEDB_DISK_CAPACITY_BYTES, GAUGE_TRIEDB_DISK_USED_BYTES,
    };

    #[test]
    fn records_capacity_and_used() {
        let mut metrics = init_triedb_storage_metrics();
        record_triedb_storage_metrics(
            &mut metrics,
            StorageStats {
                disk_capacity_bytes: 1_000,
                disk_used_bytes: 600,
            },
        );
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_DISK_CAPACITY_BYTES).get(), 1_000);
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_DISK_USED_BYTES).get(), 600);
    }

    // A refresh reaches the scrape only if it writes the same gauges the
    // registry holds, so assert through the encoded output rather than through
    // the ExecutorMetrics the ticker writes to.
    #[test]
    fn refresh_after_registration_reaches_the_scrape() {
        let mut metrics = init_triedb_storage_metrics();
        let registry = Registry::new();
        metrics.register(&registry).expect("gauges registered");

        for used in [600, 700] {
            record_triedb_storage_metrics(
                &mut metrics,
                StorageStats {
                    disk_capacity_bytes: 1_000,
                    disk_used_bytes: used,
                },
            );
        }

        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buffer)
            .expect("encoded");
        let scraped = String::from_utf8(buffer).expect("utf-8");
        assert!(
            scraped.contains("monad_triedb_disk_capacity_bytes 1000"),
            "{scraped}"
        );
        assert!(
            scraped.contains("monad_triedb_disk_used_bytes 700"),
            "{scraped}"
        );
    }
}

#[cfg(test)]
mod node_cache_metrics_tests {
    use monad_execution_state_read::NodeCacheStats;
    use prometheus::{Encoder, Registry, TextEncoder};

    use super::{
        init_triedb_node_cache_metrics, record_triedb_node_cache_metrics,
        GAUGE_TRIEDB_NODE_CACHE_ENTRIES, GAUGE_TRIEDB_NODE_CACHE_EVICTIONS,
        GAUGE_TRIEDB_NODE_CACHE_HITS, GAUGE_TRIEDB_NODE_CACHE_MISSES,
        GAUGE_TRIEDB_NODE_CACHE_USED_BYTES,
    };

    fn stats(hits: u64, misses: u64) -> NodeCacheStats {
        NodeCacheStats {
            hits,
            misses,
            evictions: 7,
            used_bytes: 4_096,
            entries: 12,
        }
    }

    #[test]
    fn records_all_five_counters() {
        let mut metrics = init_triedb_node_cache_metrics();
        record_triedb_node_cache_metrics(&mut metrics, stats(100, 25));

        assert_eq!(metrics.gauge(GAUGE_TRIEDB_NODE_CACHE_HITS).get(), 100);
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_NODE_CACHE_MISSES).get(), 25);
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_NODE_CACHE_EVICTIONS).get(), 7);
        assert_eq!(
            metrics.gauge(GAUGE_TRIEDB_NODE_CACHE_USED_BYTES).get(),
            4_096
        );
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_NODE_CACHE_ENTRIES).get(), 12);
    }

    // The counters are cumulative, so a refresh must overwrite rather than
    // accumulate, and it only reaches a scrape if it writes the same gauges the
    // registry holds — assert through the encoded output, not the ExecutorMetrics.
    #[test]
    fn refresh_overwrites_and_reaches_the_scrape() {
        let mut metrics = init_triedb_node_cache_metrics();
        let registry = Registry::new();
        metrics.register(&registry).expect("gauges registered");

        record_triedb_node_cache_metrics(&mut metrics, stats(100, 25));
        record_triedb_node_cache_metrics(&mut metrics, stats(180, 30));

        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buffer)
            .expect("encoded");
        let scraped = String::from_utf8(buffer).expect("utf-8");
        assert!(
            scraped.contains("monad_triedb_node_cache_hits 180"),
            "{scraped}"
        );
        assert!(
            scraped.contains("monad_triedb_node_cache_misses 30"),
            "{scraped}"
        );
    }
}
