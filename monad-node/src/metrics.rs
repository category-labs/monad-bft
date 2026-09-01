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
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use actix_server::Server;
use actix_web::{http::header, web, App, HttpRequest, HttpResponse, HttpServer};
use monad_consensus_types::metrics::Metrics as StateMetrics;
use monad_executor::{metric_consts, ExecutorMetrics, ExecutorMetricsChain, Gauge};
use monad_triedb_utils::{MigrationPhase, StorageStats, TriedbStatsReader, UpdateStats};
use prometheus::{Encoder, ProtobufEncoder, Registry, TextEncoder};
use tracing::info;

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
        help: "Dual-DB migration phase: 0=legacy (not started), 1=dual-timeline (migrating), 2=page-encoded (complete), 3=promoted (primary is page-encoded)",
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
    // Map to the published 0/1/2/3 codes explicitly so the metric's wire
    // contract stays fixed even if the MigrationPhase discriminants change
    // upstream (the enum is defined in monad-triedb).
    let code: u64 = match phase {
        MigrationPhase::Legacy => 0,
        MigrationPhase::DualTimeline => 1,
        MigrationPhase::PageEncoded => 2,
        MigrationPhase::Promoted => 3,
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
    pub GAUGE_TRIEDB_NODES_CREATED_OR_UPDATED {
        name: "monad.triedb.nodes_created_or_updated",
        help: "Trie nodes written by execution's upserts, cumulative. The denominator for the compaction counters below.",
    }
    pub GAUGE_TRIEDB_COMPACTION_READS {
        name: "monad.triedb.compaction_reads",
        help: "Node reads issued by compaction, cumulative. These compete with execution's own reads for disk.",
    }
    pub GAUGE_TRIEDB_COMPACTED_BYTES_FAST_TO_SLOW {
        name: "monad.triedb.compacted_bytes_fast_to_slow",
        help: "Bytes compaction copied out of the fast ring into the slow ring, cumulative.",
    }
    pub GAUGE_TRIEDB_COMPACTED_BYTES_SLOW_TO_SLOW {
        name: "monad.triedb.compacted_bytes_slow_to_slow",
        help: "Bytes compaction copied within the slow ring, cumulative. This is the recycling that reclaims slow-ring chunks.",
    }
    pub GAUGE_TRIEDB_COPIED_BYTES_SLOW_TO_FAST {
        name: "monad.triedb.copied_bytes_slow_to_fast",
        help: "Bytes copied from the slow ring back into the fast ring on behalf of slow-ring writes, cumulative.",
    }
    pub GAUGE_TRIEDB_EXPIRE_NODES_UPDATED {
        name: "monad.triedb.expire_nodes_updated",
        help: "Trie nodes rewritten by history expiry, cumulative.",
    }
    pub GAUGE_TRIEDB_EXPIRE_READS {
        name: "monad.triedb.expire_reads",
        help: "Node reads issued by history expiry, cumulative.",
    }
}

pub fn init_triedb_update_stats_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        GAUGE_TRIEDB_NODES_CREATED_OR_UPDATED,
        GAUGE_TRIEDB_COMPACTION_READS,
        GAUGE_TRIEDB_COMPACTED_BYTES_FAST_TO_SLOW,
        GAUGE_TRIEDB_COMPACTED_BYTES_SLOW_TO_SLOW,
        GAUGE_TRIEDB_COPIED_BYTES_SLOW_TO_FAST,
        GAUGE_TRIEDB_EXPIRE_NODES_UPDATED,
        GAUGE_TRIEDB_EXPIRE_READS,
    ])
}

/// Opens the statistics sidecar at `path`, if one is configured, and returns
/// the gauges to register along with the reader to refresh them from.
///
/// A configured sidecar defines its gauges whether or not the file is there
/// yet: execution creates it when it opens the db, which can be after this
/// point, and the Prometheus registry takes the gauge set once. The caller
/// retries the open on its refresh ticker.
pub fn init_triedb_update_stats(
    path: Option<&Path>,
) -> (ExecutorMetrics, Option<TriedbStatsReader>) {
    let Some(path) = path else {
        return (ExecutorMetrics::with_metric_defs(&[]), None);
    };

    let mut metrics = init_triedb_update_stats_metrics();
    let reader = TriedbStatsReader::try_new(path);
    match &reader {
        Some(reader) => {
            if let Some(stats) = reader.update_stats() {
                record_triedb_update_stats_metrics(&mut metrics, stats);
            }
        }
        None => info!(
            ?path,
            "triedb stats sidecar not published yet, retrying on the metrics ticker"
        ),
    }
    (metrics, reader)
}

/// The counters restart at zero when execution restarts, which PromQL's rate
/// and increase already read as a counter reset.
pub fn record_triedb_update_stats_metrics(metrics: &mut ExecutorMetrics, stats: UpdateStats) {
    metrics
        .gauge(GAUGE_TRIEDB_NODES_CREATED_OR_UPDATED)
        .set(stats.nodes_created_or_updated);
    metrics
        .gauge(GAUGE_TRIEDB_COMPACTION_READS)
        .set(stats.nreads_compaction);
    metrics
        .gauge(GAUGE_TRIEDB_COMPACTED_BYTES_FAST_TO_SLOW)
        .set(stats.compacted_bytes_in_fast);
    metrics
        .gauge(GAUGE_TRIEDB_COMPACTED_BYTES_SLOW_TO_SLOW)
        .set(stats.compacted_bytes_in_slow);
    metrics
        .gauge(GAUGE_TRIEDB_COPIED_BYTES_SLOW_TO_FAST)
        .set(stats.bytes_copied_slow_to_fast_for_slow);
    metrics
        .gauge(GAUGE_TRIEDB_EXPIRE_NODES_UPDATED)
        .set(stats.nodes_updated_expire);
    metrics
        .gauge(GAUGE_TRIEDB_EXPIRE_READS)
        .set(stats.nreads_expire);
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
            (MigrationPhase::Promoted, 3),
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
mod update_stats_metrics_tests {
    use monad_triedb_utils::UpdateStats;
    use prometheus::{Encoder, Registry, TextEncoder};

    use super::{
        init_triedb_update_stats_metrics, record_triedb_update_stats_metrics,
        GAUGE_TRIEDB_COMPACTED_BYTES_FAST_TO_SLOW, GAUGE_TRIEDB_COMPACTED_BYTES_SLOW_TO_SLOW,
        GAUGE_TRIEDB_COMPACTION_READS, GAUGE_TRIEDB_COPIED_BYTES_SLOW_TO_FAST,
        GAUGE_TRIEDB_EXPIRE_NODES_UPDATED, GAUGE_TRIEDB_EXPIRE_READS,
        GAUGE_TRIEDB_NODES_CREATED_OR_UPDATED,
    };

    // Distinct per exported field, so a gauge wired to the wrong counter
    // reports a value that belongs to another gauge.
    fn stats() -> UpdateStats {
        UpdateStats {
            nodes_created_or_updated: 11,
            nreads_compaction: 22,
            nreads_before_compact_offset_fast: 0,
            nreads_before_compact_offset_slow: 0,
            nreads_after_compact_offset_fast: 0,
            nreads_after_compact_offset_slow: 0,
            bytes_read_before_compact_offset_fast: 0,
            bytes_read_before_compact_offset_slow: 0,
            bytes_read_after_compact_offset_fast: 0,
            bytes_read_after_compact_offset_slow: 0,
            compacted_nodes_in_fast: 0,
            compacted_nodes_in_slow: 0,
            nodes_copied_fast_to_fast_for_fast: 0,
            nodes_copied_fast_to_fast_for_slow: 0,
            nodes_copied_slow_to_fast_for_slow: 0,
            compacted_bytes_in_fast: 33,
            compacted_bytes_in_slow: 44,
            bytes_copied_slow_to_fast_for_slow: 55,
            nodes_updated_expire: 66,
            nreads_expire: 77,
        }
    }

    #[test]
    fn records_every_exported_counter() {
        let mut metrics = init_triedb_update_stats_metrics();
        record_triedb_update_stats_metrics(&mut metrics, stats());

        assert_eq!(
            metrics.gauge(GAUGE_TRIEDB_NODES_CREATED_OR_UPDATED).get(),
            11
        );
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_COMPACTION_READS).get(), 22);
        assert_eq!(
            metrics
                .gauge(GAUGE_TRIEDB_COMPACTED_BYTES_FAST_TO_SLOW)
                .get(),
            33
        );
        assert_eq!(
            metrics
                .gauge(GAUGE_TRIEDB_COMPACTED_BYTES_SLOW_TO_SLOW)
                .get(),
            44
        );
        assert_eq!(
            metrics.gauge(GAUGE_TRIEDB_COPIED_BYTES_SLOW_TO_FAST).get(),
            55
        );
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_EXPIRE_NODES_UPDATED).get(), 66);
        assert_eq!(metrics.gauge(GAUGE_TRIEDB_EXPIRE_READS).get(), 77);
    }

    // A refresh reaches the scrape only if it writes the same gauges the
    // registry holds, so assert through the encoded output rather than through
    // the ExecutorMetrics the ticker writes to.
    #[test]
    fn refresh_after_registration_reaches_the_scrape() {
        let mut metrics = init_triedb_update_stats_metrics();
        let registry = Registry::new();
        metrics.register(&registry).expect("gauges registered");

        for nodes in [11, 99] {
            record_triedb_update_stats_metrics(
                &mut metrics,
                UpdateStats {
                    nodes_created_or_updated: nodes,
                    ..stats()
                },
            );
        }

        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buffer)
            .expect("encoded");
        let scraped = String::from_utf8(buffer).expect("utf-8");
        assert!(
            scraped.contains("monad_triedb_nodes_created_or_updated 99"),
            "{scraped}"
        );
        assert!(
            scraped.contains("monad_triedb_expire_reads 77"),
            "{scraped}"
        );
    }
}

#[cfg(test)]
mod update_stats_startup_tests {
    use std::path::Path;

    use prometheus::{Encoder, Registry, TextEncoder};

    use super::init_triedb_update_stats;

    fn scrape(metrics: &mut super::ExecutorMetrics) -> String {
        let registry = Registry::new();
        metrics.register(&registry).expect("gauges registered");
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buffer)
            .expect("encoded");
        String::from_utf8(buffer).expect("utf-8")
    }

    // Execution creates the sidecar when it opens the db, which can be after
    // monad-node starts, and the registry snapshots the gauge set once. A node
    // configured for the sidecar therefore has to define its gauges before
    // anything has been read, or a later read has nowhere to land.
    #[test]
    fn a_configured_sidecar_registers_its_gauges_before_execution_creates_it() {
        let (mut metrics, reader) =
            init_triedb_update_stats(Some(Path::new("/nonexistent/triedb-stats")));

        assert!(reader.is_none(), "no sidecar to open");
        assert!(
            scrape(&mut metrics).contains("monad_triedb_nodes_created_or_updated 0"),
            "gauges must be registered before the first successful read"
        );
    }

    #[test]
    fn an_unconfigured_sidecar_registers_nothing() {
        let (mut metrics, reader) = init_triedb_update_stats(None);

        assert!(reader.is_none());
        assert!(!scrape(&mut metrics).contains("monad_triedb_nodes_created_or_updated"));
    }
}
