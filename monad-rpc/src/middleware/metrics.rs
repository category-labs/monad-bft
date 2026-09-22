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

use std::{sync::Arc, time::Duration};

use actix_web::{
    body::MessageBody,
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
};
use futures_util::future::{FutureExt as _, LocalBoxFuture};
use monad_triedb_utils::triedb_env::NodeCacheStats;
use opentelemetry::{
    metrics::{Histogram, MeterProvider, UpDownCounter},
    KeyValue,
};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;

pub struct MetricsMiddleware<S> {
    service: S,
    inner: std::sync::Arc<Metrics>,
}

impl<S, B> Service<ServiceRequest> for MetricsMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error>,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let timer = std::time::Instant::now();

        let mut attributes = attributes_from_request(&req);

        self.inner.active_requests.add(1, &attributes);

        let request_metrics = self.inner.clone();
        Box::pin(self.service.call(req).map(move |res| {
            request_metrics.active_requests.add(-1, &attributes);
            if let Ok(res) = res {
                let elapsed = timer.elapsed();

                attributes.push(KeyValue::new(
                    "http.response.status_code",
                    res.status().as_u16() as i64,
                ));

                request_metrics
                    .request_duration
                    .record(elapsed.as_secs_f64(), &attributes);
                Ok(res)
            } else {
                res
            }
        }))
    }
}

impl<S, B> Transform<S, ServiceRequest> for Metrics
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error>,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type InitError = ();
    type Transform = MetricsMiddleware<S>;
    type Future = futures_util::future::Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        futures_util::future::ready(Ok(MetricsMiddleware {
            service,
            inner: std::sync::Arc::new(self.clone()),
        }))
    }
}

fn attributes_from_request(req: &ServiceRequest) -> Vec<KeyValue> {
    let conn_info = req.connection_info();

    let mut attributes = Vec::with_capacity(3);

    let mut host_parts = conn_info.host().split_terminator(':');
    if let Some(host) = host_parts.next() {
        attributes.push(KeyValue::new("server.address", host.to_string()));
    }
    if let Some(port) = host_parts.next().and_then(|port| port.parse::<i64>().ok()) {
        attributes.push(KeyValue::new("server.port", port))
    }

    attributes
}

// A tuple table so a name cannot be added without an accessor -- that is how a
// metric gets registered but never observed.
type NodeCacheGauge = (&'static str, &'static str, fn(&NodeCacheStats) -> u64);

const NODE_CACHE_GAUGES: &[NodeCacheGauge] = &[
    (
        "monad.rpc.triedb.node_cache_hits",
        "Trie-node cache hits on the rpc read handle, cumulative.",
        |s| s.hits,
    ),
    (
        "monad.rpc.triedb.node_cache_misses",
        "Trie-node cache misses on the rpc read handle, cumulative. Neither implies the other with a disk read: concurrent misses on the same node are served by one read, and only the async read and traverse paths consult this cache at all.",
        |s| s.misses,
    ),
    (
        "monad.rpc.triedb.node_cache_evictions",
        "Trie nodes dropped from the rpc read cache to stay within its bounds, cumulative.",
        |s| s.evictions,
    ),
    (
        "monad.rpc.triedb.node_cache_used_bytes",
        "Bytes of trie nodes cached, against node_cache_max_bytes. Primary timeline only: while a secondary timeline is active its separate cache of the same size is not included, so the process can hold twice this.",
        |s| s.used_bytes,
    ),
    (
        "monad.rpc.triedb.node_cache_entries",
        "Trie nodes cached, against node_cache_max_entries. The cache is bounded by both, so read alongside node_cache_used_bytes to see which bound is binding.",
        |s| s.entries,
    ),
    (
        "monad.rpc.triedb.node_cache_max_bytes",
        "--triedb-node-lru-max-mem, so utilisation can be computed without knowing how this process was configured.",
        |s| s.max_bytes,
    ),
    (
        "monad.rpc.triedb.node_cache_max_entries",
        "Slot count derived from --triedb-node-lru-max-mem.",
        |s| s.max_entries,
    ),
];

#[derive(Clone)]
pub struct Metrics {
    provider: SdkMeterProvider,

    request_duration: Histogram<f64>,
    active_requests: UpDownCounter<i64>,
    active_websocket_connections: UpDownCounter<i64>,
    active_websocket_topics: UpDownCounter<i64>,
    pub(crate) execution_histogram: Histogram<f64>,
}

impl Metrics {
    pub fn new_with_otel_endpoint(
        otel_endpoint: String,
        service_name: String,
        interval: Duration,
    ) -> Self {
        let exporter = MetricExporter::builder()
            .with_tonic()
            .with_endpoint(otel_endpoint)
            .with_timeout(interval * 2)
            .build()
            .unwrap();

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
            .with_interval(interval / 2)
            .build();

        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(
                opentelemetry_sdk::Resource::builder_empty()
                    .with_attributes(vec![opentelemetry::KeyValue::new(
                        "service.name".to_string(),
                        service_name,
                    )])
                    .build(),
            )
            .build();

        Self::new_with_otel_provider(provider)
    }

    pub fn new_with_otel_provider(provider: SdkMeterProvider) -> Self {
        const LOW_US_TO_S: &[f64] = &[
            0.000_001, 0.000_002, 0.000_005, 0.000_01, 0.000_02, 0.000_05, 0.000_1, 0.000_2,
            0.000_5, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0,
        ];

        let meter = provider.meter("opentelemetry");

        let request_duration = meter
            .f64_histogram("monad.rpc.request_duration")
            .with_description("Duration of inbound http requests")
            .with_unit("s")
            .with_boundaries(LOW_US_TO_S.to_vec())
            .build();

        let active_requests = meter
            .i64_up_down_counter("monad.rpc.active_requests")
            .with_description("Number of concurrent http requests that are in-flight")
            .build();

        let active_websocket_connections = meter
            .i64_up_down_counter("monad.ws.active_connections")
            .with_description("Number of active websocket connections")
            .build();

        let active_websocket_topics = meter
            .i64_up_down_counter("monad.ws.active_topics")
            .with_description("Number of active websocket topics")
            .build();

        let execution_histogram = meter
            .f64_histogram("monad.rpc.execution_duration")
            .with_description("duration of the rpc method execution")
            .with_unit("s")
            .with_boundaries(LOW_US_TO_S.to_vec())
            .build();

        Self {
            provider,

            request_duration,
            active_requests,
            active_websocket_connections,
            active_websocket_topics,
            execution_histogram,
        }
    }

    /// Observable gauges for the trie-node cache on the TriedbEnv polling
    /// handle. Methods routed through EthCallHandler run against a separate
    /// cache in the C++ executor and are not counted here, nor are reads
    /// served from an archive.
    ///
    /// `snapshot` is polled on each collection, so the resolution is the
    /// reader's interval rather than any publishing cadence. It can run on any
    /// thread, which is why it is a closure over the cache's own counters
    /// rather than the triedb handle, which is `!Send`.
    pub fn register_triedb_node_cache(
        &self,
        snapshot: impl Fn() -> Option<NodeCacheStats> + Send + Sync + 'static,
    ) {
        let meter = self.provider.meter("opentelemetry");
        let snapshot = Arc::new(snapshot);

        for (name, description, field) in NODE_CACHE_GAUGES {
            let snapshot = snapshot.clone();
            meter
                .u64_observable_gauge(*name)
                .with_description(*description)
                .with_callback(move |observer| {
                    // Absent rather than zero when there is no cache to read:
                    // zero is what an idle one reports.
                    if let Some(stats) = snapshot() {
                        observer.observe(field(&stats), &[]);
                    }
                })
                .build();
        }
    }

    pub fn record_websocket_connection(&self, increment: i64) {
        self.active_websocket_connections.add(increment, &[]);
    }

    pub fn record_websocket_topic(&self, increment: i64) {
        self.active_websocket_topics.add(increment, &[]);
    }
}

#[cfg(test)]
mod node_cache_gauge_tests {
    use monad_triedb_utils::triedb_env::NodeCacheStats;
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData},
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    };

    use super::{Metrics, NODE_CACHE_GAUGES};

    // Distinct per field, so a gauge wired to the wrong accessor reports a
    // value that belongs to another gauge.
    fn stats() -> NodeCacheStats {
        NodeCacheStats {
            hits: 11,
            misses: 22,
            evictions: 33,
            used_bytes: 44,
            entries: 55,
            max_bytes: 66,
            max_entries: 77,
        }
    }

    // A gauge whose callback never fires is indistinguishable from one that
    // was never registered, and both are invisible until someone reads a
    // dashboard, so collect through the SDK rather than calling the
    // accessors directly.
    #[test]
    fn every_gauge_is_observed_with_its_own_field() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        Metrics::new_with_otel_provider(provider.clone())
            .register_triedb_node_cache(|| Some(stats()));
        provider.force_flush().expect("flushed");

        let collected = exporter.get_finished_metrics().expect("collected");
        let mut observed = Vec::new();
        for resource in &collected {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
                        continue;
                    };
                    for point in gauge.data_points() {
                        observed.push((metric.name().to_owned(), point.value()));
                    }
                }
            }
        }

        // Names are written out rather than read back from the table under
        // test: zipping the table against itself would assert only that some
        // name was observed, so a typo in a published name -- or two names
        // swapped between rows -- would satisfy it.
        let expected = [
            ("monad.rpc.triedb.node_cache_hits", 11),
            ("monad.rpc.triedb.node_cache_misses", 22),
            ("monad.rpc.triedb.node_cache_evictions", 33),
            ("monad.rpc.triedb.node_cache_used_bytes", 44),
            ("monad.rpc.triedb.node_cache_entries", 55),
            ("monad.rpc.triedb.node_cache_max_bytes", 66),
            ("monad.rpc.triedb.node_cache_max_entries", 77),
        ];
        assert_eq!(NODE_CACHE_GAUGES.len(), expected.len());
        for (name, value) in expected {
            assert!(
                observed.contains(&(name.to_owned(), value)),
                "{name} = {value} missing from {observed:?}"
            );
        }
    }
}
