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

use std::sync::{
    atomic::{AtomicBool, Ordering},
    RwLock,
};

use actix_server::Server;
use actix_web::{http::header, web, App, HttpRequest, HttpResponse, HttpServer};
use futures::join;
use opentelemetry::metrics::{Gauge, MeterProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};
use prometheus::{Encoder, IntGauge, Registry, TextEncoder};

use super::*;

#[derive(Default)]
pub struct Metrics {
    pub total_txs_sent: AtomicUsize,
    pub total_rpc_calls: AtomicUsize,
    pub total_committed_txs: AtomicUsize,

    pub receipts_rpc_calls: AtomicUsize,
    pub receipts_rpc_calls_error: AtomicUsize,
    pub receipts_tx_success: AtomicUsize,
    pub receipts_tx_failure: AtomicUsize,
    pub receipts_contracts_deployed: AtomicUsize,
    pub receipts_gas_consumed: Arc<RwLock<U256>>,

    pub logs_rpc_calls: AtomicUsize,
    pub logs_rpc_calls_error: AtomicUsize,
    pub logs_total: AtomicUsize,
    // pub logs_erc20_transfers: AtomicUsize,
    // pub logs_erc20_total_value_transfered: Arc<RwLock<U256>>,

    // pub txs_by_hash_rpc_calls: AtomicUsize,
    // pub txs_by_hash_rpc_calls_error: AtomicUsize,
}

impl Metrics {
    pub async fn run(self: Arc<Metrics>, shutdown: Arc<AtomicBool>) {
        let secs_5 = self.metrics_at_timestep(Duration::from_secs(5), Arc::clone(&shutdown));
        let min_1 = self.metrics_at_timestep(Duration::from_secs(60), Arc::clone(&shutdown));
        let min_60 = self.metrics_at_timestep(Duration::from_secs(60 * 60), Arc::clone(&shutdown));

        join!(secs_5, min_1, min_60);
    }

    async fn metrics_at_timestep(&self, report_interval: Duration, shutdown: Arc<AtomicBool>) {
        let mut report_interval = tokio::time::interval(report_interval);
        report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = Instant::now();

        // Basic metrics
        let mut txs_sent = Rate::new(&self.total_txs_sent);
        let mut rpc_calls = Rate::new(&self.total_rpc_calls);
        let mut committed_txs = Rate::new(&self.total_committed_txs);

        // Receipt metrics
        let mut receipts_rpc_calls = Rate::new(&self.receipts_rpc_calls);
        let mut receipts_rpc_calls_error = Rate::new(&self.receipts_rpc_calls_error);
        let mut receipts_tx_success = Rate::new(&self.receipts_tx_success);
        let mut receipts_tx_failure = Rate::new(&self.receipts_tx_failure);
        let mut receipts_contracts_deployed = Rate::new(&self.receipts_contracts_deployed);
        // let mut receipts_gas_consumed = Rate::new(&self.receipts_gas_consumed);

        // Logs metrics
        let mut logs_rpc_calls = Rate::new(&self.logs_rpc_calls);
        let mut logs_rpc_calls_error = Rate::new(&self.logs_rpc_calls_error);
        let mut logs_total = Rate::new(&self.logs_total);

        loop {
            if shutdown.load(Ordering::Relaxed) {
                warn!("Metrics shutting down");
                break;
            }

            let now = report_interval.tick().await;
            let elapsed = last.elapsed().as_secs_f64();

            // Note: can't call rate twice, so must make var if used twice
            let tx_success_ps = receipts_tx_success.rate(elapsed);

            info!(
                sent_txs = txs_sent.val(),
                committed_txs = committed_txs.val(),
                rps = rpc_calls.rate(elapsed),
                tx_success_ps,
                committed_tps = committed_txs.rate(elapsed),
                tps = txs_sent.rate(elapsed),
                "Metrics          (freq: {}m:{}s)",
                report_interval.period().as_secs() / 60,
                report_interval.period().as_secs() % 60
            );

            info!(
                contracts_deployed = receipts_contracts_deployed.val(),
                tx_success = receipts_tx_success.val(),
                tx_failure = receipts_tx_failure.val(),
                rpc_calls = receipts_rpc_calls.val(),
                rpc_calls_error = receipts_rpc_calls_error.val(),
                contracts_deployed_ps = receipts_contracts_deployed.rate(elapsed),
                rpc_calls_error_ps = receipts_rpc_calls_error.rate(elapsed),
                rpc_calls_ps = receipts_rpc_calls.rate(elapsed),
                tx_failure_ps = receipts_tx_failure.rate(elapsed),
                tx_success_ps,
                "Metrics Receipts (freq: {}m:{}s)",
                report_interval.period().as_secs() / 60,
                report_interval.period().as_secs() % 60
            );

            info!(
                rpc_calls = logs_rpc_calls.val(),
                rpc_calls_error = logs_rpc_calls_error.val(),
                total = logs_total.val(),
                rpc_calls_ps = logs_rpc_calls.rate(elapsed),
                rpc_calls_error_ps = logs_rpc_calls_error.rate(elapsed),
                total_ps = logs_total.rate(elapsed),
                "Metrics Logs     (freq: {}m:{}s)",
                report_interval.period().as_secs() / 60,
                report_interval.period().as_secs() % 60
            );
            last = now;
        }
    }
}

struct Rate<'a> {
    val: &'a AtomicUsize,
    prev: usize,
}

impl<'a> Rate<'a> {
    fn new(val: &'a AtomicUsize) -> Rate<'a> {
        Rate {
            val,
            prev: val.load(SeqCst),
        }
    }

    fn rate(&mut self, elapsed: f64) -> usize {
        let curr = self.val();
        let diff = curr - self.prev;
        let raw = (diff) as f64 / elapsed;
        self.prev = curr;
        raw.round() as usize
    }

    fn val(&self) -> usize {
        self.val.load(SeqCst)
    }
}

pub struct MetricsReporter {
    metrics: Arc<Metrics>,
    gen_mode: String,

    // Gauges
    committed_tps: Gauge<u64>,
    sent_tps: Gauge<u64>,
    rpc_calls_ps: Gauge<u64>,
    rpc_calls_error_ps: Gauge<u64>,
    contracts_deployed_ps: Gauge<u64>,
    total_transactions: Gauge<u64>,
    total_contracts_created: Gauge<u64>,

    // Optional Prometheus pull metrics (mirrors OTel gauges)
    prom: Option<PrometheusMetrics>,

    // Keeping these so they don't get dropped
    _provider: SdkMeterProvider,
    _meter: opentelemetry::metrics::Meter,
}

struct Rates<'a> {
    txs_sent: Rate<'a>,
    rpc_calls: Rate<'a>,
    committed_txs: Rate<'a>,
    rpc_calls_error: Rate<'a>,
    contracts_deployed: Rate<'a>,
}

impl MetricsReporter {
    pub fn new(
        metrics: Arc<Metrics>,
        otel_endpoint: Option<impl AsRef<str>>,
        otel_replica_name: String,
        gen_mode: String,
        prom: Option<PrometheusMetrics>,
    ) -> Result<Self> {
        let provider = build_otel_meter_provider(
            otel_endpoint,
            "txgen".to_string(),
            otel_replica_name,
            Duration::from_secs(5),
        )?;
        let meter = provider.meter("opentelemetry");

        let reporter = Self {
            metrics: metrics.clone(),
            gen_mode,

            committed_tps: meter.u64_gauge("committed_tps").build(),
            sent_tps: meter.u64_gauge("sent_tps").build(),
            rpc_calls_ps: meter.u64_gauge("rpc_calls_ps").build(),
            rpc_calls_error_ps: meter.u64_gauge("rpc_calls_error_ps").build(),
            contracts_deployed_ps: meter.u64_gauge("contracts_deployed_ps").build(),
            total_transactions: meter.u64_gauge("total_transactions").build(),
            total_contracts_created: meter.u64_gauge("total_contracts_created").build(),

            prom,

            _provider: provider,
            _meter: meter,
        };

        // Report for all metrics to prevent "No data"
        reporter.report_metrics(
            0.1,
            // TODO: Make this cleaner
            &mut Rates {
                txs_sent: Rate::new(&metrics.total_txs_sent),
                rpc_calls: Rate::new(&metrics.total_rpc_calls),
                committed_txs: Rate::new(&metrics.total_committed_txs),
                rpc_calls_error: Rate::new(&metrics.receipts_rpc_calls_error),
                contracts_deployed: Rate::new(&metrics.receipts_contracts_deployed),
            },
        );

        Ok(reporter)
    }

    pub async fn run(self, shutdown: Arc<AtomicBool>) {
        let mut report_interval = tokio::time::interval(Duration::from_secs(5));
        report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = Instant::now();

        let mut rates = Rates {
            txs_sent: Rate::new(&self.metrics.total_txs_sent),
            rpc_calls: Rate::new(&self.metrics.total_rpc_calls),
            committed_txs: Rate::new(&self.metrics.total_committed_txs),
            rpc_calls_error: Rate::new(&self.metrics.receipts_rpc_calls_error),
            contracts_deployed: Rate::new(&self.metrics.receipts_contracts_deployed),
        };

        loop {
            if shutdown.load(Ordering::Relaxed) {
                warn!("MetricsReporter shutting down");
                break;
            }

            let now = report_interval.tick().await;
            let elapsed = last.elapsed().as_secs_f64();

            self.report_metrics(elapsed, &mut rates);
            last = now;
        }
    }

    fn report_metrics(&self, elapsed: f64, rates: &mut Rates) {
        debug!("Reporting Otel Metrics");

        let committed_tps_val = rates.committed_txs.rate(elapsed) as u64;
        let sent_tps_val = rates.txs_sent.rate(elapsed) as u64;
        let rpc_calls_ps_val = rates.rpc_calls.rate(elapsed) as u64;
        let rpc_calls_error_ps_val = rates.rpc_calls_error.rate(elapsed) as u64;
        let contracts_deployed_ps_val = rates.contracts_deployed.rate(elapsed) as u64;
        let total_transactions_val = rates.txs_sent.val() as u64;
        let total_contracts_created_val = rates.contracts_deployed.val() as u64;

        let gen_mode_label = &[opentelemetry::KeyValue::new(
            "Generator Mode",
            self.gen_mode.clone(),
        )];

        self.committed_tps.record(committed_tps_val, gen_mode_label);
        self.sent_tps.record(sent_tps_val, gen_mode_label);
        self.rpc_calls_ps.record(rpc_calls_ps_val, &[]);
        self.rpc_calls_error_ps.record(rpc_calls_error_ps_val, &[]);
        self.contracts_deployed_ps
            .record(contracts_deployed_ps_val, &[]);
        self.total_transactions.record(total_transactions_val, &[]);
        self.total_contracts_created
            .record(total_contracts_created_val, &[]);

        // Mirror to Prometheus gauges when enabled
        if let Some(prom) = &self.prom {
            prom.committed_tps.set(committed_tps_val as i64);
            prom.sent_tps.set(sent_tps_val as i64);
            prom.rpc_calls_ps.set(rpc_calls_ps_val as i64);
            prom.rpc_calls_error_ps.set(rpc_calls_error_ps_val as i64);
            prom.contracts_deployed_ps
                .set(contracts_deployed_ps_val as i64);
            prom.total_transactions.set(total_transactions_val as i64);
            prom.total_contracts_created
                .set(total_contracts_created_val as i64);
        }

        info!("Otel Metrics Reported");
    }
}

// ---------------------------------------------------------------------------
// Prometheus pull metrics
// ---------------------------------------------------------------------------

/// Prometheus gauges that mirror the OTel gauges so Prometheus can scrape them.
#[derive(Clone)]
pub struct PrometheusMetrics {
    pub registry: Registry,
    pub committed_tps: IntGauge,
    pub sent_tps: IntGauge,
    pub rpc_calls_ps: IntGauge,
    pub rpc_calls_error_ps: IntGauge,
    pub contracts_deployed_ps: IntGauge,
    pub total_transactions: IntGauge,
    pub total_contracts_created: IntGauge,
}

impl PrometheusMetrics {
    pub fn new() -> Result<Self> {
        let registry = Registry::new_custom(Some("txgen".to_string()), None)
            .map_err(|e| eyre::eyre!("Failed to create prometheus registry: {e}"))?;

        let committed_tps = IntGauge::new("committed_tps", "Committed transactions per second")
            .map_err(|e| eyre::eyre!("{e}"))?;
        let sent_tps = IntGauge::new("sent_tps", "Sent transactions per second")
            .map_err(|e| eyre::eyre!("{e}"))?;
        let rpc_calls_ps = IntGauge::new("rpc_calls_ps", "RPC calls per second")
            .map_err(|e| eyre::eyre!("{e}"))?;
        let rpc_calls_error_ps = IntGauge::new("rpc_calls_error_ps", "RPC call errors per second")
            .map_err(|e| eyre::eyre!("{e}"))?;
        let contracts_deployed_ps =
            IntGauge::new("contracts_deployed_ps", "Contracts deployed per second")
                .map_err(|e| eyre::eyre!("{e}"))?;
        let total_transactions = IntGauge::new("total_transactions", "Total transactions sent")
            .map_err(|e| eyre::eyre!("{e}"))?;
        let total_contracts_created =
            IntGauge::new("total_contracts_created", "Total contracts created")
                .map_err(|e| eyre::eyre!("{e}"))?;

        for gauge in [
            &committed_tps,
            &sent_tps,
            &rpc_calls_ps,
            &rpc_calls_error_ps,
            &contracts_deployed_ps,
            &total_transactions,
            &total_contracts_created,
        ] {
            registry
                .register(Box::new(gauge.clone()))
                .map_err(|e| eyre::eyre!("Failed to register prometheus gauge: {e}"))?;
        }

        Ok(Self {
            registry,
            committed_tps,
            sent_tps,
            rpc_calls_ps,
            rpc_calls_error_ps,
            contracts_deployed_ps,
            total_transactions,
            total_contracts_created,
        })
    }
}

async fn handle_metrics(_request: HttpRequest, state: web::Data<Registry>) -> HttpResponse {
    let metric_families = state.gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    if encoder.encode(&metric_families, &mut buffer).is_err() {
        return HttpResponse::InternalServerError().finish();
    }

    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, prometheus::TEXT_FORMAT))
        .body(buffer)
}

pub fn start_metrics_server(addr: String, registry: Registry) -> std::io::Result<Server> {
    info!("Starting Prometheus metrics server on {addr}");
    Ok(HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(registry.clone()))
            .route("/metrics", web::get().to(handle_metrics))
    })
    .bind(addr)?
    .workers(1)
    .run())
}

// ---------------------------------------------------------------------------
// OTel push metrics
// ---------------------------------------------------------------------------

fn build_otel_meter_provider(
    otel_endpoint: Option<impl AsRef<str>>,
    service_name: String,
    replica_name: String,
    interval: Duration,
) -> Result<SdkMeterProvider> {
    let mut provider_builder = SdkMeterProvider::builder().with_resource(
        opentelemetry_sdk::Resource::builder_empty()
            .with_attributes(vec![opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                format!("{replica_name}-{service_name}"),
            )])
            .build(),
    );

    if let Some(otel_endpoint) = otel_endpoint {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(otel_endpoint.as_ref())
            .with_timeout(interval * 2)
            .with_temporality(Temporality::default())
            .build()?;

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
            .with_interval(interval / 2)
            .build();

        provider_builder = provider_builder.with_reader(reader)
    }

    Ok(provider_builder.build())
}
