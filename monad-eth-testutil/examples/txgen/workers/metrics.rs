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

use futures::join;
use opentelemetry::metrics::{Gauge, MeterProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};

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

    pub indexer_blocks_indexed: AtomicUsize,
    pub indexer_blocks_failed: AtomicUsize,
    pub indexer_total_duration_ms: Arc<RwLock<u64>>,

    pub ws_messages_sent: AtomicUsize,
    pub ws_messages_received: AtomicUsize,
    pub wallet_comparison_attempts: AtomicUsize,
    pub wallet_comparison_mismatches: AtomicUsize,

    pub rpcws_blocks_compared: AtomicUsize,
    pub rpcws_header_mismatches: AtomicUsize,
    pub rpcws_block_number_mismatches: AtomicUsize,
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

        // Indexer workflow metrics
        let mut indexer_blocks_indexed = Rate::new(&self.indexer_blocks_indexed);
        let mut indexer_blocks_failed = Rate::new(&self.indexer_blocks_failed);

        // Wallet workflow metrics
        let mut wallet_comparison_attempts = Rate::new(&self.wallet_comparison_attempts);
        let mut wallet_comparison_mismatches = Rate::new(&self.wallet_comparison_mismatches);

        // RpcWsCompare metrics
        let mut rpcws_blocks_compared = Rate::new(&self.rpcws_blocks_compared);
        let mut rpcws_header_mismatches = Rate::new(&self.rpcws_header_mismatches);
        let rpcws_block_number_mismatches = Rate::new(&self.rpcws_block_number_mismatches);

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

            info!(
                blocks_indexed = indexer_blocks_indexed.val(),
                blocks_failed = indexer_blocks_failed.val(),
                total_duration_ms = *self.indexer_total_duration_ms.read().unwrap(),
                blocks_indexed_ps = indexer_blocks_indexed.rate(elapsed),
                blocks_failed_ps = indexer_blocks_failed.rate(elapsed),
                "Metrics RpcRequestGen -> Indexer (freq: {}m:{}s)",
                report_interval.period().as_secs() / 60,
                report_interval.period().as_secs() % 60
            );

            info!(
                comparison_attempts = wallet_comparison_attempts.val(),
                comparison_mismatches = wallet_comparison_mismatches.val(),
                comparison_attempts_ps = wallet_comparison_attempts.rate(elapsed),
                comparison_mismatches_ps = wallet_comparison_mismatches.rate(elapsed),
                "Metrics RpcRequestGen -> Wallet (freq: {}m:{}s)",
                report_interval.period().as_secs() / 60,
                report_interval.period().as_secs() % 60
            );

            info!(
                blocks_compared = rpcws_blocks_compared.val(),
                header_mismatches = rpcws_header_mismatches.val(),
                block_number_mismatches = rpcws_block_number_mismatches.val(),
                blocks_compared_ps = rpcws_blocks_compared.rate(elapsed),
                header_mismatches_ps = rpcws_header_mismatches.rate(elapsed),
                "Metrics RpcRequestGen -> RpcWsCompare (freq: {}m:{}s)",
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

    // Indexer workflow gauges
    indexer_blocks_indexed_ps: Gauge<u64>,
    indexer_blocks_failed_ps: Gauge<u64>,

    // Wallet workflow gauges
    wallet_comparison_attempts_ps: Gauge<u64>,
    wallet_comparison_mismatches_ps: Gauge<u64>,

    // RpcWsCompare gauges
    rpcws_blocks_compared_ps: Gauge<u64>,
    rpcws_header_mismatches_ps: Gauge<u64>,

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

    indexer_blocks_indexed: Rate<'a>,
    indexer_blocks_failed: Rate<'a>,

    wallet_comparison_attempts: Rate<'a>,
    wallet_comparison_mismatches: Rate<'a>,

    rpcws_blocks_compared: Rate<'a>,
    rpcws_header_mismatches: Rate<'a>,
}

impl MetricsReporter {
    pub fn new(
        metrics: Arc<Metrics>,
        otel_endpoint: Option<impl AsRef<str>>,
        otel_replica_name: String,
        gen_mode: String,
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

            indexer_blocks_indexed_ps: meter.u64_gauge("indexer_blocks_indexed_ps").build(),
            indexer_blocks_failed_ps: meter.u64_gauge("indexer_blocks_failed_ps").build(),

            wallet_comparison_attempts_ps: meter.u64_gauge("wallet_comparison_attempts_ps").build(),
            wallet_comparison_mismatches_ps: meter.u64_gauge("wallet_comparison_mismatches_ps").build(),
            wallet_comparison_errors_ps: meter.u64_gauge("wallet_comparison_errors_ps").build(),

            rpcws_blocks_compared_ps: meter.u64_gauge("rpcws_blocks_compared_ps").build(),
            rpcws_header_mismatches_ps: meter.u64_gauge("rpcws_header_mismatches_ps").build(),

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
                indexer_blocks_indexed: Rate::new(&metrics.indexer_blocks_indexed),
                indexer_blocks_failed: Rate::new(&metrics.indexer_blocks_failed),
                wallet_comparison_attempts: Rate::new(&metrics.wallet_comparison_attempts),
                wallet_comparison_mismatches: Rate::new(&metrics.wallet_comparison_mismatches),
                rpcws_blocks_compared: Rate::new(&metrics.rpcws_blocks_compared),
                rpcws_header_mismatches: Rate::new(&metrics.rpcws_header_mismatches),
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
            indexer_blocks_indexed: Rate::new(&self.metrics.indexer_blocks_indexed),
            indexer_blocks_failed: Rate::new(&self.metrics.indexer_blocks_failed),
            wallet_comparison_attempts: Rate::new(&self.metrics.wallet_comparison_attempts),
            wallet_comparison_mismatches: Rate::new(&self.metrics.wallet_comparison_mismatches),
            rpcws_blocks_compared: Rate::new(&self.metrics.rpcws_blocks_compared),
            rpcws_header_mismatches: Rate::new(&self.metrics.rpcws_header_mismatches),
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

        self.committed_tps.record(
            rates.committed_txs.rate(elapsed) as u64,
            &[opentelemetry::KeyValue::new(
                "Generator Mode",
                self.gen_mode.clone(),
            )],
        );
        self.sent_tps.record(
            rates.txs_sent.rate(elapsed) as u64,
            &[opentelemetry::KeyValue::new(
                "Generator Mode",
                self.gen_mode.clone(),
            )],
        );
        self.rpc_calls_ps
            .record(rates.rpc_calls.rate(elapsed) as u64, &[]);
        self.rpc_calls_error_ps
            .record(rates.rpc_calls_error.rate(elapsed) as u64, &[]);
        self.contracts_deployed_ps
            .record(rates.contracts_deployed.rate(elapsed) as u64, &[]);

        self.total_transactions
            .record(rates.txs_sent.val() as u64, &[]);
        self.total_contracts_created
            .record(rates.contracts_deployed.val() as u64, &[]);

        self.indexer_blocks_indexed_ps
            .record(rates.indexer_blocks_indexed.rate(elapsed) as u64, &[]);
        self.indexer_blocks_failed_ps
            .record(rates.indexer_blocks_failed.rate(elapsed) as u64, &[]);

        self.wallet_comparison_attempts_ps
            .record(rates.wallet_comparison_attempts.rate(elapsed) as u64, &[]);
        self.wallet_comparison_mismatches_ps
            .record(rates.wallet_comparison_mismatches.rate(elapsed) as u64, &[]);

        self.rpcws_blocks_compared_ps
            .record(rates.rpcws_blocks_compared.rate(elapsed) as u64, &[]);
        self.rpcws_header_mismatches_ps
            .record(rates.rpcws_header_mismatches.rate(elapsed) as u64, &[]);

        info!("Otel Metrics Reported");
    }
}

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
