use std::{
    collections::HashMap,
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::Stream;
use monad_consensus_types::{metrics::Metrics, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_executor::Executor;
use monad_executor_glue::{MetricsCommand, MetricsEvent, MonadEvent};
use opentelemetry_api::{
    metrics::{Counter, Meter, MeterProvider as _, Observer},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{data::Temporality, Aggregation, InstrumentKind, MeterProvider},
    Resource,
};
use tokio::task::{AbortHandle, JoinSet};
use tracing::warn;

type SharedMetrics = Arc<RwLock<Metrics>>;

/// A OpenTelemetry executor for recording metrics
pub struct OpenTelemetryExecutor<ST, SCT> {
    interval: Duration,
    meter_provider: MeterProvider,
    meter: Meter,
    // TODO(rene): refactor to use a Gauge type instead of `Counter<u64>`. right now, the metrics
    // will be summed together by the open telemetry backend leading to nonsensical metrics readings
    counters: HashMap<&'static str, Counter<u64>>,
    cached_metrics: Metrics,
    timers: JoinSet<Option<MetricsEvent>>,
    handle: Option<AbortHandle>,
    waker: Option<Waker>,
    phantom: PhantomData<(ST, SCT)>,
}

const COUNTERS: [&str; 48] = [
    "invalid_author",
    "not_well_formed_sig",
    "invalid_signature",
    "author_not_sender",
    "invalid_tc_round",
    "insufficient_stake",
    "invalid_seq_num",
    "val_data_unavailable",
    "invalid_vote_message",
    "invalid_version",
    "local_timeout",
    "handle_proposal",
    "failed_txn_validation",
    "invalid_proposal_round_leader",
    "out_of_order_proposals",
    "created_vote",
    "old_vote_received",
    "vote_received",
    "created_qc",
    "old_remote_timeout",
    "remote_timeout_msg",
    "remote_timeout_msg_with_tc",
    "created_tc",
    "process_old_qc",
    "process_qc",
    "creating_proposal",
    "abstain_proposal",
    "creating_empty_block_proposal",
    "rx_empty_block",
    "rx_execution_lagging",
    "rx_bad_state_root",
    "rx_missing_state_root",
    "rx_proposal",
    "proposal_with_tc",
    "failed_verify_randao_reveal_sig",
    "commit_empty_block",
    "committed_bytes",
    "state_root_update",
    "enter_new_round_qc",
    "enter_new_round_tc",
    "trigger_state_sync",
    "prune_success",
    "add_success",
    "add_dup",
    "blocksync_response_successful",
    "blocksync_response_failed",
    "blocksync_response_unexpected",
    "blocksync_request",
];

/// The following struct along with the `impl TemporalitySelector` are only to prevent a cumulative sum of counters
/// that is done behind the scenes, because the default temporality selector is `Temporality::Cumulative` which leads
/// to metric graphs that are difficult to interpret.
struct LatestValueTemporalitySelector;

impl opentelemetry_sdk::metrics::reader::TemporalitySelector for LatestValueTemporalitySelector {
    fn temporality(&self, _kind: InstrumentKind) -> Temporality {
        // A measurement interval that resets each cycle.
        // Measurements from one cycle are recorded independently, measurements from
        // other cycles do not affect them.
        Temporality::Delta
    }
}

struct AggregationSelector;

impl opentelemetry_sdk::metrics::reader::AggregationSelector for AggregationSelector {
    fn aggregation(&self, _kind: InstrumentKind) -> Aggregation {
        // An aggregation that summarizes a set of measurements as the last one made.
        Aggregation::LastValue
    }
}

fn observe_metric(observer: &dyn Observer, counter_name: &'static str, metrics: &Metrics) {}

impl<ST, SCT> OpenTelemetryExecutor<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    pub fn new(
        endpoint: impl Into<String>,
        service_name: String,
        interval: Duration,
        enable_grpc_gzip: bool,
    ) -> Self {
        let mut exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(endpoint.into());

        if enable_grpc_gzip {
            exporter = exporter.with_compression(opentelemetry_otlp::Compression::Gzip);
        }

        let meter_provider = opentelemetry_otlp::new_pipeline()
            .metrics(opentelemetry_sdk::runtime::Tokio)
            .with_period(interval)
            .with_exporter(exporter)
            // can uncomment these when using `ObservableGauge` instead of `Counter`
            // .with_temporality_selector(LatestValueTemporalitySelector {})
            // .with_aggregation_selector(AggregationSelector {})
            .with_resource(Resource::new(vec![KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name,
            )]))
            .build()
            .expect("failed to initialize opentelemetry_otlp metrics pipeline");

        let meter = meter_provider.meter("node");
        let counters = COUNTERS
            .into_iter()
            .map(|counter_name| (counter_name, meter.u64_counter(counter_name).init()))
            .collect();

        Self {
            interval,
            meter_provider,
            meter,
            counters,
            cached_metrics: Default::default(),
            timers: Default::default(),
            handle: None,
            waker: None,
            phantom: PhantomData,
        }
    }

    pub fn record(&mut self, name: &'static str, value: u64) {
        self.counters.get(name).unwrap().add(value, &[]);
    }

    fn record_metrics(&mut self, metrics: &Metrics) {
        // TODO(rene): must be a nice way to not rewrite all these fields.
        // we are recording the diff between `metrics` and `self.cached_metrics` to work around
        // the lack of a synchronous gauge counter
        let cached_metrics = self.cached_metrics;
        self.record(
            "invalid_author",
            metrics.validation_errors.invalid_author
                - cached_metrics.validation_errors.invalid_author,
        );
        self.record(
            "not_well_formed_sig",
            metrics.validation_errors.not_well_formed_sig
                - cached_metrics.validation_errors.not_well_formed_sig,
        );
        self.record(
            "invalid_signature",
            metrics.validation_errors.invalid_signature
                - cached_metrics.validation_errors.invalid_signature,
        );
        self.record(
            "author_not_sender",
            metrics.validation_errors.author_not_sender
                - cached_metrics.validation_errors.author_not_sender,
        );
        self.record(
            "invalid_tc_round",
            metrics.validation_errors.invalid_tc_round
                - cached_metrics.validation_errors.invalid_tc_round,
        );
        self.record(
            "insufficient_stake",
            metrics.validation_errors.insufficient_stake
                - cached_metrics.validation_errors.insufficient_stake,
        );
        self.record(
            "invalid_seq_num",
            metrics.validation_errors.invalid_seq_num
                - cached_metrics.validation_errors.invalid_seq_num,
        );
        self.record(
            "val_data_unavailable",
            metrics.validation_errors.val_data_unavailable
                - cached_metrics.validation_errors.val_data_unavailable,
        );
        self.record(
            "invalid_vote_message",
            metrics.validation_errors.invalid_vote_message
                - cached_metrics.validation_errors.invalid_vote_message,
        );
        self.record(
            "invalid_version",
            metrics.validation_errors.invalid_version
                - cached_metrics.validation_errors.invalid_version,
        );
        self.record(
            "local_timeout",
            metrics.consensus_events.local_timeout - cached_metrics.consensus_events.local_timeout,
        );
        self.record(
            "handle_proposal",
            metrics.consensus_events.handle_proposal
                - cached_metrics.consensus_events.handle_proposal,
        );
        self.record(
            "failed_txn_validation",
            metrics.consensus_events.failed_txn_validation
                - cached_metrics.consensus_events.failed_txn_validation,
        );
        self.record(
            "invalid_proposal_round_leader",
            metrics.consensus_events.invalid_proposal_round_leader
                - cached_metrics
                    .consensus_events
                    .invalid_proposal_round_leader,
        );
        self.record(
            "out_of_order_proposals",
            metrics.consensus_events.out_of_order_proposals
                - cached_metrics.consensus_events.out_of_order_proposals,
        );
        self.record(
            "created_vote",
            metrics.consensus_events.created_vote - cached_metrics.consensus_events.created_vote,
        );
        self.record(
            "old_vote_received",
            metrics.consensus_events.old_vote_received
                - cached_metrics.consensus_events.old_vote_received,
        );
        self.record(
            "vote_received",
            metrics.consensus_events.vote_received - cached_metrics.consensus_events.vote_received,
        );
        self.record(
            "created_qc",
            metrics.consensus_events.created_qc - cached_metrics.consensus_events.created_qc,
        );
        self.record(
            "old_remote_timeout",
            metrics.consensus_events.old_remote_timeout
                - cached_metrics.consensus_events.old_remote_timeout,
        );
        self.record(
            "remote_timeout_msg",
            metrics.consensus_events.remote_timeout_msg
                - cached_metrics.consensus_events.remote_timeout_msg,
        );
        self.record(
            "remote_timeout_msg_with_tc",
            metrics.consensus_events.remote_timeout_msg_with_tc
                - cached_metrics.consensus_events.remote_timeout_msg_with_tc,
        );
        self.record(
            "created_tc",
            metrics.consensus_events.created_tc - cached_metrics.consensus_events.created_tc,
        );
        self.record(
            "process_old_qc",
            metrics.consensus_events.process_old_qc
                - cached_metrics.consensus_events.process_old_qc,
        );
        self.record(
            "process_qc",
            metrics.consensus_events.process_qc - cached_metrics.consensus_events.process_qc,
        );
        self.record(
            "creating_proposal",
            metrics.consensus_events.creating_proposal
                - cached_metrics.consensus_events.creating_proposal,
        );
        self.record(
            "abstain_proposal",
            metrics.consensus_events.abstain_proposal
                - cached_metrics.consensus_events.abstain_proposal,
        );
        self.record(
            "creating_empty_block_proposal",
            metrics.consensus_events.creating_empty_block_proposal
                - cached_metrics
                    .consensus_events
                    .creating_empty_block_proposal,
        );
        self.record(
            "rx_empty_block",
            metrics.consensus_events.rx_empty_block
                - cached_metrics.consensus_events.rx_empty_block,
        );
        self.record(
            "rx_execution_lagging",
            metrics.consensus_events.rx_execution_lagging
                - cached_metrics.consensus_events.rx_execution_lagging,
        );
        self.record(
            "rx_bad_state_root",
            metrics.consensus_events.rx_bad_state_root
                - cached_metrics.consensus_events.rx_bad_state_root,
        );
        self.record(
            "rx_missing_state_root",
            metrics.consensus_events.rx_missing_state_root
                - cached_metrics.consensus_events.rx_missing_state_root,
        );
        self.record(
            "rx_proposal",
            metrics.consensus_events.rx_proposal - cached_metrics.consensus_events.rx_proposal,
        );
        self.record(
            "proposal_with_tc",
            metrics.consensus_events.proposal_with_tc
                - cached_metrics.consensus_events.proposal_with_tc,
        );
        self.record(
            "failed_verify_randao_reveal_sig",
            metrics.consensus_events.failed_verify_randao_reveal_sig
                - cached_metrics
                    .consensus_events
                    .failed_verify_randao_reveal_sig,
        );
        self.record(
            "commit_empty_block",
            metrics.consensus_events.commit_empty_block
                - cached_metrics.consensus_events.commit_empty_block,
        );
        self.record(
            "committed_bytes",
            metrics.consensus_events.committed_bytes
                - cached_metrics.consensus_events.committed_bytes,
        );
        self.record(
            "state_root_update",
            metrics.consensus_events.state_root_update
                - cached_metrics.consensus_events.state_root_update,
        );
        self.record(
            "enter_new_round_qc",
            metrics.consensus_events.enter_new_round_qc
                - cached_metrics.consensus_events.enter_new_round_qc,
        );
        self.record(
            "enter_new_round_tc",
            metrics.consensus_events.enter_new_round_tc
                - cached_metrics.consensus_events.enter_new_round_tc,
        );
        self.record(
            "trigger_state_sync",
            metrics.consensus_events.trigger_state_sync
                - cached_metrics.consensus_events.trigger_state_sync,
        );
        self.record(
            "prune_success",
            metrics.blocktree_events.prune_success - cached_metrics.blocktree_events.prune_success,
        );
        self.record(
            "add_success",
            metrics.blocktree_events.add_success - cached_metrics.blocktree_events.add_success,
        );
        self.record(
            "add_dup",
            metrics.blocktree_events.add_dup - cached_metrics.blocktree_events.add_dup,
        );
        self.record(
            "blocksync_response_successful",
            metrics.blocksync_events.blocksync_response_successful
                - cached_metrics
                    .blocksync_events
                    .blocksync_response_successful,
        );
        self.record(
            "blocksync_response_failed",
            metrics.blocksync_events.blocksync_response_failed
                - cached_metrics.blocksync_events.blocksync_response_failed,
        );
        self.record(
            "blocksync_response_unexpected",
            metrics.blocksync_events.blocksync_response_unexpected
                - cached_metrics
                    .blocksync_events
                    .blocksync_response_unexpected,
        );
        self.record(
            "blocksync_request",
            metrics.blocksync_events.blocksync_request
                - cached_metrics.blocksync_events.blocksync_request,
        );
        self.cached_metrics = *metrics;
    }
}

impl<ST, SCT> Drop for OpenTelemetryExecutor<ST, SCT> {
    fn drop(&mut self) {
        if self.meter_provider.shutdown().is_err() {
            warn!("opentelemetry meter provider already shut down");
        }
    }
}

impl<ST, SCT> Executor for OpenTelemetryExecutor<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    type Command = MetricsCommand;

    fn replay(&mut self, mut _commands: Vec<Self::Command>) {}

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;
        for command in commands {
            match command {
                MetricsCommand::RecordMetrics(metrics) => {
                    wake = true;
                    let interval = self.interval;
                    self.record_metrics(&metrics);
                    let future = async move {
                        tokio::time::sleep(interval).await;
                        Some(MetricsEvent::Timeout)
                    };
                    let handle = self.timers.spawn(future);
                    if let Some(old_handle) = self.handle.replace(handle) {
                        old_handle.abort();
                    }
                }
            }
        }
        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }
}

impl<ST, SCT> Stream for OpenTelemetryExecutor<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut _otel_poll_span = tracing::trace_span!("otel_poll_span").entered();

        let this = self.deref_mut();

        // its possible to get Poll::Ready(None) because the join_set might be empty
        while let Poll::Ready(Some(poll_result)) = this.timers.poll_join_next(cx) {
            match poll_result {
                Ok(e) => {
                    return Poll::Ready(e.and_then(|e| Some(MonadEvent::MetricsEvent(e))));
                }
                Err(join_error) => {
                    // only case where this happen is when task is aborted
                    assert!(join_error.is_cancelled());
                }
            };
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
