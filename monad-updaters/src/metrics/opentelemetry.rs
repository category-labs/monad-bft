use std::{
    collections::HashMap,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::Stream;
use monad_consensus_types::metrics::Metrics;
use monad_executor::Executor;
use monad_executor_glue::MetricsCommand;
use monad_types::TimeoutVariant;
use opentelemetry_api::{
    metrics::{Counter, Meter, MeterProvider as _},
    KeyValue,
};
use opentelemetry_sdk::{
    metrics::{data::ResourceMetrics, exporter::PushMetricsExporter, ManualReader, MeterProvider},
    Resource,
};
use opentelemetry_stdout::MetricsExporter;
use tokio::task::{AbortHandle, JoinSet};

/// A OpenTelemetry executor for recording metrics
pub struct OpenTelemetryExecutor<E> {
    meter_provider: MeterProvider,
    exporter: MetricsExporter,
    meter: Meter,
    counters: HashMap<&'static str, Counter<u64>>,
    timers: JoinSet<Option<E>>,
    aborts: HashMap<TimeoutVariant, AbortHandle>,
    waker: Option<Waker>,
    _phantom: PhantomData<E>,
}

const COUNTERS: [&str; 46] = [
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
    "state_root_update",
    "enter_new_round_qc",
    "enter_new_round_tc",
    "prune_success",
    "add_success",
    "add_dup",
    "blocksync_response_successful",
    "blocksync_response_failed",
    "blocksync_response_unexpected",
    "blocksync_request",
];

impl<E> OpenTelemetryExecutor<E> {
    pub fn new(interval: Duration) -> Self {
        let exporter = opentelemetry_stdout::MetricsExporterBuilder::default()
            .with_encoder(|writer, data| {
                serde_json::to_writer_pretty(writer, &data).unwrap();
                Ok(())
            })
            .build();
        let reader = ManualReader::builder().build();

        let meter_provider = MeterProvider::builder()
            .with_reader(reader)
            .with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                "metrics-basic-example",
            )]))
            .build();
        let meter = meter_provider.meter("node");
        let counters = COUNTERS
            .into_iter()
            .map(|counter_name| (counter_name, meter.u64_counter(counter_name).init()))
            .collect();
        Self {
            meter_provider,
            exporter,
            meter,
            counters,
            timers: Default::default(),
            aborts: Default::default(),
            waker: None,
            _phantom: PhantomData,
        }
    }

    pub fn record(&mut self, name: &'static str, value: u64) {
        self.counters.get(name).unwrap().add(value, &[]);
    }

    fn record_metrics(&mut self, metrics: &Metrics) {
        // TODO(rene): must be a nice way to not rewrite all these fields.
        self.record("invalid_author", metrics.validation_errors.invalid_author);
        self.record(
            "not_well_formed_sig",
            metrics.validation_errors.not_well_formed_sig,
        );
        self.record(
            "invalid_signature",
            metrics.validation_errors.invalid_signature,
        );
        self.record(
            "author_not_sender",
            metrics.validation_errors.author_not_sender,
        );
        self.record(
            "invalid_tc_round",
            metrics.validation_errors.invalid_tc_round,
        );
        self.record(
            "insufficient_stake",
            metrics.validation_errors.insufficient_stake,
        );
        self.record("invalid_seq_num", metrics.validation_errors.invalid_seq_num);
        self.record(
            "val_data_unavailable",
            metrics.validation_errors.val_data_unavailable,
        );
        self.record(
            "invalid_vote_message",
            metrics.validation_errors.invalid_vote_message,
        );
        self.record("invalid_version", metrics.validation_errors.invalid_version);
        self.record("local_timeout", metrics.consensus_events.local_timeout);
        self.record("handle_proposal", metrics.consensus_events.handle_proposal);
        self.record(
            "failed_txn_validation",
            metrics.consensus_events.failed_txn_validation,
        );
        self.record(
            "invalid_proposal_round_leader",
            metrics.consensus_events.invalid_proposal_round_leader,
        );
        self.record(
            "out_of_order_proposals",
            metrics.consensus_events.out_of_order_proposals,
        );
        self.record("created_vote", metrics.consensus_events.created_vote);
        self.record(
            "old_vote_received",
            metrics.consensus_events.old_vote_received,
        );
        self.record("vote_received", metrics.consensus_events.vote_received);
        self.record("created_qc", metrics.consensus_events.created_qc);
        self.record(
            "old_remote_timeout",
            metrics.consensus_events.old_remote_timeout,
        );
        self.record(
            "remote_timeout_msg",
            metrics.consensus_events.remote_timeout_msg,
        );
        self.record(
            "remote_timeout_msg_with_tc",
            metrics.consensus_events.remote_timeout_msg_with_tc,
        );
        self.record("created_tc", metrics.consensus_events.created_tc);
        self.record("process_old_qc", metrics.consensus_events.process_old_qc);
        self.record("process_qc", metrics.consensus_events.process_qc);
        self.record(
            "creating_proposal",
            metrics.consensus_events.creating_proposal,
        );
        self.record(
            "abstain_proposal",
            metrics.consensus_events.abstain_proposal,
        );
        self.record(
            "creating_empty_block_proposal",
            metrics.consensus_events.creating_empty_block_proposal,
        );
        self.record("rx_empty_block", metrics.consensus_events.rx_empty_block);
        self.record(
            "rx_execution_lagging",
            metrics.consensus_events.rx_execution_lagging,
        );
        self.record(
            "rx_bad_state_root",
            metrics.consensus_events.rx_bad_state_root,
        );
        self.record(
            "rx_missing_state_root",
            metrics.consensus_events.rx_missing_state_root,
        );
        self.record("rx_proposal", metrics.consensus_events.rx_proposal);
        self.record(
            "proposal_with_tc",
            metrics.consensus_events.proposal_with_tc,
        );
        self.record(
            "failed_verify_randao_reveal_sig",
            metrics.consensus_events.failed_verify_randao_reveal_sig,
        );
        self.record(
            "commit_empty_block",
            metrics.consensus_events.commit_empty_block,
        );
        self.record(
            "state_root_update",
            metrics.consensus_events.state_root_update,
        );
        self.record(
            "enter_new_round_qc",
            metrics.consensus_events.enter_new_round_qc,
        );
        self.record(
            "enter_new_round_tc",
            metrics.consensus_events.enter_new_round_tc,
        );
        self.record("prune_success", metrics.blocktree_events.prune_success);
        self.record("add_success", metrics.blocktree_events.add_success);
        self.record("add_dup", metrics.blocktree_events.add_dup);
        self.record(
            "blocksync_response_successful",
            metrics.blocksync_events.blocksync_response_successful,
        );
        self.record(
            "blocksync_response_failed",
            metrics.blocksync_events.blocksync_response_failed,
        );
        self.record(
            "blocksync_response_unexpected",
            metrics.blocksync_events.blocksync_response_unexpected,
        );
        self.record(
            "blocksync_request",
            metrics.blocksync_events.blocksync_request,
        );
    }
}

impl<E> Executor for OpenTelemetryExecutor<E> {
    type Command = MetricsCommand;

    fn replay(&mut self, mut _commands: Vec<Self::Command>) {}

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for cmd in commands {
            match cmd {
                MetricsCommand::RecordMetrics(metrics) => {
                    self.record_metrics(&metrics);

                    let mut rm = ResourceMetrics {
                        resource: Default::default(),
                        scope_metrics: vec![],
                    };
                    let fut = self.exporter.export(&mut rm);

                }
            }
        }
    }
}

impl<E> Stream for OpenTelemetryExecutor<E> {
    type Item = E;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
