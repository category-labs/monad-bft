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

use std::{future::Future, pin::Pin, task::Poll};

use bytes::Bytes;
use futures::Stream;
use itertools::{Either, Itertools};
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_fair_queue::{FairQueue, FairQueueBuilder};
use monad_peer_score::{
    ema::{ScoreProvider, ScoreReader},
    StdClock,
};
use monad_secp::ExtractEthAddress;
use monad_state_backend::StateBackend;
use monad_types::NodeId;
use monad_validator::signature_collection::SignatureCollection;

pub struct ForwardedTxs<SCT>
where
    SCT: SignatureCollection,
{
    pub sender: NodeId<SCT::NodeIdPubKey>,
    pub txs: Vec<Bytes>,
}

#[derive(Debug, Clone, Copy)]
pub struct ForwardedIngressFairQueueConfig {
    pub per_id_limit: usize,
    pub max_size: usize,
    pub regular_per_id_limit: usize,
    pub regular_max_size: usize,
    pub regular_bandwidth_pct: u8,
}

impl Default for ForwardedIngressFairQueueConfig {
    fn default() -> Self {
        Self {
            per_id_limit: 10_000,
            max_size: 100_000,
            regular_per_id_limit: 1_000,
            regular_max_size: 100_000,
            regular_bandwidth_pct: 10,
        }
    }
}

const DEFAULT_COMMAND_BUFFER_SIZE: usize = 1024;
const DEFAULT_FORWARDED_BUFFER_SIZE: usize = 1;
const DEFAULT_EVENT_BUFFER_SIZE: usize = 1024;
const INGRESS_CHUNK_MAX_SIZE: usize = 128;
const COUNTER_TXPOOL_FORWARDED_INGRESS_ENQUEUED_TXS: &str =
    "monad.bft.txpool.forwarded_ingress_enqueued_txs";
const COUNTER_TXPOOL_FORWARDED_INGRESS_DROPPED_TXS: &str =
    "monad.bft.txpool.forwarded_ingress_dropped_txs";
const COUNTER_TXPOOL_FORWARDED_INGRESS_DROP_EVENTS: &str =
    "monad.bft.txpool.forwarded_ingress_drop_events";
const COUNTER_TXPOOL_FORWARDED_INGRESS_SENT_BATCHES: &str =
    "monad.bft.txpool.forwarded_ingress_sent_batches";
const COUNTER_TXPOOL_FORWARDED_INGRESS_SENT_TXS: &str =
    "monad.bft.txpool.forwarded_ingress_sent_txs";

type ForwardedPermitFuture<SCT> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    tokio::sync::mpsc::OwnedPermit<Vec<ForwardedTxs<SCT>>>,
                    tokio::sync::mpsc::error::SendError<()>,
                >,
            > + 'static,
    >,
>;

struct PendingForwardedSend<SCT>
where
    SCT: SignatureCollection,
{
    permit_fut: ForwardedPermitFuture<SCT>,
    batch: Option<Vec<ForwardedTxs<SCT>>>,
}

impl<SCT> PendingForwardedSend<SCT>
where
    SCT: SignatureCollection,
{
    fn new(
        sender: tokio::sync::mpsc::Sender<Vec<ForwardedTxs<SCT>>>,
        batch: Vec<ForwardedTxs<SCT>>,
    ) -> Self {
        tracing::debug!(
            batch_items = batch.len(),
            "txpool forwarded_ingress: reserving channel slot for batch"
        );
        Self {
            permit_fut: Box::pin(sender.reserve_owned()),
            batch: Some(batch),
        }
    }
}

pub struct EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    handle: tokio::task::JoinHandle<()>,
    metrics: ExecutorMetrics,
    update_metrics: Box<dyn Fn(&mut ExecutorMetrics)>,

    command_tx: tokio::sync::mpsc::Sender<
        Vec<
            TxPoolCommand<
                ST,
                SCT,
                EthExecutionProtocol,
                EthBlockPolicy<ST, SCT, CCT, CRT>,
                SBT,
                CCT,
                CRT,
            >,
        >,
    >,
    forwarded_tx: tokio::sync::mpsc::Sender<Vec<ForwardedTxs<SCT>>>,
    score_provider: ScoreProvider<NodeId<CertificateSignaturePubKey<ST>>, StdClock>,
    forwarded_ingress:
        FairQueue<ScoreReader<NodeId<CertificateSignaturePubKey<ST>>, StdClock>, Bytes>,
    pending_forwarded_send: Option<PendingForwardedSend<SCT>>,
    event_rx: tokio::sync::mpsc::Receiver<MonadEvent<ST, SCT, EthExecutionProtocol>>,
}

impl<ST, SCT, SBT, CCT, CRT> EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn new<F>(
        updater: impl FnOnce(
                tokio::sync::mpsc::Receiver<
                    Vec<
                        TxPoolCommand<
                            ST,
                            SCT,
                            EthExecutionProtocol,
                            EthBlockPolicy<ST, SCT, CCT, CRT>,
                            SBT,
                            CCT,
                            CRT,
                        >,
                    >,
                >,
                tokio::sync::mpsc::Receiver<Vec<ForwardedTxs<SCT>>>,
                tokio::sync::mpsc::Sender<MonadEvent<ST, SCT, EthExecutionProtocol>>,
            ) -> F
            + Send
            + 'static,
        update_metrics: Box<dyn Fn(&mut ExecutorMetrics) + Send + 'static>,
        score_provider: ScoreProvider<NodeId<CertificateSignaturePubKey<ST>>, StdClock>,
        score_reader: ScoreReader<NodeId<CertificateSignaturePubKey<ST>>, StdClock>,
        forwarded_ingress_fair_queue_config: ForwardedIngressFairQueueConfig,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self::new_with_buffer_sizes(
            updater,
            update_metrics,
            score_provider,
            score_reader,
            DEFAULT_COMMAND_BUFFER_SIZE,
            DEFAULT_FORWARDED_BUFFER_SIZE,
            DEFAULT_EVENT_BUFFER_SIZE,
            forwarded_ingress_fair_queue_config,
        )
    }

    pub fn new_with_buffer_sizes<F>(
        updater: impl FnOnce(
                tokio::sync::mpsc::Receiver<
                    Vec<
                        TxPoolCommand<
                            ST,
                            SCT,
                            EthExecutionProtocol,
                            EthBlockPolicy<ST, SCT, CCT, CRT>,
                            SBT,
                            CCT,
                            CRT,
                        >,
                    >,
                >,
                tokio::sync::mpsc::Receiver<Vec<ForwardedTxs<SCT>>>,
                tokio::sync::mpsc::Sender<MonadEvent<ST, SCT, EthExecutionProtocol>>,
            ) -> F
            + Send
            + 'static,
        update_metrics: Box<dyn Fn(&mut ExecutorMetrics) + Send + 'static>,
        score_provider: ScoreProvider<NodeId<CertificateSignaturePubKey<ST>>, StdClock>,
        score_reader: ScoreReader<NodeId<CertificateSignaturePubKey<ST>>, StdClock>,
        command_buffer_size: usize,
        forwarded_buffer_size: usize,
        event_buffer_size: usize,
        forwarded_ingress_fair_queue_config: ForwardedIngressFairQueueConfig,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(command_buffer_size);
        let (forwarded_tx, forwarded_rx) = tokio::sync::mpsc::channel(forwarded_buffer_size);
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(event_buffer_size);

        let handle = tokio::spawn(updater(command_rx, forwarded_rx, event_tx));

        Self {
            handle,
            metrics: ExecutorMetrics::default(),
            update_metrics,

            command_tx,
            forwarded_tx,
            score_provider,
            forwarded_ingress: FairQueueBuilder::new()
                .per_id_limit(forwarded_ingress_fair_queue_config.per_id_limit)
                .max_size(forwarded_ingress_fair_queue_config.max_size)
                .regular_per_id_limit(forwarded_ingress_fair_queue_config.regular_per_id_limit)
                .regular_max_size(forwarded_ingress_fair_queue_config.regular_max_size)
                .regular_bandwidth_pct(forwarded_ingress_fair_queue_config.regular_bandwidth_pct)
                .build(score_reader),
            pending_forwarded_send: None,
            event_rx,
        }
    }

    fn verify_handle_liveness(&self) {
        if self.handle.is_finished() {
            panic!("EthTxPoolExecutorClient handle terminated!");
        }

        if self.command_tx.is_closed() {
            panic!("EthTxPoolExecutorClient command_rx dropped!");
        }

        if self.forwarded_tx.is_closed() {
            panic!("EthTxPoolExecutorClient forwarded_rx dropped!");
        }

        if self.event_rx.is_closed() {
            panic!("EthTxPoolExecutorClient event_tx dropped!");
        }
    }

    fn enqueue_forwarded(&mut self, forwarded: Vec<ForwardedTxs<SCT>>) {
        let fq_len_before = self.forwarded_ingress.len();
        let mut total_txs = 0usize;
        let mut total_dropped = 0u64;

        for ForwardedTxs { sender, txs } in forwarded {
            let txs_len = txs.len();
            total_txs += txs_len;
            for (index, tx) in txs.into_iter().enumerate() {
                if let Err(err) = self.forwarded_ingress.push(sender, tx) {
                    let dropped = (txs_len - index) as u64;
                    total_dropped += dropped;
                    self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_DROPPED_TXS] += dropped;
                    self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_DROP_EVENTS] += 1;
                    tracing::debug!(
                        ?sender,
                        error = %err,
                        dropped_txs = dropped,
                        "forwarded ingress queue full, dropping remaining txs for sender"
                    );
                    break;
                }

                self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_ENQUEUED_TXS] += 1;
            }
        }

        tracing::debug!(
            fq_len_before,
            fq_len_after = self.forwarded_ingress.len(),
            total_txs,
            total_dropped,
            "txpool forwarded_ingress: enqueued"
        );
    }

    fn pop_forwarded_batch(&mut self) -> Vec<ForwardedTxs<SCT>> {
        let fq_len_before = self.forwarded_ingress.len();
        let mut batch = Vec::with_capacity(INGRESS_CHUNK_MAX_SIZE);
        while batch.len() < INGRESS_CHUNK_MAX_SIZE {
            let Some((sender, tx)) = self.forwarded_ingress.pop() else {
                break;
            };
            batch.push(ForwardedTxs {
                sender,
                txs: vec![tx],
            });
        }

        if !batch.is_empty() {
            tracing::debug!(
                fq_len_before,
                fq_len_after = self.forwarded_ingress.len(),
                batch_items = batch.len(),
                "txpool forwarded_ingress: popped batch for channel send"
            );
        }
        batch
    }

    fn poll_pending_forwarded_send(&mut self, cx: &mut std::task::Context<'_>) -> bool {
        let Some(pending) = self.pending_forwarded_send.as_mut() else {
            return false;
        };

        match pending.permit_fut.as_mut().poll(cx) {
            Poll::Ready(Ok(permit)) => {
                let batch = pending
                    .batch
                    .take()
                    .expect("forwarded batch must be present");
                self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_SENT_BATCHES] += 1;
                self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_SENT_TXS] += batch.len() as u64;
                tracing::debug!(
                    batch_items = batch.len(),
                    "txpool forwarded_ingress: channel slot acquired, sending batch"
                );
                permit.send(batch);
                self.pending_forwarded_send = None;
                true
            }
            Poll::Ready(Err(_)) => {
                panic!("EthTxPoolExecutorClient forwarded_rx dropped!");
            }
            Poll::Pending => false,
        }
    }

    fn poll_forwarded_send(&mut self, cx: &mut std::task::Context<'_>) {
        if self.poll_pending_forwarded_send(cx) {
            if !self.forwarded_ingress.is_empty() {
                cx.waker().wake_by_ref();
            }
            return;
        }

        if self.pending_forwarded_send.is_some() || self.forwarded_ingress.is_empty() {
            return;
        }

        let batch = self.pop_forwarded_batch();
        if batch.is_empty() {
            return;
        }
        self.pending_forwarded_send =
            Some(PendingForwardedSend::new(self.forwarded_tx.clone(), batch));

        if self.poll_pending_forwarded_send(cx) && !self.forwarded_ingress.is_empty() {
            cx.waker().wake_by_ref();
        }
    }
}

impl<ST, SCT, SBT, CCT, CRT> Executor for EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    type Command = TxPoolCommand<
        ST,
        SCT,
        EthExecutionProtocol,
        EthBlockPolicy<ST, SCT, CCT, CRT>,
        SBT,
        CCT,
        CRT,
    >;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.verify_handle_liveness();

        let (commands, forwarded): (Vec<Self::Command>, Vec<ForwardedTxs<SCT>>) =
            commands.into_iter().partition_map(|command| match command {
                TxPoolCommand::InsertForwardedTxs { sender, txs } => {
                    Either::Right(ForwardedTxs { sender, txs })
                }
                command => Either::Left(command),
            });

        if !commands.is_empty() {
            self.command_tx
                .try_send(commands)
                .expect("EthTxPoolExecutorClient executor is lagging")
        }

        if !forwarded.is_empty() {
            self.enqueue_forwarded(forwarded);
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::from(&self.metrics)
            .push(self.forwarded_ingress.executor_metrics())
            .push(self.score_provider.executor_metrics())
    }
}

impl<ST, SCT, SBT, CCT, CRT> Stream for EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        this.verify_handle_liveness();

        (this.update_metrics)(&mut this.metrics);
        this.poll_forwarded_send(cx);

        match this.event_rx.poll_recv(cx) {
            Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::ProposalWithScores {
                epoch,
                round,
                seq_num,
                high_qc,
                timestamp_ns,
                round_signature,
                base_fee,
                base_fee_trend,
                base_fee_moment,
                delayed_execution_results,
                proposed_execution_inputs,
                last_round_tc,
                fresh_proposal_certificate,
                forwarded_senders_with_gas,
            }))) => {
                for forwarded_sender in forwarded_senders_with_gas {
                    this.score_provider
                        .record_contribution(forwarded_sender.sender, forwarded_sender.gas);
                }

                Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::Proposal {
                    epoch,
                    round,
                    seq_num,
                    high_qc,
                    timestamp_ns,
                    round_signature,
                    base_fee,
                    base_fee_trend,
                    base_fee_moment,
                    delayed_execution_results,
                    proposed_execution_inputs,
                    last_round_tc,
                    fresh_proposal_certificate,
                })))
            }
            other => other,
        }
    }
}
