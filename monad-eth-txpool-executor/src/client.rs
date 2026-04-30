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
use monad_types::{ForwardedTxList, NodeId};
use monad_validator::signature_collection::SignatureCollection;

use crate::{forward::INGRESS_CHUNK_MAX_SIZE, TxPoolExecutorCommand, TxPoolExecutorEvent};

pub struct ForwardedTxs<SCT>
where
    SCT: SignatureCollection,
{
    pub sender: NodeId<SCT::NodeIdPubKey>,
    pub txs: ForwardedTxList,
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
            per_id_limit: 4_000,
            max_size: 40_000,
            regular_per_id_limit: 4_000,
            regular_max_size: 40_000,
            regular_bandwidth_pct: 10,
        }
    }
}

const DEFAULT_COMMAND_BUFFER_SIZE: usize = 1024;
const DEFAULT_FORWARDED_BUFFER_SIZE: usize = 1;
const DEFAULT_EVENT_BUFFER_SIZE: usize = 1024;

monad_executor::metric_consts! {
    COUNTER_TXPOOL_FORWARDED_INGRESS_ENQUEUED_TXS {
        name: "monad.bft.txpool.forwarded_ingress_enqueued_txs",
        help: "Forwarded ingress transactions accepted into fair queue",
    }
    COUNTER_TXPOOL_FORWARDED_INGRESS_DROPPED_TXS {
        name: "monad.bft.txpool.forwarded_ingress_dropped_txs",
        help: "Forwarded ingress transactions dropped from fair queue",
    }
    COUNTER_TXPOOL_FORWARDED_INGRESS_DROP_EVENTS {
        name: "monad.bft.txpool.forwarded_ingress_drop_events",
        help: "Forwarded ingress drop events (per sender batch)",
    }
    COUNTER_TXPOOL_FORWARDED_INGRESS_SENT_BATCHES {
        name: "monad.bft.txpool.forwarded_ingress_sent_batches",
        help: "Batches sent from fair queue to txpool worker",
    }
    COUNTER_TXPOOL_FORWARDED_INGRESS_SENT_TXS {
        name: "monad.bft.txpool.forwarded_ingress_sent_txs",
        help: "Transactions sent from fair queue to txpool worker",
    }
}

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
}

impl<SCT> PendingForwardedSend<SCT>
where
    SCT: SignatureCollection,
{
    fn new(sender: tokio::sync::mpsc::Sender<Vec<ForwardedTxs<SCT>>>) -> Self {
        tracing::debug!("txpool forwarded_ingress: reserving channel slot for next batch");
        Self {
            permit_fut: Box::pin(sender.reserve_owned()),
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
            TxPoolExecutorCommand<
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
    forwarded_queue: FairQueue<ScoreReader<NodeId<SCT::NodeIdPubKey>, StdClock>, Bytes>,
    forwarded_pending_send: Option<PendingForwardedSend<SCT>>,
    score_provider: ScoreProvider<NodeId<SCT::NodeIdPubKey>, StdClock>,
    event_rx: tokio::sync::mpsc::Receiver<TxPoolExecutorEvent<ST, SCT, EthExecutionProtocol>>,
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
                        TxPoolExecutorCommand<
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
                tokio::sync::mpsc::Sender<TxPoolExecutorEvent<ST, SCT, EthExecutionProtocol>>,
            ) -> F
            + Send
            + 'static,
        update_metrics: Box<dyn Fn(&mut ExecutorMetrics) + Send + 'static>,
        score_provider: ScoreProvider<NodeId<SCT::NodeIdPubKey>, StdClock>,
        score_reader: ScoreReader<NodeId<SCT::NodeIdPubKey>, StdClock>,
        forwarded_queue_config: ForwardedIngressFairQueueConfig,
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
            forwarded_queue_config,
        )
    }

    pub fn new_with_buffer_sizes<F>(
        updater: impl FnOnce(
                tokio::sync::mpsc::Receiver<
                    Vec<
                        TxPoolExecutorCommand<
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
                tokio::sync::mpsc::Sender<TxPoolExecutorEvent<ST, SCT, EthExecutionProtocol>>,
            ) -> F
            + Send
            + 'static,
        update_metrics: Box<dyn Fn(&mut ExecutorMetrics) + Send + 'static>,
        score_provider: ScoreProvider<NodeId<SCT::NodeIdPubKey>, StdClock>,
        score_reader: ScoreReader<NodeId<SCT::NodeIdPubKey>, StdClock>,
        command_buffer_size: usize,
        forwarded_buffer_size: usize,
        event_buffer_size: usize,
        forwarded_queue_config: ForwardedIngressFairQueueConfig,
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
            forwarded_queue: FairQueueBuilder::new()
                .per_id_limit(forwarded_queue_config.per_id_limit)
                .max_size(forwarded_queue_config.max_size)
                .regular_per_id_limit(forwarded_queue_config.regular_per_id_limit)
                .regular_max_size(forwarded_queue_config.regular_max_size)
                .regular_bandwidth_pct(forwarded_queue_config.regular_bandwidth_pct)
                .build(score_reader),
            forwarded_pending_send: None,
            score_provider,
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
        let fq_len_before = self.forwarded_queue.len();
        let mut total_txs = 0usize;
        let mut total_dropped = 0u64;

        for ForwardedTxs { sender, txs } in forwarded {
            let txs_len = txs.len();
            total_txs += txs_len;
            for (index, tx) in txs.into_iter().enumerate() {
                match self.forwarded_queue.push(sender, tx) {
                    Ok(()) => {
                        self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_ENQUEUED_TXS] += 1;
                    }
                    Err(err) => {
                        let dropped = (txs_len - index) as u64;
                        total_dropped += dropped;
                        self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_DROPPED_TXS] += dropped;
                        self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_DROP_EVENTS] += 1;
                        tracing::debug!(
                            ?sender,
                            error = %err,
                            sender_batch_txs = txs_len,
                            accepted_txs = index,
                            dropped_txs = dropped,
                            "forwarded ingress queue full, dropping current and remaining txs for sender"
                        );
                        break;
                    }
                }
            }
        }

        tracing::debug!(
            fq_len_before,
            fq_len_after = self.forwarded_queue.len(),
            total_txs,
            total_enqueued = total_txs as u64 - total_dropped,
            total_dropped,
            "txpool forwarded_ingress: enqueued"
        );
    }

    fn pop_forwarded_batch(&mut self) -> Vec<ForwardedTxs<SCT>> {
        let fq_len_before = self.forwarded_queue.len();
        let mut batch = Vec::with_capacity(INGRESS_CHUNK_MAX_SIZE);
        while batch.len() < INGRESS_CHUNK_MAX_SIZE {
            let Some((sender, tx)) = self.forwarded_queue.pop() else {
                break;
            };
            batch.push(ForwardedTxs {
                sender,
                txs: vec![tx]
                    .try_into()
                    .expect("forwarded tx list limit must be greater than 0"),
            });
        }

        if !batch.is_empty() {
            tracing::debug!(
                fq_len_before,
                fq_len_after = self.forwarded_queue.len(),
                batch_items = batch.len(),
                "txpool forwarded_ingress: popped batch for channel send"
            );
        }
        batch
    }

    fn poll_pending_forwarded_send(&mut self, cx: &mut std::task::Context<'_>) -> bool {
        let poll_result = {
            let Some(pending) = self.forwarded_pending_send.as_mut() else {
                return false;
            };
            pending.permit_fut.as_mut().poll(cx)
        };

        match poll_result {
            Poll::Ready(Ok(permit)) => {
                let batch = self.pop_forwarded_batch();
                if batch.is_empty() {
                    tracing::debug!(
                        "txpool forwarded_ingress: channel slot acquired with empty queue"
                    );
                    self.forwarded_pending_send = None;
                    return false;
                }
                self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_SENT_BATCHES] += 1;
                self.metrics[COUNTER_TXPOOL_FORWARDED_INGRESS_SENT_TXS] += batch.len() as u64;
                tracing::debug!(
                    batch_items = batch.len(),
                    "txpool forwarded_ingress: channel slot acquired, sending batch"
                );
                permit.send(batch);
                self.forwarded_pending_send = None;
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
            if !self.forwarded_queue.is_empty() {
                cx.waker().wake_by_ref();
            }
            return;
        }

        if self.forwarded_pending_send.is_some() || self.forwarded_queue.is_empty() {
            return;
        }

        self.forwarded_pending_send = Some(PendingForwardedSend::new(self.forwarded_tx.clone()));

        if self.poll_pending_forwarded_send(cx) && !self.forwarded_queue.is_empty() {
            cx.waker().wake_by_ref();
        }
    }

    fn process_event(
        &mut self,
        event: TxPoolExecutorEvent<ST, SCT, EthExecutionProtocol>,
    ) -> Option<MempoolEvent<ST, SCT, EthExecutionProtocol>> {
        match event {
            TxPoolExecutorEvent::Proposal {
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
            } => Some(MempoolEvent::Proposal {
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
            }),
            TxPoolExecutorEvent::Contribution { sender_gas } => {
                for (sender, gas) in sender_gas {
                    self.score_provider.record_contribution(sender, gas);
                }
                None
            }
            TxPoolExecutorEvent::ForwardTxs(txs) => Some(MempoolEvent::ForwardTxs(txs)),
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

        let (commands, forwarded): (
            Vec<
                TxPoolExecutorCommand<
                    ST,
                    SCT,
                    EthExecutionProtocol,
                    EthBlockPolicy<ST, SCT, CCT, CRT>,
                    SBT,
                    CCT,
                    CRT,
                >,
            >,
            Vec<ForwardedTxs<SCT>>,
        ) = commands.into_iter().partition_map(|command| match command {
            TxPoolCommand::BlockCommit(block_commit) => {
                Either::Left(TxPoolExecutorCommand::BlockCommit(block_commit))
            }
            TxPoolCommand::CreateProposal {
                node_id,
                epoch,
                round,
                seq_num,
                high_qc,
                round_signature,
                last_round_tc,
                fresh_proposal_certificate,
                tx_limit,
                proposal_gas_limit,
                proposal_byte_limit,
                beneficiary,
                timestamp_ns,
                extending_blocks,
                delayed_execution_results,
            } => Either::Left(TxPoolExecutorCommand::CreateProposal {
                node_id,
                epoch,
                round,
                seq_num,
                high_qc,
                round_signature,
                last_round_tc,
                fresh_proposal_certificate,
                tx_limit,
                proposal_gas_limit,
                proposal_byte_limit,
                beneficiary,
                timestamp_ns,
                extending_blocks,
                delayed_execution_results,
            }),
            TxPoolCommand::InsertForwardedTxs { sender, txs } => {
                Either::Right(ForwardedTxs { sender, txs })
            }
            TxPoolCommand::EnterRound {
                epoch,
                round,
                upcoming_leader_rounds,
            } => Either::Left(TxPoolExecutorCommand::EnterRound {
                epoch,
                round,
                upcoming_leader_rounds,
            }),
            TxPoolCommand::Reset {
                last_delay_committed_blocks,
            } => Either::Left(TxPoolExecutorCommand::Reset {
                last_delay_committed_blocks,
            }),
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
            .push(self.forwarded_queue.metrics())
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

        loop {
            let Poll::Ready(result) = this.event_rx.poll_recv(cx) else {
                break;
            };

            let Some(event) = result else {
                return Poll::Ready(None);
            };

            if let Some(event) = this.process_event(event) {
                return Poll::Ready(Some(MonadEvent::MempoolEvent(event)));
            }
        }

        this.poll_forwarded_send(cx);

        Poll::Pending
    }
}
