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
    collections::{BTreeMap, HashSet},
    io,
    marker::PhantomData,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::Poll,
    time::Duration,
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::Address;
use alloy_rlp::Decodable;
use futures::Stream;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::block::BlockPolicy;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::{
    EthTxPool, EthTxPoolConfig, EthTxPoolEventTracker, PoolTxKind, TrackedTxLimitsConfig,
};
use monad_eth_txpool_types::{
    EthTxPoolDropReason, EthTxPoolEventType, EthTxPoolIpcTx, EthTxPoolTxInputStream,
};
use monad_eth_types::{EthExecutionProtocol, ExtractEthAddress};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_peer_score::{ema, StdClock};
use monad_secp::RecoverableAddress;
use monad_state_backend::StateBackend;
use monad_types::{DropTimer, NodeId, Round};
use monad_validator::signature_collection::SignatureCollection;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::{sync::mpsc, time::Instant};
use tracing::{debug, debug_span, error, info, trace_span, warn};

use self::{
    client::ForwardedTxs, forward::EthTxPoolForwardingManager, ipc::EthTxPoolIpcServer,
    metrics::EthTxPoolExecutorMetrics, preload::EthTxPoolPreloadManager,
    reset::EthTxPoolResetTrigger,
};
pub use self::{
    client::{EthTxPoolExecutorClient, ForwardedIngressFairQueueConfig},
    ipc::EthTxPoolIpcConfig,
};
use crate::forward::INGRESS_CHUNK_INTERVAL_MS;

mod client;
pub mod forward;
mod ipc;
mod metrics;
mod preload;
mod reset;

pub struct EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT, TIS>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
    TIS: EthTxPoolTxInputStream,
{
    pool: EthTxPool<ST, SCT, SBT, CCT, CRT>,
    tx_input_stream: Pin<Box<TIS>>,

    reset: EthTxPoolResetTrigger,
    block_policy: EthBlockPolicy<ST, SCT, CCT, CRT>,
    state_backend: SBT,
    chain_config: CCT,

    events_tx: mpsc::UnboundedSender<MempoolEvent<ST, SCT, EthExecutionProtocol>>,
    events: mpsc::UnboundedReceiver<MempoolEvent<ST, SCT, EthExecutionProtocol>>,

    forwarding_manager: Pin<Box<EthTxPoolForwardingManager>>,
    preload_manager: Pin<Box<EthTxPoolPreloadManager>>,

    metrics: Arc<EthTxPoolExecutorMetrics>,
    executor_metrics: ExecutorMetrics,

    _phantom: PhantomData<CRT>,
}

impl<ST, SCT, SBT, CCT, CRT> EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT, EthTxPoolIpcServer>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT> + Send + 'static,
    CCT: ChainConfig<CRT> + Send + 'static,
    CRT: ChainRevision + Send + 'static,
    Self: Unpin,
{
    pub fn start(
        block_policy: EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: SBT,
        ipc_config: EthTxPoolIpcConfig,
        soft_tx_expiry: Duration,
        hard_tx_expiry: Duration,
        chain_config: CCT,
        round: Round,
        execution_timestamp_s: u64,
    ) -> io::Result<EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>> {
        let ipc = Box::pin(EthTxPoolIpcServer::new(ipc_config)?);

        let (events_tx, events) = mpsc::unbounded_channel();

        let metrics = Arc::new(EthTxPoolExecutorMetrics::default());
        let mut executor_metrics = ExecutorMetrics::default();
        let (_score_provider, score_reader) = ema::create::<
            NodeId<CertificateSignaturePubKey<ST>>,
            StdClock,
        >(ema::ScoreConfig::default(), StdClock);

        metrics.update(&mut executor_metrics);

        Ok(EthTxPoolExecutorClient::new(
            {
                let metrics = metrics.clone();

                move |command_rx, forwarded_rx, event_tx| {
                    let pool = EthTxPool::new(
                        EthTxPoolConfig {
                            limits: TrackedTxLimitsConfig::new(
                                None,
                                None,
                                None,
                                None,
                                soft_tx_expiry,
                                hard_tx_expiry,
                            ),
                        },
                        chain_config.chain_id(),
                        chain_config.get_chain_revision(round),
                        chain_config.get_execution_chain_revision(execution_timestamp_s),
                    );

                    Self {
                        pool,
                        tx_input_stream: ipc,
                        block_policy,
                        reset: EthTxPoolResetTrigger::default(),
                        state_backend,
                        chain_config,

                        events_tx,
                        events,

                        forwarding_manager: Box::pin(EthTxPoolForwardingManager::default()),
                        preload_manager: Box::pin(EthTxPoolPreloadManager::default()),

                        metrics,
                        executor_metrics,

                        _phantom: PhantomData,
                    }
                    .run(command_rx, forwarded_rx, event_tx)
                }
            },
            Box::new(move |executor_metrics: &mut ExecutorMetrics| {
                metrics.update(executor_metrics)
            }),
            score_reader,
            ForwardedIngressFairQueueConfig::default(),
        ))
    }
}

impl<ST, SCT, SBT, CCT, CRT, TIS> EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT, TIS>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
    TIS: EthTxPoolTxInputStream,
    Self: Unpin,
{
    async fn run(
        mut self,
        mut command_rx: mpsc::Receiver<
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
        mut forwarded_rx: mpsc::Receiver<Vec<ForwardedTxs<SCT>>>,
        event_tx: mpsc::Sender<MonadEvent<ST, SCT, EthExecutionProtocol>>,
    ) {
        use futures::StreamExt;

        let mut forwarded_channel_poll = tokio::time::interval_at(
            Instant::now() + Duration::from_millis(INGRESS_CHUNK_INTERVAL_MS),
            Duration::from_millis(INGRESS_CHUNK_INTERVAL_MS),
        );
        forwarded_channel_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                // biased is kept so that if timer and command fire at the same time we prioritize command
                // specifically relevant for proposal creation, so that it is not delayed by 1 additional chunk
                biased;

                result = command_rx.recv() => {
                    let Some(commands) = result else {
                        warn!("command channel was dropped, shutting down txpool executor");
                        break;
                    };

                    self.exec(commands);
                }
                // we drain ingestion queue at a steady rate of 128 tx per 8ms, ~16k tx/s
                // if actual processing rate is lower we will shed load in the client and here we will rely on Delay policy
                _ = forwarded_channel_poll.tick() => {
                    if !self.forwarding_manager.as_ref().get_ref().ingress_is_empty() {
                        continue;
                    }

                    match forwarded_rx.try_recv() {
                        Ok(forwarded_txs) => {
                            debug!(
                                batch_items = forwarded_txs.len(),
                                "txpool executor: received forwarded batch from channel on timeout"
                            );
                            self.process_forwarded_txs(forwarded_txs);
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                            warn!("forwarded channel was dropped, shutting down txpool executor");
                            break;
                        }
                    }
                }

                result = self.next() => {
                    let Some(event) = result else {
                        error!("txpool executor stream terminated, shutting down txpool executor");
                        continue;
                    };

                    if let Err(err) = event_tx.send(event).await {
                        warn!(?err, "failed to send event to BFT, shutting down txpool executor");
                        break;
                    }
                }
            }
        }
    }

    fn process_forwarded_txs(&mut self, forwarded_txs: Vec<ForwardedTxs<SCT>>) {
        for ForwardedTxs { sender, txs } in forwarded_txs {
            let _span = debug_span!("processing forwarded txs").entered();
            debug!(
                ?sender,
                num_txs = txs.len(),
                "txpool executor received forwarded txs"
            );

            let mut num_invalid_bytes = 0;

            let txs = txs
                .into_iter()
                .filter_map(|raw_tx| {
                    if let Ok(tx) = TxEnvelope::decode(&mut raw_tx.as_ref()) {
                        Some(tx)
                    } else {
                        num_invalid_bytes += 1;
                        None
                    }
                })
                .collect::<Vec<_>>();

            self.metrics
                .reject_forwarded_invalid_bytes
                .fetch_add(num_invalid_bytes, Ordering::SeqCst);

            if num_invalid_bytes != 0 {
                tracing::warn!(?sender, ?num_invalid_bytes, "invalid forwarded txs");
            }

            self.forwarding_manager
                .as_mut()
                .project()
                .add_ingress_txs(txs);
        }
    }
}

impl<ST, SCT, SBT, CCT, CRT, TIS> Executor for EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT, TIS>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
    TIS: EthTxPoolTxInputStream,
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
        let _span = debug_span!("txpool exec").entered();

        let mut ipc_events = BTreeMap::default();
        let mut event_tracker = EthTxPoolEventTracker::new(&self.metrics.pool, &mut ipc_events);

        for command in commands {
            match command {
                TxPoolCommand::BlockCommit(committed_blocks) => {
                    let _span = debug_span!("block commit").entered();
                    for committed_block in committed_blocks {
                        BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT, CCT, CRT>::update_committed_block(
                            &mut self.block_policy,
                            &committed_block,
                        );

                        self.preload_manager
                            .update_committed_block(&committed_block);

                        self.pool.update_committed_block(
                            &mut event_tracker,
                            &self.chain_config,
                            committed_block,
                        );
                    }

                    self.forwarding_manager
                        .as_mut()
                        .project()
                        .schedule_egress_txs(&mut self.pool);
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
                } => {
                    let _span =
                        debug_span!("create proposal", seq_num = seq_num.as_u64(),).entered();
                    self.preload_manager.update_on_create_proposal(seq_num);

                    let create_proposal_start = Instant::now();

                    let (base_fee, base_fee_trend, base_fee_moment) = self
                        .block_policy
                        .compute_base_fee(&extending_blocks, &self.chain_config);

                    match self.pool.create_proposal(
                        &mut event_tracker,
                        epoch,
                        round,
                        seq_num,
                        base_fee,
                        tx_limit,
                        proposal_gas_limit,
                        proposal_byte_limit,
                        beneficiary,
                        timestamp_ns,
                        node_id,
                        round_signature.clone(),
                        extending_blocks,
                        &self.block_policy,
                        &self.state_backend,
                        &self.chain_config,
                    ) {
                        Ok(proposed_execution_inputs) => {
                            let elapsed = create_proposal_start.elapsed();

                            self.metrics.create_proposal.fetch_add(1, Ordering::SeqCst);
                            self.metrics
                                .create_proposal_elapsed_ns
                                .fetch_add(elapsed.as_nanos() as u64, Ordering::SeqCst);

                            self.events_tx
                                .send(MempoolEvent::Proposal {
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
                                })
                                .expect("events never dropped");
                        }
                        Err(err) => {
                            error!(?err, "txpool executor failed to create proposal");
                        }
                    }
                }
                TxPoolCommand::InsertForwardedTxs { sender, txs } => {
                    // This will never happen because we separate out these commands in `EthTxPoolExecutorClient`.
                    error!("txpool executor received InsertForwardedTxs command over command rx");
                }
                TxPoolCommand::EnterRound {
                    epoch: _,
                    round,
                    upcoming_leader_rounds,
                } => {
                    self.pool
                        .enter_round(&mut event_tracker, &self.chain_config, round);
                    debug!(
                        ?round,
                        "txpool executor entered round, submitting preload requests"
                    );

                    self.preload_manager.enter_round(
                        round,
                        self.block_policy.get_last_commit(),
                        upcoming_leader_rounds,
                        || self.pool.generate_sender_snapshot(),
                    );
                }
                TxPoolCommand::Reset {
                    last_delay_committed_blocks,
                } => {
                    BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT, CCT, CRT>::reset(
                        &mut self.block_policy,
                        last_delay_committed_blocks.iter().collect(),
                    );

                    self.pool.reset(
                        &mut event_tracker,
                        &self.chain_config,
                        last_delay_committed_blocks,
                    );

                    self.reset.set_reset();
                }
            }
        }

        self.metrics.update(&mut self.executor_metrics);

        self.tx_input_stream
            .as_mut()
            .broadcast_tx_events(ipc_events);
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default().push(&self.executor_metrics)
    }
}

impl<ST, SCT, SBT, CCT, CRT, TIS> Stream for EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT, TIS>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
    TIS: EthTxPoolTxInputStream,

    Self: Unpin,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let _span = debug_span!("txpool poll").entered();
        let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
            info!(?elapsed, "txpool executor long poll");
        });

        let Self {
            pool,
            tx_input_stream,

            reset,
            block_policy,
            state_backend,
            chain_config,

            events_tx: _,
            events,

            forwarding_manager,
            preload_manager,

            metrics,
            executor_metrics,

            _phantom,
        } = self.get_mut();

        if let Poll::Ready(result) = events.poll_recv(cx) {
            let event = result.expect("events_tx never dropped");

            return Poll::Ready(Some(MonadEvent::MempoolEvent(event)));
        };

        if !reset.poll_is_ready(cx) {
            return Poll::Pending;
        }

        if let Poll::Ready(unvalidated_txs) = tx_input_stream
            .as_mut()
            .poll_txs(cx, || pool.generate_snapshot())
        {
            let _span = debug_span!("ipc txs", len = unvalidated_txs.len()).entered();

            let mut ipc_events = BTreeMap::default();

            let recovered_txs = {
                let (recovered_txs, dropped_txs): (Vec<_>, BTreeMap<_, _>) =
                    unvalidated_txs.into_par_iter().partition_map(
                        |EthTxPoolIpcTx {
                             tx,
                             priority,
                             extra_data,
                         }| {
                            let _span = trace_span!("txpool: ipc tx recover signer").entered();
                            match tx.secp256k1_recover() {
                                Ok(signer) => rayon::iter::Either::Left((
                                    Recovered::new_unchecked(tx, signer),
                                    PoolTxKind::Owned {
                                        priority,
                                        extra_data,
                                    },
                                )),
                                Err(_) => rayon::iter::Either::Right((
                                    *tx.tx_hash(),
                                    EthTxPoolEventType::Drop {
                                        reason: EthTxPoolDropReason::InvalidSignature,
                                    },
                                )),
                            }
                        },
                    );
                ipc_events.extend(dropped_txs);
                recovered_txs
            };

            let mut inserted_addresses = HashSet::<Address>::default();
            let mut immediately_forwardable_txs = Vec::default();

            pool.insert_txs(
                &mut EthTxPoolEventTracker::new(&metrics.pool, &mut ipc_events),
                block_policy,
                state_backend,
                chain_config,
                recovered_txs,
                |tx| {
                    inserted_addresses.insert(tx.signer());

                    if tx.is_owned_and_forwardable() {
                        immediately_forwardable_txs.push(tx.raw().clone_inner());
                    }
                },
            );

            preload_manager.add_requests(inserted_addresses.iter());

            forwarding_manager
                .as_mut()
                .project()
                .add_egress_txs(immediately_forwardable_txs.iter());

            metrics.update(executor_metrics);
            tx_input_stream.as_mut().broadcast_tx_events(ipc_events);

            cx.waker().wake_by_ref();
        }

        if let Poll::Ready(forward_txs) = forwarding_manager
            .as_mut()
            .poll_egress(pool.current_revision().1.execution_chain_params(), cx)
        {
            return Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::ForwardTxs(
                forward_txs,
            ))));
        }

        let mut ipc_events = BTreeMap::default();

        while let Poll::Ready(forwarded_txs) = forwarding_manager.as_mut().poll_ingress(cx) {
            let _span = debug_span!("forwarded txs", len = forwarded_txs.len()).entered();

            let recovered_txs = {
                let (recovered_txs, dropped_txs): (Vec<_>, Vec<_>) =
                    forwarded_txs.into_par_iter().partition_map(|tx| {
                        let _span = trace_span!("txpool: forwarded tx recover signer").entered();
                        match tx.secp256k1_recover() {
                            Ok(signer) => rayon::iter::Either::Left((
                                Recovered::new_unchecked(tx, signer),
                                PoolTxKind::Forwarded,
                            )),
                            Err(_) => rayon::iter::Either::Right((
                                *tx.tx_hash(),
                                EthTxPoolEventType::Drop {
                                    reason: EthTxPoolDropReason::InvalidSignature,
                                },
                            )),
                        }
                    });
                ipc_events.extend(dropped_txs);
                recovered_txs
            };

            let mut inserted_addresses = HashSet::<Address>::default();

            pool.insert_txs(
                &mut EthTxPoolEventTracker::new(&metrics.pool, &mut ipc_events),
                block_policy,
                state_backend,
                chain_config,
                recovered_txs,
                |tx| {
                    inserted_addresses.insert(tx.signer());
                },
            );

            preload_manager.add_requests(inserted_addresses.iter());
        }

        while let Poll::Ready((predicted_proposal_seqnum, addresses)) =
            preload_manager.as_mut().poll_requests(cx)
        {
            debug!(
                ?predicted_proposal_seqnum,
                "txpool executor preloading account balances"
            );

            let total_db_lookups_before = state_backend.total_db_lookups();

            if let Err(state_backend_error) = block_policy.compute_account_base_balances(
                predicted_proposal_seqnum,
                state_backend,
                chain_config,
                None,
                addresses.iter(),
            ) {
                warn!(
                    ?state_backend_error,
                    "txpool executor failed to preload account balances"
                )
            }

            metrics.preload_backend_lookups.fetch_add(
                state_backend.total_db_lookups() - total_db_lookups_before,
                Ordering::SeqCst,
            );
            metrics
                .preload_backend_requests
                .fetch_add(addresses.len() as u64, Ordering::SeqCst);

            preload_manager
                .complete_polled_requests(predicted_proposal_seqnum, addresses.into_iter());
        }

        metrics.update(executor_metrics);
        tx_input_stream.as_mut().broadcast_tx_events(ipc_events);

        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::BTreeMap,
        marker::PhantomData,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    };

    use alloy_consensus::transaction::SignerRecoverable;
    use alloy_primitives::TxHash;
    use futures::StreamExt;
    use monad_chain_config::{revision::MockChainRevision, ChainConfig, MockChainConfig};
    use monad_consensus_types::{
        block::GENESIS_TIMESTAMP, payload::RoundSignature, quorum_certificate::QuorumCertificate,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopKeyPair, NopSignature,
    };
    use monad_eth_block_policy::EthBlockPolicy;
    use monad_eth_testutil::{generate_block_with_txs, make_legacy_tx, secret_to_eth_address, S1};
    use monad_eth_txpool::{EthTxPool, EthTxPoolConfig, TrackedTxLimitsConfig};
    use monad_eth_txpool_types::{
        EthTxPoolEventType, EthTxPoolIpcTx, EthTxPoolSnapshot, EthTxPoolTxInputStream,
    };
    use monad_eth_types::EthExecutionProtocol;
    use monad_executor::{Executor, ExecutorMetrics};
    use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
    use monad_peer_score::{ema, StdClock};
    use monad_state_backend::{
        AccountState, InMemoryBlockState, InMemoryState, InMemoryStateInner,
    };
    use monad_testutil::signing::{node_id, MockSignatures};
    use monad_tfm::base_fee::MIN_BASE_FEE;
    use monad_types::{Epoch, NodeId, Round, SeqNum, GENESIS_ROUND, GENESIS_SEQ_NUM};
    use tokio::sync::mpsc;

    use crate::{
        forward::{EthTxPoolForwardingManager, INGRESS_CHUNK_INTERVAL_MS, INGRESS_CHUNK_MAX_SIZE},
        metrics::EthTxPoolExecutorMetrics,
        preload::EthTxPoolPreloadManager,
        reset::EthTxPoolResetTrigger,
        EthTxPoolExecutor, EthTxPoolExecutorClient, ForwardedIngressFairQueueConfig,
    };

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;
    type StateBackendType = InMemoryState<SignatureType, SignatureCollectionType>;
    type ExecutorType = EthTxPoolExecutor<
        SignatureType,
        SignatureCollectionType,
        StateBackendType,
        MockChainConfig,
        MockChainRevision,
        NoopTxInputStream,
    >;
    type CommandType = TxPoolCommand<
        SignatureType,
        SignatureCollectionType,
        EthExecutionProtocol,
        EthBlockPolicy<SignatureType, SignatureCollectionType, MockChainConfig, MockChainRevision>,
        StateBackendType,
        MockChainConfig,
        MockChainRevision,
    >;

    #[derive(Default)]
    struct NoopTxInputStream;

    impl EthTxPoolTxInputStream for NoopTxInputStream {
        fn poll_txs(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _generate_snapshot: impl Fn() -> EthTxPoolSnapshot,
        ) -> Poll<Vec<EthTxPoolIpcTx>> {
            Poll::Pending
        }

        fn broadcast_tx_events(
            self: Pin<&mut Self>,
            _events: BTreeMap<TxHash, EthTxPoolEventType>,
        ) {
        }
    }

    fn make_forwarded_txs(start_nonce: u64, count: usize) -> Vec<bytes::Bytes> {
        (start_nonce..(start_nonce + count as u64))
            .map(|nonce| {
                alloy_rlp::encode(make_legacy_tx(S1, MIN_BASE_FEE.into(), 100_000, nonce, 512))
                    .into()
            })
            .collect()
    }

    fn proposal_command(round: Round) -> CommandType {
        let mut key_bytes = [7_u8; 32];
        let keypair = NopKeyPair::from_bytes(&mut key_bytes).unwrap();

        TxPoolCommand::CreateProposal {
            node_id: node_id::<SignatureType>(),
            epoch: Epoch(1),
            round,
            seq_num: GENESIS_SEQ_NUM + SeqNum(1),
            high_qc: QuorumCertificate::genesis_qc(),
            round_signature: RoundSignature::new(round, &keypair),
            last_round_tc: None,
            fresh_proposal_certificate: None,
            tx_limit: 1_024,
            proposal_gas_limit: 30_000_000_u64 * 1_024,
            proposal_byte_limit: 10_000_000_u64,
            beneficiary: [0_u8; 20],
            timestamp_ns: GENESIS_TIMESTAMP + 1,
            extending_blocks: Vec::default(),
            delayed_execution_results: Vec::default(),
        }
    }

    fn collect_forwarded_txs(
        event: MonadEvent<SignatureType, SignatureCollectionType, EthExecutionProtocol>,
    ) -> Vec<bytes::Bytes> {
        let expected_signer = secret_to_eth_address(S1);

        match event {
            MonadEvent::MempoolEvent(MempoolEvent::Proposal {
                proposed_execution_inputs,
                ..
            }) => proposed_execution_inputs
                .body
                .transactions
                .into_iter()
                .filter_map(|tx| {
                    let signer = tx.recover_signer().expect("proposal tx signer recovers");
                    (signer == expected_signer).then(|| alloy_rlp::encode(tx).into())
                })
                .collect(),
            other => panic!("unexpected txpool event: {other:?}"),
        }
    }

    fn assert_no_proposal(
        proposal_rx: &mut mpsc::UnboundedReceiver<Vec<bytes::Bytes>>,
        context: &str,
    ) {
        assert!(
            matches!(
                proposal_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "{context}"
        );
    }

    const MAX_YIELDS: usize = 10;

    async fn recv_proposal_with_yields(
        proposal_rx: &mut mpsc::UnboundedReceiver<Vec<bytes::Bytes>>,
        yields: usize,
        context: &str,
    ) -> Vec<bytes::Bytes> {
        assert!(yields <= MAX_YIELDS, "yield limit exceeded");

        for _ in 0..yields {
            match proposal_rx.try_recv() {
                Ok(proposal) => return proposal,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    tokio::task::yield_now().await;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    panic!("proposal channel disconnected");
                }
            }
        }

        match proposal_rx.try_recv() {
            Ok(proposal) => proposal,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => panic!("{context}"),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                panic!("proposal channel disconnected");
            }
        }
    }

    fn metric_value(metrics: &[(&'static str, u64, &'static str)], name: &'static str) -> u64 {
        metrics
            .iter()
            .find_map(|(metric_name, value, _)| (*metric_name == name).then_some(*value))
            .unwrap_or_default()
    }

    fn start_test_client_with_forwarded_ingress_config(
        forwarded_ingress_fair_queue_config: ForwardedIngressFairQueueConfig,
    ) -> EthTxPoolExecutorClient<
        SignatureType,
        SignatureCollectionType,
        StateBackendType,
        MockChainConfig,
        MockChainRevision,
    > {
        const TEST_TX_EXPIRY: Duration = Duration::from_secs(24 * 60 * 60);

        let block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, u64::MAX);
        let state_backend: StateBackendType = InMemoryStateInner::new(
            SeqNum::MAX,
            InMemoryBlockState::genesis(BTreeMap::from_iter([(
                secret_to_eth_address(S1),
                AccountState::max_balance(),
            )])),
        );
        let chain_config = MockChainConfig::DEFAULT;
        let round = GENESIS_ROUND;
        let execution_timestamp_s = GENESIS_TIMESTAMP as u64;

        let (events_tx, events) = mpsc::unbounded_channel();

        let (_score_provider, score_reader) = ema::create::<
            NodeId<CertificateSignaturePubKey<SignatureType>>,
            StdClock,
        >(ema::ScoreConfig::default(), StdClock);

        EthTxPoolExecutorClient::new(
            move |command_rx, forwarded_rx, event_tx| {
                let pool = EthTxPool::new(
                    EthTxPoolConfig {
                        limits: TrackedTxLimitsConfig::new(
                            None,
                            None,
                            None,
                            None,
                            TEST_TX_EXPIRY,
                            TEST_TX_EXPIRY,
                        ),
                    },
                    chain_config.chain_id(),
                    chain_config.get_chain_revision(round),
                    chain_config.get_execution_chain_revision(execution_timestamp_s),
                );

                ExecutorType {
                    pool,
                    tx_input_stream: Box::pin(NoopTxInputStream),
                    reset: EthTxPoolResetTrigger::default(),
                    block_policy,
                    state_backend,
                    chain_config,
                    events_tx,
                    events,
                    forwarding_manager: Box::pin(EthTxPoolForwardingManager::default()),
                    preload_manager: Box::pin(EthTxPoolPreloadManager::default()),
                    metrics: Arc::new(EthTxPoolExecutorMetrics::default()),
                    executor_metrics: ExecutorMetrics::default(),
                    _phantom: PhantomData,
                }
                .run(command_rx, forwarded_rx, event_tx)
            },
            Box::new(|_| {}),
            score_reader,
            forwarded_ingress_fair_queue_config,
        )
    }

    fn start_test_client() -> EthTxPoolExecutorClient<
        SignatureType,
        SignatureCollectionType,
        StateBackendType,
        MockChainConfig,
        MockChainRevision,
    > {
        start_test_client_with_forwarded_ingress_config(ForwardedIngressFairQueueConfig::default())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_forwarded_ingress_drop_metrics_count_remaining_batch_once() {
        let mut client =
            start_test_client_with_forwarded_ingress_config(ForwardedIngressFairQueueConfig {
                per_id_limit: 2,
                max_size: 2,
                regular_per_id_limit: 2,
                regular_max_size: 2,
                ..ForwardedIngressFairQueueConfig::default()
            });

        client.exec(vec![TxPoolCommand::InsertForwardedTxs {
            sender: node_id::<SignatureType>(),
            txs: make_forwarded_txs(0, 4),
        }]);

        let metrics = client.metrics().into_inner();
        assert_eq!(
            metric_value(&metrics, "monad.bft.txpool.forwarded_ingress_enqueued_txs",),
            2
        );
        assert_eq!(
            metric_value(&metrics, "monad.bft.txpool.forwarded_ingress_dropped_txs",),
            2
        );
        assert_eq!(
            metric_value(&metrics, "monad.bft.txpool.forwarded_ingress_drop_events",),
            1
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_forwarded_ingress_buffering_and_executor_timers() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let mut client = start_test_client();
                let expected_txs = make_forwarded_txs(0, INGRESS_CHUNK_MAX_SIZE);
                let expected_txs_after_second_batch =
                    make_forwarded_txs(INGRESS_CHUNK_MAX_SIZE as u64, INGRESS_CHUNK_MAX_SIZE);
                let all_expected_txs = expected_txs
                    .iter()
                    .cloned()
                    .chain(expected_txs_after_second_batch.iter().cloned())
                    .collect::<Vec<_>>();

                client.exec(vec![TxPoolCommand::Reset {
                    last_delay_committed_blocks: vec![generate_block_with_txs(
                        GENESIS_ROUND,
                        GENESIS_SEQ_NUM,
                        MIN_BASE_FEE,
                        &MockChainConfig::DEFAULT,
                        vec![],
                    )],
                }]);

                let (proposal_tx, mut proposal_rx) = mpsc::unbounded_channel();
                let (command_tx, mut command_rx) = mpsc::unbounded_channel::<Vec<CommandType>>();
                let driver = tokio::task::spawn_local(async move {
                    loop {
                        tokio::select! {
                            biased;

                            Some(commands) = command_rx.recv() => {
                                client.exec(commands);
                            }
                            event = client.next() => {
                                let Some(event) = event else {
                                    break;
                                };

                                proposal_tx
                                    .send(collect_forwarded_txs(event))
                                    .expect("proposal receiver is alive");
                            }
                        }
                    }
                });

                tokio::task::yield_now().await;
                assert_no_proposal(
                    &mut proposal_rx,
                    "driver should not emit a proposal on its own",
                );

                command_tx
                    .send(vec![proposal_command(Round(1))])
                    .expect("proposal command is queued");
                assert_eq!(
                    recv_proposal_with_yields(
                        &mut proposal_rx,
                        MAX_YIELDS,
                        "initial proposal did not arrive within yield budget",
                    )
                    .await,
                    Vec::<bytes::Bytes>::default()
                );
                assert_no_proposal(
                    &mut proposal_rx,
                    "forwarded txs should not be in the pool before the first executor timer tick",
                );

                command_tx
                    .send(vec![TxPoolCommand::InsertForwardedTxs {
                        sender: node_id::<SignatureType>(),
                        txs: expected_txs.clone(),
                    }])
                    .expect("forwarded txs are queued");
                tokio::task::yield_now().await;

                tokio::time::advance(Duration::from_millis(4)).await;
                command_tx
                    .send(vec![proposal_command(Round(2))])
                    .expect("proposal command is queued");
                assert_eq!(
                    recv_proposal_with_yields(
                        &mut proposal_rx,
                        MAX_YIELDS,
                        "proposal after 4ms did not arrive within yield budget",
                    )
                    .await,
                    Vec::<bytes::Bytes>::default()
                );

                tokio::time::advance(Duration::from_millis(INGRESS_CHUNK_INTERVAL_MS - 4)).await;
                tokio::task::yield_now().await;

                command_tx
                    .send(vec![proposal_command(Round(3))])
                    .expect("proposal command is queued");
                assert_eq!(
                    recv_proposal_with_yields(
                        &mut proposal_rx,
                        MAX_YIELDS,
                        "proposal after first tick did not arrive within yield budget",
                    )
                    .await,
                    expected_txs
                );

                command_tx
                    .send(vec![TxPoolCommand::InsertForwardedTxs {
                        sender: node_id::<SignatureType>(),
                        txs: expected_txs_after_second_batch.clone(),
                    }])
                    .expect("forwarded txs are queued");
                tokio::task::yield_now().await;

                tokio::time::advance(Duration::from_millis(4)).await;
                command_tx
                    .send(vec![proposal_command(Round(4))])
                    .expect("proposal command is queued");
                assert_eq!(
                    recv_proposal_with_yields(
                        &mut proposal_rx,
                        MAX_YIELDS,
                        "proposal after second 4ms window did not arrive within yield budget",
                    )
                    .await,
                    expected_txs.clone()
                );

                tokio::time::advance(Duration::from_millis(INGRESS_CHUNK_INTERVAL_MS - 4)).await;
                tokio::task::yield_now().await;

                command_tx
                    .send(vec![proposal_command(Round(5))])
                    .expect("proposal command is queued");
                assert_eq!(
                    recv_proposal_with_yields(
                        &mut proposal_rx,
                        MAX_YIELDS,
                        "proposal after second full tick did not arrive within yield budget",
                    )
                    .await,
                    all_expected_txs
                );
                assert_no_proposal(
                    &mut proposal_rx,
                    "unexpected extra proposal after checking buffered forwarded batches",
                );

                driver.abort();
                let _ = driver.await;
            })
            .await;
    }
}
