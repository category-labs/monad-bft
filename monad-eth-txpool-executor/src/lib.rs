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
    EthTxPool, EthTxPoolConfig, EthTxPoolEventTracker, PoolTransactionKind, TrackedTxLimitsConfig,
};
use monad_eth_txpool_ipc::EthTxPoolIpcTx;
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolEventType};
use monad_eth_types::{EthExecutionProtocol, ExtractEthAddress};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_secp::RecoverableAddress;
use monad_state_backend::StateBackend;
use monad_types::{DropTimer, Round};
use monad_validator::signature_collection::SignatureCollection;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::{sync::mpsc, time::Instant};
use tracing::{debug, debug_span, error, info, trace_span, warn};

pub use self::{client::EthTxPoolExecutorClient, ipc::EthTxPoolIpcConfig};
use self::{
    client::ForwardedTxs, forward::EthTxPoolForwardingManager, ipc::EthTxPoolIpcServer,
    metrics::EthTxPoolExecutorMetrics, preload::EthTxPoolPreloadManager,
    reset::EthTxPoolResetTrigger,
};

mod client;
pub mod forward;
mod ipc;
mod metrics;
mod preload;
mod reset;

pub struct EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pool: EthTxPool<ST, SCT, SBT, CCT, CRT>,
    ipc: Pin<Box<EthTxPoolIpcServer>>,

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

impl<ST, SCT, SBT, CCT, CRT> EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
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
        do_local_insert: bool,
    ) -> io::Result<EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>> {
        let ipc = Box::pin(EthTxPoolIpcServer::new(ipc_config)?);

        let (events_tx, events) = mpsc::unbounded_channel();

        let metrics = Arc::new(EthTxPoolExecutorMetrics::default());
        let mut executor_metrics = ExecutorMetrics::default();

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
                            do_local_insert,
                        },
                        chain_config.chain_id(),
                        chain_config.get_chain_revision(round),
                        chain_config.get_execution_chain_revision(execution_timestamp_s),
                    );

                    Self {
                        pool,
                        ipc,
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
        ))
    }

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

        loop {
            tokio::select! {
                biased;

                result = command_rx.recv() => {
                    let Some(commands) = result else {
                        warn!("command channel was dropped, shutting down txpool executor");
                        break;
                    };

                    self.exec(commands);
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

                result = forwarded_rx.recv() => {
                    let Some(forwarded_txs) = result else {
                        warn!("forwarded channel was dropped, shutting down txpool executor");
                        break;
                    };

                    self.process_forwarded_txs(forwarded_txs);
                }
            }
        }
    }
}

impl<ST, SCT, SBT, CCT, CRT> EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
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

impl<ST, SCT, SBT, CCT, CRT> Executor for EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
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
                            &self.chain_config,
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

                    // Some() if tfm is enabled, else None
                    let maybe_tfm_base_fees = self.block_policy.compute_base_fee(
                        &extending_blocks,
                        &self.chain_config,
                        timestamp_ns,
                    );

                    let (base_fee, base_fee_field, base_fee_trend_field, base_fee_moment_field) =
                        match maybe_tfm_base_fees {
                            Some((base_fee, base_fee_trend, base_fee_moment)) => (
                                base_fee,
                                Some(base_fee),
                                Some(base_fee_trend),
                                Some(base_fee_moment),
                            ),
                            None => (monad_tfm::base_fee::PRE_TFM_BASE_FEE, None, None, None),
                        };

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
                                    base_fee: base_fee_field,
                                    base_fee_trend: base_fee_trend_field,
                                    base_fee_moment: base_fee_moment_field,
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
                        &self.chain_config,
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

        self.ipc.as_mut().broadcast_tx_events(ipc_events);
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default().push(&self.executor_metrics)
    }
}

impl<ST, SCT, SBT, CCT, CRT> Stream for EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,

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
            ipc,

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

        if let Poll::Ready(unvalidated_txs) = ipc.as_mut().poll_txs(cx, || pool.generate_snapshot())
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
                                    PoolTransactionKind::Owned {
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
                        immediately_forwardable_txs.push(tx.raw().tx().clone());
                    }
                },
            );

            preload_manager.add_requests(inserted_addresses.iter());

            forwarding_manager
                .as_mut()
                .project()
                .add_egress_txs(immediately_forwardable_txs.iter());

            metrics.update(executor_metrics);
            ipc.as_mut().broadcast_tx_events(ipc_events);

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
                                PoolTransactionKind::Forwarded,
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

            forwarding_manager.as_mut().complete_ingress();
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
        ipc.as_mut().broadcast_tx_events(ipc_events);

        Poll::Pending
    }
}
