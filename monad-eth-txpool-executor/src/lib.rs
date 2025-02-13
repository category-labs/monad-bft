use std::{io, marker::PhantomData, pin::Pin, sync::atomic::AtomicU64, task::Poll, time::Duration};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_rlp::Decodable;
use futures::Stream;
use itertools::Itertools;
use metrics::EthTxPoolExecutorMetrics;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{block::BlockPolicy, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::StateBackend;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::sync::mpsc;
use tracing::{error, warn};

pub use self::ipc::EthTxPoolIpcConfig;
use self::ipc::EthTxPoolIpcServer;

mod ipc;
mod metrics;

const FORWARD_MIN_SEQ_NUM_DIFF: u64 = 5;
const FORWARD_MAX_RETRIES: usize = 2;
const FORWARD_EVENT_MAX_TXS: usize = 256;

pub struct EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pool: EthTxPool<ST, SCT, SBT>,
    block_policy: EthBlockPolicy<ST, SCT>,
    state_backend: SBT,
    chain_config: CCT,

    ipc: Pin<Box<EthTxPoolIpcServer>>,

    events_tx: mpsc::UnboundedSender<MempoolEvent<SCT, EthExecutionProtocol>>,
    events: mpsc::UnboundedReceiver<MempoolEvent<SCT, EthExecutionProtocol>>,

    metrics: EthTxPoolExecutorMetrics,
    executor_metrics: ExecutorMetrics,

    _phantom: PhantomData<CRT>,
}

impl<ST, SCT, SBT, CCT, CRT> EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn new(
        block_policy: EthBlockPolicy<ST, SCT>,
        state_backend: SBT,
        ipc_config: EthTxPoolIpcConfig,
        do_local_insert: bool,
        soft_tx_expiry: Duration,
        hard_tx_expiry: Duration,
        chain_config: CCT,
        proposal_gas_limit: u64,
    ) -> io::Result<Self> {
        let (events_tx, events) = mpsc::unbounded_channel();

        Ok(Self {
            pool: EthTxPool::new(
                do_local_insert,
                soft_tx_expiry,
                hard_tx_expiry,
                proposal_gas_limit,
            ),
            block_policy,
            state_backend,
            ipc: Box::pin(EthTxPoolIpcServer::new(ipc_config)?),
            chain_config,

            events_tx,
            events,

            metrics: EthTxPoolExecutorMetrics::default(),
            executor_metrics: ExecutorMetrics::default(),

            _phantom: PhantomData,
        })
    }
}

impl<ST, SCT, SBT, CCT, CRT> Executor for EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    type Command = TxPoolCommand<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut ipc_events = Vec::default();

        let mut ipc_projection = self.ipc.as_mut().project();

        let mut event_tracker = EthTxPoolEventTracker::new(
            &mut self.metrics.pool,
            ipc_projection.get_snapshot_manager(),
            &mut ipc_events,
        );

        for command in commands {
            match command {
                TxPoolCommand::BlockCommit(committed_blocks) => {
                    for committed_block in committed_blocks {
                        BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::update_committed_block(
                            &mut self.block_policy,
                            &committed_block,
                        );
                        self.pool
                            .update_committed_block(&mut event_tracker, committed_block);

                        let Some(forwardable_txs) = self
                            .pool
                            .get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                        else {
                            continue;
                        };

                        for forwardable_tx_chunk in forwardable_txs
                            .cloned()
                            .map(alloy_rlp::encode)
                            .map(Into::into)
                            .chunks(FORWARD_EVENT_MAX_TXS)
                            .into_iter()
                        {
                            self.events_tx
                                .send(MempoolEvent::ForwardTxs(forwardable_tx_chunk.collect_vec()))
                                .expect("events never dropped");
                        }
                    }
                }
                TxPoolCommand::CreateProposal {
                    epoch,
                    round,
                    seq_num,
                    high_qc,
                    round_signature,
                    last_round_tc,
                    tx_limit,
                    proposal_gas_limit,
                    proposal_byte_limit,
                    beneficiary,
                    timestamp_ns,
                    extending_blocks,
                    delayed_execution_results,
                } => {
                    match self.pool.create_proposal(
                        &mut event_tracker,
                        seq_num,
                        tx_limit,
                        proposal_gas_limit,
                        proposal_byte_limit,
                        beneficiary,
                        timestamp_ns,
                        round_signature.clone(),
                        extending_blocks,
                        &self.block_policy,
                        &self.state_backend,
                    ) {
                        Ok(proposed_execution_inputs) => self
                            .events_tx
                            .send(MempoolEvent::Proposal {
                                epoch,
                                round,
                                seq_num,
                                high_qc,
                                timestamp_ns,
                                round_signature,
                                delayed_execution_results,
                                proposed_execution_inputs,
                                last_round_tc,
                            })
                            .expect("events never dropped"),
                        Err(err) => {
                            error!(?err, "txpool executor failed to create proposal");
                        }
                    }
                }
                TxPoolCommand::InsertForwardedTxs { sender, mut txs } => {
                    let num_invalid_bytes = AtomicU64::default();
                    let num_invalid_signer = AtomicU64::default();

                    if txs.len() > FORWARD_EVENT_MAX_TXS {
                        warn!(
                            ?sender,
                            forwarded_txs_len = txs.len(),
                            "invalid forwarded len, truncating event"
                        );
                        txs.truncate(FORWARD_EVENT_MAX_TXS);
                    }

                    let txs = txs
                        .into_par_iter()
                        .filter_map(|raw_tx| {
                            let Ok(tx) = TxEnvelope::decode(&mut raw_tx.as_ref()) else {
                                num_invalid_bytes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                return None;
                            };

                            let Ok(signer) = tx.recover_signer() else {
                                num_invalid_signer
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                return None;
                            };

                            Some(Recovered::new_unchecked(tx, signer))
                        })
                        .collect::<Vec<_>>();

                    let num_invalid_bytes =
                        num_invalid_bytes.load(std::sync::atomic::Ordering::SeqCst);
                    let num_invalid_signer =
                        num_invalid_signer.load(std::sync::atomic::Ordering::SeqCst);

                    self.metrics.reject_forwarded_invalid_bytes += num_invalid_bytes;
                    self.metrics.reject_forwarded_invalid_signer += num_invalid_signer;

                    if num_invalid_bytes != 0 || num_invalid_signer != 0 {
                        tracing::warn!(
                            ?sender,
                            ?num_invalid_bytes,
                            ?num_invalid_signer,
                            "invalid forwarded txs"
                        );
                    }

                    self.pool.insert_txs(
                        &mut event_tracker,
                        &self.block_policy,
                        &self.state_backend,
                        txs,
                        false,
                        |_| {},
                    );
                }
                TxPoolCommand::EnterRound { epoch: _, round } => {
                    let proposal_gas_limit = self
                        .chain_config
                        .get_chain_revision(round)
                        .chain_params()
                        .proposal_gas_limit;
                    self.pool.set_tx_gas_limit(proposal_gas_limit);
                }
                TxPoolCommand::Reset {
                    last_delay_committed_blocks,
                } => {
                    BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::reset(
                        &mut self.block_policy,
                        last_delay_committed_blocks.iter().collect(),
                    );
                    self.pool
                        .reset(&mut event_tracker, last_delay_committed_blocks);
                }
            }
        }

        self.metrics.update(&mut self.executor_metrics);

        self.ipc.as_mut().broadcast_tx_events(&ipc_events);
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::default().push(&self.executor_metrics)
    }
}

impl<ST, SCT, SBT, CCT, CRT> Stream for EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,

    Self: Unpin,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Self {
            pool,
            block_policy,
            state_backend,
            chain_config: _,

            ipc,

            events_tx: _,
            events,

            metrics,
            executor_metrics,

            _phantom,
        } = self.get_mut();

        if let Poll::Ready(result) = events.poll_recv(cx) {
            let event = result.expect("events_tx never dropped");

            return Poll::Ready(Some(MonadEvent::MempoolEvent(event)));
        };

        if let Poll::Ready(result) = ipc.as_mut().poll_next(cx) {
            let txs = result.expect("txpool executor ipc server is alive");

            let mut ipc_events = Vec::default();
            let mut inserted_txs = Vec::default();

            let mut ipc_projection = ipc.as_mut().project();

            pool.insert_txs(
                &mut EthTxPoolEventTracker::new(
                    &mut metrics.pool,
                    ipc_projection.get_snapshot_manager(),
                    &mut ipc_events,
                ),
                block_policy,
                state_backend,
                txs,
                true,
                |tx| {
                    let tx: &TxEnvelope = tx.raw().tx();
                    inserted_txs.push(alloy_rlp::encode(tx).into());
                },
            );

            metrics.update(executor_metrics);

            ipc.as_mut().broadcast_tx_events(&ipc_events);

            return Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::ForwardTxs(
                inserted_txs,
            ))));
        }

        Poll::Pending
    }
}
