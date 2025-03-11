use std::{io, marker::PhantomData, pin::Pin, sync::atomic::AtomicU64, task::Poll, time::Duration};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_rlp::Decodable;
use futures::Stream;
use ipc::{EthTxPoolConfig, EthTxPoolIpcConnection};
use itertools::Itertools;
use metrics::EthTxPoolExecutorMetrics;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{block::BlockPolicy, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker};
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolEvent};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::StateBackend;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::{sync::mpsc, time::Instant};
use tracing::error;

pub use self::ipc::EthTxPoolIpcConfig;
use self::ipc::EthTxPoolIpcServer;

mod ipc;
mod metrics;

const FORWARD_MIN_SEQ_NUM_DIFF: u64 = 5;
const FORWARD_MAX_RETRIES: usize = 2;

pub struct EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    ipc: Pin<Box<EthTxPoolIpcConnection<ST, SCT, SBT>>>,
    block_policy: EthBlockPolicy<ST, SCT>,
    state_backend: SBT,
    chain_config: CCT,

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

        let pool_config = EthTxPoolConfig {
            do_local_insert,
            soft_tx_expiry,
            hard_tx_expiry,
            proposal_gas_limit,
        };

        Ok(Self {
            block_policy,
            state_backend,
            ipc: Box::pin(EthTxPoolIpcConnection::NotReady(ipc_config, pool_config)),
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

        let mut event_tracker = EthTxPoolEventTracker::new(&mut self.metrics.pool, &mut ipc_events);

        if let ipc::EthTxPoolIpcConnectionProjected::NotReady(tx_pool_ipc_config, pool_config) =
            self.ipc.as_mut().project()
        {
            // Check if we have a Reset command to initialize the server
            let should_initialize = commands
                .iter()
                .any(|cmd| matches!(cmd, TxPoolCommand::Reset { .. }));

            if should_initialize {
                let pool = EthTxPool::new(
                    pool_config.do_local_insert,
                    pool_config.soft_tx_expiry,
                    pool_config.hard_tx_expiry,
                    pool_config.proposal_gas_limit,
                );

                let server = EthTxPoolIpcServer::new(tx_pool_ipc_config.clone(), pool).unwrap();
                self.ipc = Box::pin(EthTxPoolIpcConnection::Ready(server));
            } else {
                return; // Nothing to do if not initializing
            }
        }

        if let ipc::EthTxPoolIpcConnectionProjected::Ready(mut server) = self.ipc.as_mut().project()
        {
            let ipc_projection = server.as_mut().project();

            for command in commands {
                match command {
                    TxPoolCommand::BlockCommit(committed_blocks) => {
                        for committed_block in committed_blocks {
                            BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::update_committed_block(
                                &mut self.block_policy,
                                &committed_block,
                            );
                            ipc_projection
                                .pool
                                .update_committed_block(&mut event_tracker, committed_block);

                            let Some(forwardable_txs) = ipc_projection
                                .pool
                                .get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                            else {
                                continue;
                            };

                            let forwardable_txs = forwardable_txs
                                .cloned()
                                .map(alloy_rlp::encode)
                                .map(Into::into)
                                .collect_vec();

                            if forwardable_txs.is_empty() {
                                continue;
                            }

                            self.events_tx
                                .send(MempoolEvent::ForwardTxs(forwardable_txs))
                                .expect("events never dropped");
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
                        let create_proposal_start = Instant::now();

                        match ipc_projection.pool.create_proposal(
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
                            Ok(proposed_execution_inputs) => {
                                let elapsed = create_proposal_start.elapsed();

                                self.metrics.create_proposal += 1;
                                self.metrics.create_proposal_elapsed_ns +=
                                    elapsed.as_nanos() as u64;
                                self.metrics.create_proposal_txs +=
                                    proposed_execution_inputs.body.transactions.len() as u64;
                                self.metrics.create_proposal_available_txs +=
                                    ipc_projection.pool.num_txs() as u64;

                                self.events_tx
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
                                    .expect("events never dropped");
                            }
                            Err(err) => {
                                error!(?err, "txpool executor failed to create proposal");
                            }
                        }
                    }
                    TxPoolCommand::InsertForwardedTxs { sender, txs } => {
                        let num_invalid_bytes = AtomicU64::default();
                        let num_invalid_signer = AtomicU64::default();

                        let txs = txs
                            .into_par_iter()
                            .filter_map(|raw_tx| {
                                let Ok(tx) = TxEnvelope::decode(&mut raw_tx.as_ref()) else {
                                    num_invalid_bytes
                                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

                        ipc_projection.pool.insert_txs(
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
                        ipc_projection.pool.set_tx_gas_limit(proposal_gas_limit);
                    }
                    TxPoolCommand::Reset {
                        last_delay_committed_blocks,
                    } => {
                        BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::reset(
                            &mut self.block_policy,
                            last_delay_committed_blocks.iter().collect(),
                        );

                        ipc_projection
                            .pool
                            .reset(&mut event_tracker, last_delay_committed_blocks);
                    }
                }
            }

            server.as_mut().broadcast_tx_events(&ipc_events);
        }

        self.metrics.update(&mut self.executor_metrics);
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
            let event: MempoolEvent<SCT, EthExecutionProtocol> =
                result.expect("events_tx never dropped");

            return Poll::Ready(Some(MonadEvent::MempoolEvent(event)));
        };

        // Check if the IPC connection is ready before polling it
        match ipc.as_mut().project() {
            ipc::EthTxPoolIpcConnectionProjected::Ready(server) => {
                // Only poll if the connection is ready
                if let Poll::Ready(result) = server.poll_next(cx) {
                    let mut ipc_events = Vec::default();
                    let mut inserted_txs = Vec::default();

                    let recovered_txs = {
                        let unvalidated_txs = result.expect("txpool executor ipc server is alive");
                        let (recovered_txs, dropped_txs): (Vec<_>, Vec<_>) = unvalidated_txs
                            .into_par_iter()
                            .partition_map(|tx| match tx.recover_signer() {
                                Ok(signer) => {
                                    rayon::iter::Either::Left(Recovered::new_unchecked(tx, signer))
                                }
                                Err(_) => rayon::iter::Either::Right(EthTxPoolEvent::Drop {
                                    tx_hash: *tx.tx_hash(),
                                    reason: EthTxPoolDropReason::InvalidSignature,
                                }),
                            });
                        ipc_events.extend_from_slice(&dropped_txs);
                        recovered_txs
                    };

                    // Re-project after polling to get the updated state
                    let ipc_projection = ipc.as_mut().project();

                    // This should always be Ready since we checked above, but we'll match again for safety
                    if let ipc::EthTxPoolIpcConnectionProjected::Ready(mut server) = ipc_projection
                    {
                        let server_projection = server.as_mut().project();

                        server_projection.pool.insert_txs(
                            &mut EthTxPoolEventTracker::new(&mut metrics.pool, &mut ipc_events),
                            block_policy,
                            state_backend,
                            recovered_txs,
                            true,
                            |tx| {
                                let tx: &TxEnvelope = tx.raw().tx();
                                inserted_txs.push(alloy_rlp::encode(tx).into());
                            },
                        );

                        metrics.update(executor_metrics);

                        server.as_mut().broadcast_tx_events(&ipc_events);

                        return Poll::Ready(Some(MonadEvent::MempoolEvent(
                            MempoolEvent::ForwardTxs(inserted_txs),
                        )));
                    }
                }
            }
            ipc::EthTxPoolIpcConnectionProjected::NotReady(_, _) => {
                // Connection is not ready, don't poll it
                // Just continue to return Poll::Pending below
            }
        }

        Poll::Pending
    }
}
