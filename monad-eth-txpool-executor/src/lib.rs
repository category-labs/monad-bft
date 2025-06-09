use std::{io, marker::PhantomData, pin::Pin, sync::atomic::AtomicU64, task::Poll, time::Duration};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_rlp::Decodable;
use futures::Stream;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{block::BlockPolicy, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker};
use monad_eth_txpool_metrics::TxPoolMetrics;
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolEvent};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::Executor;
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::StateBackend;
use monad_types::DropTimer;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::{sync::mpsc, time::Instant};
use tracing::{debug, error, info};

pub use self::ipc::EthTxPoolIpcConfig;
use self::{forward::EthTxPoolForwardingManager, ipc::EthTxPoolIpcServer};

mod forward;
mod ipc;

pub struct EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pool: EthTxPool<ST, SCT, SBT>,
    ipc: Pin<Box<EthTxPoolIpcServer>>,
    // Flag used to skip polling the ipc socket until EthTxPool has been reset after state syncing.
    // TODO(phil): Remove this once RPC uses execution events to decide if state syncing is done.
    has_been_reset: bool,
    block_policy: EthBlockPolicy<ST, SCT>,
    state_backend: SBT,
    chain_config: CCT,

    events_tx: mpsc::UnboundedSender<MempoolEvent<SCT, EthExecutionProtocol>>,
    events: mpsc::UnboundedReceiver<MempoolEvent<SCT, EthExecutionProtocol>>,

    forwarding_manager: Pin<Box<EthTxPoolForwardingManager>>,

    metrics: TxPoolMetrics,

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

        let pool = EthTxPool::new(
            do_local_insert,
            soft_tx_expiry,
            hard_tx_expiry,
            proposal_gas_limit,
            // it's safe to default max_code_size to zero because it gets set on commit + reset
            0,
        );

        Ok(Self {
            pool,
            ipc: Box::pin(EthTxPoolIpcServer::new(ipc_config)?),
            block_policy,
            has_been_reset: false,
            state_backend,
            chain_config,

            events_tx,
            events,

            forwarding_manager: Box::pin(EthTxPoolForwardingManager::new()),

            metrics: TxPoolMetrics::default(),

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
    type Metrics = TxPoolMetrics;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut ipc_events = Vec::default();

        let mut event_tracker = EthTxPoolEventTracker::new(&mut self.metrics.pool, &mut ipc_events);

        for command in commands {
            match command {
                TxPoolCommand::BlockCommit(committed_blocks) => {
                    for committed_block in committed_blocks {
                        BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::update_committed_block(
                            &mut self.block_policy,
                            &committed_block,
                        );

                        let execution_revision = self.chain_config.get_execution_chain_revision(
                            committed_block.header().execution_inputs.timestamp,
                        );
                        self.pool.set_max_code_size(
                            execution_revision.execution_chain_params().max_code_size,
                        );

                        self.pool
                            .update_committed_block(&mut event_tracker, committed_block);
                    }

                    self.forwarding_manager
                        .as_mut()
                        .project()
                        .add_egress_txs(&mut self.pool);
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
                        Ok(proposed_execution_inputs) => {
                            let elapsed = create_proposal_start.elapsed();

                            self.metrics.create_proposal.inc();
                            self.metrics
                                .create_proposal_elapsed_ns
                                .add(elapsed.as_nanos() as u64);

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
                    debug!(
                        ?sender,
                        num_txs = txs.len(),
                        "txpool executor received forwarded txs"
                    );

                    let num_invalid_bytes = AtomicU64::default();
                    let num_invalid_signer = AtomicU64::default();

                    let recovered_txs = txs
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

                    self.metrics
                        .reject_forwarded
                        .invalid_bytes
                        .add(num_invalid_bytes);
                    self.metrics
                        .reject_forwarded
                        .invalid_signer
                        .add(num_invalid_signer);

                    if num_invalid_bytes != 0 || num_invalid_signer != 0 {
                        tracing::warn!(
                            ?sender,
                            ?num_invalid_bytes,
                            ?num_invalid_signer,
                            "invalid forwarded txs"
                        );
                    }

                    self.forwarding_manager
                        .as_mut()
                        .project()
                        .add_ingress_txs(recovered_txs);
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

                    if let Some(block) = last_delay_committed_blocks.last() {
                        let execution_revision = self.chain_config.get_execution_chain_revision(
                            block.header().execution_inputs.timestamp,
                        );
                        self.pool.set_max_code_size(
                            execution_revision.execution_chain_params().max_code_size,
                        );
                    }

                    self.pool
                        .reset(&mut event_tracker, last_delay_committed_blocks);

                    self.has_been_reset = true;
                }
            }
        }

        self.ipc.as_mut().broadcast_tx_events(&ipc_events);
    }

    fn metrics(&self) -> &Self::Metrics {
        &self.metrics
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
        let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
            info!(?elapsed, "txpool executor long poll");
        });

        let Self {
            pool,
            ipc,

            has_been_reset,
            block_policy,
            state_backend,
            chain_config: _,

            events_tx: _,
            events,

            forwarding_manager,

            metrics,

            _phantom,
        } = self.get_mut();

        if let Poll::Ready(result) = events.poll_recv(cx) {
            let event = result.expect("events_tx never dropped");

            return Poll::Ready(Some(MonadEvent::MempoolEvent(event)));
        };

        if !*has_been_reset {
            return Poll::Pending;
        }

        if let Poll::Ready(forward_txs) = forwarding_manager.as_mut().poll_egress(cx) {
            return Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::ForwardTxs(
                forward_txs,
            ))));
        }

        while let Poll::Ready(forwarded_txs) = forwarding_manager.as_mut().poll_ingress(cx) {
            let mut ipc_events = Vec::default();

            pool.insert_txs(
                &mut EthTxPoolEventTracker::new(&mut metrics.pool, &mut ipc_events),
                block_policy,
                state_backend,
                forwarded_txs,
                false,
                |_| {},
            );

            ipc.as_mut().broadcast_tx_events(&ipc_events);

            forwarding_manager.as_mut().complete_ingress();
        }

        if let Poll::Ready(unvalidated_txs) = ipc.as_mut().poll_txs(cx, || pool.generate_snapshot())
        {
            let mut ipc_events = Vec::default();
            let mut inserted_txs = Vec::default();

            let recovered_txs = {
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

            pool.insert_txs(
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

            ipc.as_mut().broadcast_tx_events(&ipc_events);

            return Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::ForwardTxs(
                inserted_txs,
            ))));
        }

        Poll::Pending
    }
}
