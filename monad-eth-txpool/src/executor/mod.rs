use std::{io, marker::PhantomData, task::Poll, time::Duration};

use futures::{Stream, StreamExt};
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{block::BlockPolicy, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::StateBackend;
use tokio::sync::mpsc;

use crate::{ipc::EthTxPoolIpc, EthTxPool, EthTxPoolIpcConfig, TxPoolMetrics};

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
    ipc: EthTxPoolIpc,
    chain_config: CCT,

    events_tx: mpsc::UnboundedSender<MempoolEvent<SCT, EthExecutionProtocol>>,
    events: mpsc::UnboundedReceiver<MempoolEvent<SCT, EthExecutionProtocol>>,

    metrics: TxPoolMetrics,
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
            ipc: EthTxPoolIpc::new(ipc_config)?,
            chain_config,

            events_tx,
            events,

            metrics: TxPoolMetrics::default(),
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
        for command in commands {
            match command {
                TxPoolCommand::BlockCommit(committed_blocks) => {
                    for committed_block in committed_blocks {
                        BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::update_committed_block(
                            &mut self.block_policy,
                            &committed_block,
                        );
                        self.pool
                            .update_committed_block(committed_block, &mut self.metrics);
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
                    let proposed_execution_inputs = self
                        .pool
                        .create_proposal(
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
                            &mut self.metrics,
                        )
                        .expect("proposal succeeds");

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
                TxPoolCommand::InsertForwardedTxs { sender, txs } => {
                    let num_txs = txs.len();

                    let valid_encoded_txs = self.pool.insert_txs(
                        txs,
                        &self.block_policy,
                        &self.state_backend,
                        &mut self.metrics,
                    );

                    let num_valid_txs = valid_encoded_txs.len();

                    self.metrics.insert_forwarded_txs += num_valid_txs as u64;

                    if num_valid_txs != num_txs {
                        tracing::warn!(?sender, "sender forwarded bad txns");
                    }
                }
                TxPoolCommand::Reset {
                    last_delay_committed_blocks,
                } => {
                    BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::reset(
                        &mut self.block_policy,
                        last_delay_committed_blocks.iter().collect(),
                    );
                    self.pool
                        .reset(last_delay_committed_blocks, &mut self.metrics);
                }
                TxPoolCommand::EnterRound { epoch: _, round } => {
                    let proposal_gas_limit = self
                        .chain_config
                        .get_chain_revision(round)
                        .chain_params()
                        .proposal_gas_limit;
                    self.pool.set_tx_gas_limit(proposal_gas_limit);
                }
            }
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
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Self {
            pool,
            block_policy,
            state_backend,
            ipc,
            chain_config: _,

            events_tx: _,
            events,

            metrics,
            executor_metrics: _,
            _phantom,
        } = self.get_mut();

        if let Poll::Ready(result) = events.poll_recv(cx) {
            let event = result.expect("events_tx never dropped");

            return Poll::Ready(Some(MonadEvent::MempoolEvent(event)));
        };

        if let Poll::Ready(result) = ipc.poll_next_unpin(cx) {
            let txs = result.expect("txpool ipc is alive");

            let valid_encoded_txs = pool.insert_txs(txs, block_policy, state_backend, metrics);

            let num_valid_txs = valid_encoded_txs.len();

            metrics.insert_mempool_txs += num_valid_txs as u64;

            return Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::ForwardTxs(
                valid_encoded_txs,
            ))));
        }

        Poll::Pending
    }
}
