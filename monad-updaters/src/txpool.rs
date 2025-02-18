use std::{
    collections::VecDeque,
    task::{Poll, Waker},
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_rlp::Decodable;
use bytes::Bytes;
use futures::Stream;
use monad_consensus_types::{
    block::{
        BlockPolicy, MockExecutionBody, MockExecutionProposedHeader, MockExecutionProtocol,
        ProposedExecutionInputs,
    },
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::StateBackend;
use monad_types::ExecutionProtocol;

pub trait MockableTxPool:
    Executor<
        Command = TxPoolCommand<
            Self::Signature,
            Self::SignatureCollection,
            Self::ExecutionProtocol,
            Self::BlockPolicy,
            Self::StateBackend,
        >,
    > + Stream<Item = Self::Event>
    + Unpin
{
    type Signature: CertificateSignatureRecoverable;
    type SignatureCollection: SignatureCollection<
        NodeIdPubKey = CertificateSignaturePubKey<Self::Signature>,
    >;
    type ExecutionProtocol: ExecutionProtocol;
    type BlockPolicy: BlockPolicy<
        Self::Signature,
        Self::SignatureCollection,
        Self::ExecutionProtocol,
        Self::StateBackend,
    >;
    type StateBackend: StateBackend;

    type Event;

    fn ready(&self) -> bool;

    fn send_transaction(&mut self, tx: Bytes);
}

impl<T: MockableTxPool + ?Sized> MockableTxPool for Box<T> {
    type Signature = T::Signature;
    type SignatureCollection = T::SignatureCollection;
    type ExecutionProtocol = T::ExecutionProtocol;
    type BlockPolicy = T::BlockPolicy;
    type StateBackend = T::StateBackend;

    type Event = T::Event;

    fn ready(&self) -> bool {
        (**self).ready()
    }

    fn send_transaction(&mut self, tx: Bytes) {
        (**self).send_transaction(tx);
    }
}

pub struct MockTxPoolExecutor<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // This field is only populated when the execution protocol is EthExecutionProtocol
    eth: Option<(EthTxPool<ST, SCT, SBT>, BPT, SBT)>,

    events: VecDeque<MempoolEvent<SCT, EPT>>,
    waker: Option<Waker>,

    metrics: EthTxPoolMetrics,
    executor_metrics: ExecutorMetrics,
}

impl<ST, SCT, BPT, SBT> Default for MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    fn default() -> Self {
        Self {
            eth: None,

            events: VecDeque::default(),
            waker: None,

            metrics: EthTxPoolMetrics::default(),
            executor_metrics: ExecutorMetrics::default(),
        }
    }
}

impl<ST, SCT, SBT> MockTxPoolExecutor<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    pub fn new(block_policy: EthBlockPolicy<ST, SCT>, state_backend: SBT) -> Self {
        Self {
            eth: Some((EthTxPool::default_testing(), block_policy, state_backend)),

            events: VecDeque::default(),
            waker: None,

            metrics: EthTxPoolMetrics::default(),
            executor_metrics: ExecutorMetrics::default(),
        }
    }
}

impl<ST, SCT, BPT, SBT> Executor for MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<ST, SCT, MockExecutionProtocol, SBT>,
    SBT: StateBackend,
{
    type Command = TxPoolCommand<ST, SCT, MockExecutionProtocol, BPT, SBT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                TxPoolCommand::CreateProposal {
                    epoch,
                    round,
                    seq_num,
                    high_qc,
                    round_signature,
                    last_round_tc,
                    tx_limit: _,
                    proposal_gas_limit: _,
                    proposal_byte_limit: _,
                    beneficiary: _,
                    timestamp_ns,
                    extending_blocks: _,
                    delayed_execution_results,
                } => {
                    self.events.push_back(MempoolEvent::Proposal {
                        epoch,
                        round,
                        seq_num,
                        high_qc,
                        timestamp_ns,
                        round_signature,
                        delayed_execution_results,
                        proposed_execution_inputs: ProposedExecutionInputs {
                            header: MockExecutionProposedHeader::default(),
                            body: MockExecutionBody::default(),
                        },
                        last_round_tc,
                    });

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                TxPoolCommand::BlockCommit(_) | TxPoolCommand::Reset { .. } => {}
                TxPoolCommand::InsertForwardedTxs { .. } => {
                    unimplemented!(
                        "MockTxPoolExecutor should never recieve txs with MockExecutionProtocol"
                    );
                }
                TxPoolCommand::EnterRound { .. } => {}
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::default()
    }
}

impl<ST, SCT, SBT> Executor
    for MockTxPoolExecutor<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    type Command = TxPoolCommand<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let (pool, block_policy, state_backend) = self.eth.as_mut().unwrap();

        let mut events = Vec::default();
        let mut event_tracker = EthTxPoolEventTracker::new(&mut self.metrics, &mut events);

        for command in commands {
            match command {
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
                    let proposed_execution_inputs = pool
                        .create_proposal(
                            &mut event_tracker,
                            seq_num,
                            tx_limit,
                            proposal_gas_limit,
                            proposal_byte_limit,
                            beneficiary,
                            timestamp_ns,
                            round_signature.clone(),
                            extending_blocks,
                            block_policy,
                            state_backend,
                        )
                        .expect("proposal succeeds");

                    self.events.push_back(MempoolEvent::Proposal {
                        epoch,
                        round,
                        seq_num,
                        high_qc,
                        timestamp_ns,
                        round_signature,
                        delayed_execution_results,
                        proposed_execution_inputs,
                        last_round_tc,
                    });

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                TxPoolCommand::BlockCommit(committed_blocks) => {
                    for committed_block in committed_blocks {
                        BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::update_committed_block(
                            block_policy,
                            &committed_block,
                        );
                        pool.update_committed_block(&mut event_tracker, committed_block);
                    }
                }
                TxPoolCommand::Reset {
                    last_delay_committed_blocks,
                } => {
                    BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::reset(
                        block_policy,
                        last_delay_committed_blocks.iter().collect(),
                    );
                    pool.reset(&mut event_tracker, last_delay_committed_blocks);
                }
                TxPoolCommand::InsertForwardedTxs { sender: _, txs } => {
                    pool.insert_txs(
                        &mut event_tracker,
                        block_policy,
                        state_backend,
                        txs.into_iter()
                            .filter_map(|raw_tx| {
                                let tx = TxEnvelope::decode(&mut raw_tx.as_ref()).ok()?;
                                let signer = tx.recover_signer().ok()?;
                                Some(Recovered::new_unchecked(tx, signer))
                            })
                            .collect(),
                        false,
                        |_| {},
                    );
                }
                // TODO: add chain config to MockTxPoolExecutor if we're testing
                // param forking with it
                TxPoolCommand::EnterRound { .. } => {}
            }
        }

        self.metrics.update(&mut self.executor_metrics);
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::default().push(&self.executor_metrics)
    }
}

impl<ST, SCT, EPT, BPT, SBT> Stream for MockTxPoolExecutor<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,

    Self: Unpin,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(Some(MonadEvent::MempoolEvent(event)));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<ST, SCT, BPT, SBT> MockableTxPool
    for MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<ST, SCT, MockExecutionProtocol, SBT>,
    SBT: StateBackend,

    Self: Executor<Command = TxPoolCommand<ST, SCT, MockExecutionProtocol, BPT, SBT>> + Unpin,
{
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = MockExecutionProtocol;
    type BlockPolicy = BPT;
    type StateBackend = SBT;

    type Event = MonadEvent<ST, SCT, MockExecutionProtocol>;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn send_transaction(&mut self, _: Bytes) {
        unreachable!(
            "MockTxPoolExecutor does not support send_transaction with MockExecutionProtocol"
        );
    }
}

impl<ST, SCT, SBT> MockableTxPool
    for MockTxPoolExecutor<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,

    Self: Executor<
            Command = TxPoolCommand<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>,
        > + Unpin,
{
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = EthExecutionProtocol;
    type BlockPolicy = EthBlockPolicy<ST, SCT>;
    type StateBackend = SBT;

    type Event = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn send_transaction(&mut self, tx: Bytes) {
        let (pool, block_policy, state_backend) = self.eth.as_mut().unwrap();

        let Ok(tx) = TxEnvelope::decode(&mut tx.as_ref()) else {
            panic!("MockableTxPool received invalid tx bytes!");
        };

        let Ok(signer) = tx.recover_signer() else {
            panic!("MockableTxPool received tx with invalid signer");
        };

        let tx = Recovered::new_unchecked(tx, signer);

        pool.insert_txs(
            &mut EthTxPoolEventTracker::new(&mut self.metrics, &mut Vec::default()),
            block_policy,
            state_backend,
            vec![tx],
            true,
            |tx| {
                self.events.push_back(MempoolEvent::ForwardTxs(vec![
                    alloy_rlp::encode(tx.raw()).into()
                ]));
            },
        );

        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}
