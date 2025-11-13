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
    collections::{BTreeMap, VecDeque},
    task::{Poll, Waker},
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_rlp::Decodable;
use bytes::Bytes;
use futures::Stream;
use monad_chain_config::{
    revision::{ChainParams, ChainRevision, MockChainRevision},
    ChainConfig, MockChainConfig,
};
use monad_consensus_types::block::{
    BlockPolicy, MockExecutionBody, MockExecutionProposedHeader, MockExecutionProtocol,
    ProposedExecutionInputs,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics, PoolTransactionKind};
use monad_eth_types::{EthExecutionProtocol, ExtractEthAddress};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::StateBackend;
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;

pub trait MockableTxPool:
    Executor<
        Command = TxPoolCommand<
            Self::Signature,
            Self::SignatureCollection,
            Self::ExecutionProtocol,
            Self::BlockPolicy,
            Self::StateBackend,
            Self::ChainConfig,
            Self::ChainRevision,
        >,
    > + Stream<Item = Self::Event>
    + Unpin
{
    type Signature: CertificateSignatureRecoverable;
    type SignatureCollection: SignatureCollection<
        NodeIdPubKey = CertificateSignaturePubKey<Self::Signature>,
    >;
    type ExecutionProtocol: ExecutionProtocol;
    type ChainConfig: ChainConfig<Self::ChainRevision>;
    type ChainRevision: ChainRevision;
    type BlockPolicy: BlockPolicy<
        Self::Signature,
        Self::SignatureCollection,
        Self::ExecutionProtocol,
        Self::StateBackend,
        Self::ChainConfig,
        Self::ChainRevision,
    >;
    type StateBackend: StateBackend<Self::Signature, Self::SignatureCollection>;

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
    type ChainConfig = T::ChainConfig;
    type ChainRevision = T::ChainRevision;

    type Event = T::Event;

    fn ready(&self) -> bool {
        (**self).ready()
    }

    fn send_transaction(&mut self, tx: Bytes) {
        (**self).send_transaction(tx);
    }
}

pub struct MockTxPoolExecutor<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    // This field is only populated when the execution protocol is EthExecutionProtocol
    eth: Option<(EthTxPool<ST, SCT, SBT, CCT, CRT>, BPT, SBT)>,
    chain_config: CCT,

    events: VecDeque<MempoolEvent<ST, SCT, EPT>>,
    waker: Option<Waker>,

    metrics: EthTxPoolMetrics,
    executor_metrics: ExecutorMetrics,
}

impl<ST, SCT, BPT, SBT, CCT, CRT> Default
    for MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT> + Default,
    CRT: ChainRevision,
{
    fn default() -> Self {
        Self {
            eth: None,
            chain_config: CCT::default(),

            events: VecDeque::default(),
            waker: None,

            metrics: EthTxPoolMetrics::default(),
            executor_metrics: ExecutorMetrics::default(),
        }
    }
}

impl<ST, SCT, SBT, CCT, CRT>
    MockTxPoolExecutor<
        ST,
        SCT,
        EthExecutionProtocol,
        EthBlockPolicy<ST, SCT, CCT, CRT>,
        SBT,
        MockChainConfig,
        MockChainRevision,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
{
    pub fn new(block_policy: EthBlockPolicy<ST, SCT, CCT, CRT>, state_backend: SBT) -> Self {
        Self {
            eth: Some((EthTxPool::default_testing(), block_policy, state_backend)),
            chain_config: MockChainConfig::DEFAULT,

            events: VecDeque::default(),
            waker: None,

            metrics: EthTxPoolMetrics::default(),
            executor_metrics: ExecutorMetrics::default(),
        }
    }

    pub fn with_chain_params(mut self, chain_params: &'static ChainParams) -> Self {
        self.chain_config = MockChainConfig::new(chain_params);
        self
    }
}

impl<ST, SCT, BPT, SBT>
    MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, BPT, SBT, MockChainConfig, MockChainRevision>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    pub fn with_chain_params(mut self, chain_params: &'static ChainParams) -> Self {
        self.chain_config = MockChainConfig::new(chain_params);
        self
    }
}

impl<ST, SCT, BPT, SBT> Executor
    for MockTxPoolExecutor<
        ST,
        SCT,
        MockExecutionProtocol,
        BPT,
        SBT,
        MockChainConfig,
        MockChainRevision,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<ST, SCT, MockExecutionProtocol, SBT, MockChainConfig, MockChainRevision>,
    SBT: StateBackend<ST, SCT>,
{
    type Command =
        TxPoolCommand<ST, SCT, MockExecutionProtocol, BPT, SBT, MockChainConfig, MockChainRevision>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                TxPoolCommand::CreateProposal {
                    node_id: _,
                    epoch,
                    round,
                    seq_num,
                    high_qc,
                    round_signature,
                    last_round_tc,
                    fresh_proposal_certificate,
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
                        base_fee: Some(monad_tfm::base_fee::MIN_BASE_FEE),
                        base_fee_trend: Some(monad_tfm::base_fee::GENESIS_BASE_FEE_TREND),
                        base_fee_moment: Some(monad_tfm::base_fee::GENESIS_BASE_FEE_MOMENT),
                        delayed_execution_results,
                        proposed_execution_inputs: ProposedExecutionInputs {
                            header: MockExecutionProposedHeader::default(),
                            body: MockExecutionBody::default(),
                        },
                        last_round_tc,
                        fresh_proposal_certificate,
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
    for MockTxPoolExecutor<
        ST,
        SCT,
        EthExecutionProtocol,
        EthBlockPolicy<ST, SCT, MockChainConfig, MockChainRevision>,
        SBT,
        MockChainConfig,
        MockChainRevision,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
{
    type Command = TxPoolCommand<
        ST,
        SCT,
        EthExecutionProtocol,
        EthBlockPolicy<ST, SCT, MockChainConfig, MockChainRevision>,
        SBT,
        MockChainConfig,
        MockChainRevision,
    >;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let (pool, block_policy, state_backend) = self.eth.as_mut().unwrap();

        let mut events = BTreeMap::default();
        let mut event_tracker = EthTxPoolEventTracker::new(&self.metrics, &mut events);

        for command in commands {
            match command {
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
                    // Some() if tfm is enabled, else None
                    let maybe_tfm_base_fees = block_policy.compute_base_fee(
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

                    let proposed_execution_inputs = pool
                        .create_proposal(
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
                            block_policy,
                            state_backend,
                            &self.chain_config,
                        )
                        .expect("proposal succeeds");

                    self.events.push_back(MempoolEvent::Proposal {
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
                    });

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                TxPoolCommand::BlockCommit(committed_blocks) => {
                    for committed_block in committed_blocks {
                        BlockPolicy::<
                            ST,
                            SCT,
                            EthExecutionProtocol,
                            SBT,
                            MockChainConfig,
                            MockChainRevision,
                        >::update_committed_block(
                            block_policy,
                            &committed_block,
                            &self.chain_config,
                        );
                        pool.update_committed_block(
                            &mut event_tracker,
                            &self.chain_config,
                            committed_block,
                        );
                    }
                }
                TxPoolCommand::Reset {
                    last_delay_committed_blocks,
                } => {
                    BlockPolicy::<
                        ST,
                        SCT,
                        EthExecutionProtocol,
                        SBT,
                        MockChainConfig,
                        MockChainRevision,
                    >::reset(
                        block_policy,
                        last_delay_committed_blocks.iter().collect(),
                        &self.chain_config,
                    );
                    pool.reset(
                        &mut event_tracker,
                        &self.chain_config,
                        last_delay_committed_blocks,
                    );
                }
                TxPoolCommand::InsertForwardedTxs { sender: _, txs } => {
                    pool.insert_txs(
                        &mut event_tracker,
                        block_policy,
                        state_backend,
                        &self.chain_config,
                        txs.into_iter()
                            .filter_map(|raw_tx| {
                                let tx = TxEnvelope::decode(&mut raw_tx.as_ref()).ok()?;
                                let signer = tx.recover_signer().ok()?;
                                Some((
                                    Recovered::new_unchecked(tx, signer),
                                    PoolTransactionKind::Forwarded,
                                ))
                            })
                            .collect(),
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

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> Stream
    for MockTxPoolExecutor<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,

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

impl<ST, SCT, BPT, SBT, CCT, CRT> MockableTxPool
    for MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<ST, SCT, MockExecutionProtocol, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,

    Self: Executor<Command = TxPoolCommand<ST, SCT, MockExecutionProtocol, BPT, SBT, CCT, CRT>>
        + Unpin,
{
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = MockExecutionProtocol;
    type BlockPolicy = BPT;
    type StateBackend = SBT;
    type ChainConfig = CCT;
    type ChainRevision = CRT;

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
    for MockTxPoolExecutor<
        ST,
        SCT,
        EthExecutionProtocol,
        EthBlockPolicy<ST, SCT, MockChainConfig, MockChainRevision>,
        SBT,
        MockChainConfig,
        MockChainRevision,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,

    Self: Executor<
            Command = TxPoolCommand<
                ST,
                SCT,
                EthExecutionProtocol,
                EthBlockPolicy<ST, SCT, MockChainConfig, MockChainRevision>,
                SBT,
                MockChainConfig,
                MockChainRevision,
            >,
        > + Unpin,
{
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = EthExecutionProtocol;
    type BlockPolicy = EthBlockPolicy<ST, SCT, MockChainConfig, MockChainRevision>;
    type StateBackend = SBT;
    type ChainConfig = MockChainConfig;
    type ChainRevision = MockChainRevision;

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
            &mut EthTxPoolEventTracker::new(&self.metrics, &mut BTreeMap::default()),
            block_policy,
            state_backend,
            &MockChainConfig::DEFAULT,
            vec![(tx, PoolTransactionKind::Owned)],
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
