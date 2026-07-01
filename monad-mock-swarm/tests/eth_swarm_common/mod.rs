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

#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    time::Duration,
};

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_primitives::Address;
use monad_bls::BlsSignatureCollection;
use monad_chain_config::{
    revision::{ChainParams, MockChainRevision},
    MockChainConfig,
};
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_ledger::MockEthLedger;
use monad_eth_types::EthExecutionProtocol;
use monad_execution_state_read::{
    AccountState, InMemoryBlockState, InMemoryState, InMemoryStateInner,
};
use monad_mock_swarm::{
    mock::TimestamperConfig,
    mock_swarm::{Nodes, SwarmBuilder},
    node::NodeBuilder,
    sim::{SimNodeBuilder, SimSwarm, DEFAULT_TIMESTAMP_PERIOD},
    swarm::make_state_configs,
    swarm_relation::SwarmRelation,
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_secp::{PubKey, SecpSignature};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer, ID};
use monad_types::{NodeId, SeqNum, GENESIS_SEQ_NUM};
use monad_updaters::{
    ledger::MockableLedger,
    statesync::MockStateSyncExecutor,
    txpool::{ByzantineConfig, MockTxPoolExecutor},
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

pub struct EthSwarm;
impl SwarmRelation for EthSwarm {
    type SignatureType = SecpSignature;
    type SignatureCollectionType = BlsSignatureCollection<PubKey>;
    type ExecutionProtocolType = EthExecutionProtocol;
    type ExecutionStateReadType = InMemoryState<Self::SignatureType, Self::SignatureCollectionType>;
    type BlockPolicyType = EthBlockPolicy<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ChainConfigType,
        Self::ChainRevisionType,
    >;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;

    type TransportMessage = VerifiedMonadMessage<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;

    type BlockValidator = EthBlockValidator<Self::SignatureType, Self::SignatureCollectionType>;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type Ledger = MockEthLedger<Self::SignatureType, Self::SignatureCollectionType>;

    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
        VerifiedMonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type ValSetUpdater = MockValSetUpdaterNop<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
    type TxPoolExecutor = MockTxPoolExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
        Self::BlockPolicyType,
        Self::ExecutionStateReadType,
        Self::ChainConfigType,
        Self::ChainRevisionType,
    >;
    type StateSyncExecutor = MockStateSyncExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
}

pub const CONSENSUS_DELTA: Duration = Duration::from_millis(100);
pub const BASE_FEE: u128 = monad_tfm::base_fee::MIN_BASE_FEE as u128;
pub const GAS_LIMIT: u64 = 30000;

pub static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 10_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(0),
};

pub fn eth_swarm_config_with_accounts(
    num_nodes: u16,
    existing_accounts: BTreeMap<Address, AccountState>,
    txpool_byzantine_config: impl Fn(usize) -> ByzantineConfig,
    validate_reserve_balance: bool,
) -> SwarmBuilder<EthSwarm> {
    let epoch_length = SeqNum(2000);
    let execution_delay = SeqNum(4);

    let chain_config = MockChainConfig::new(&CHAIN_PARAMS);

    let create_block_policy = || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0);

    let state_configs = make_state_configs::<EthSwarm>(
        num_nodes,
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        EthBlockValidator::default,
        create_block_policy,
        || {
            let state = InMemoryStateInner::new(
                execution_delay,
                InMemoryBlockState::genesis(existing_accounts.clone()),
            );
            if validate_reserve_balance {
                state.lock().unwrap().validate_reserve_balance = true;
            }
            state
        },
        execution_delay,
        CONSENSUS_DELTA,
        chain_config,
        SeqNum(100),
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    SwarmBuilder::<EthSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(idx, state_builder)| {
                let validators = state_builder.locked_epoch_validators[0].clone();
                let state_read = state_builder.state_read.clone();
                NodeBuilder::<EthSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockValSetUpdaterNop::new(validators.validators, epoch_length),
                    MockTxPoolExecutor::new(create_block_policy(), state_read.clone())
                        .with_chain_params(&CHAIN_PARAMS)
                        .with_byzantine_config(txpool_byzantine_config(idx)),
                    MockEthLedger::new(state_read.clone()),
                    MockStateSyncExecutor::new(state_read),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
                        CONSENSUS_DELTA,
                    ))],
                    vec![],
                    TimestamperConfig::default(),
                    idx.try_into().unwrap(),
                )
            })
            .collect(),
    )
}

pub fn generate_eth_swarm_with_accounts(
    num_nodes: u16,
    existing_accounts: BTreeMap<Address, AccountState>,
    txpool_byzantine_config: impl Fn(usize) -> ByzantineConfig,
    validate_reserve_balance: bool,
) -> Nodes<EthSwarm> {
    eth_swarm_config_with_accounts(
        num_nodes,
        existing_accounts,
        txpool_byzantine_config,
        validate_reserve_balance,
    )
    .build()
}

/// The `monad-sim` counterpart of [`generate_eth_swarm`]: returns the node
/// builders ready to feed into [`SimSwarm::from_builders`]. Constructed directly
/// (no transformer pipelines) so the new framework stays independent of the
/// legacy `SwarmBuilder` / `NodeBuilder` path.
pub fn eth_swarm_config(
    num_nodes: u16,
    existing_accounts: impl IntoIterator<Item = Address>,
    txpool_byzantine_config: impl Fn(usize) -> ByzantineConfig,
) -> Vec<SimNodeBuilder<EthSwarm>> {
    let existing_accounts: BTreeMap<Address, AccountState> = existing_accounts
        .into_iter()
        .map(|acc| (acc, AccountState::max_balance()))
        .collect();
    eth_sim_builders_with_accounts(num_nodes, existing_accounts, txpool_byzantine_config, false)
}

/// `monad-sim` builders mirroring [`eth_swarm_config_with_accounts`].
pub fn eth_sim_builders_with_accounts(
    num_nodes: u16,
    existing_accounts: BTreeMap<Address, AccountState>,
    txpool_byzantine_config: impl Fn(usize) -> ByzantineConfig,
    validate_reserve_balance: bool,
) -> Vec<SimNodeBuilder<EthSwarm>> {
    let epoch_length = SeqNum(2000);
    let execution_delay = SeqNum(4);
    let chain_config = MockChainConfig::new(&CHAIN_PARAMS);
    let create_block_policy = || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0);

    let state_configs = make_state_configs::<EthSwarm>(
        num_nodes,
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        EthBlockValidator::default,
        create_block_policy,
        || {
            let state = InMemoryStateInner::new(
                execution_delay,
                InMemoryBlockState::genesis(existing_accounts.clone()),
            );
            if validate_reserve_balance {
                state.lock().unwrap().validate_reserve_balance = true;
            }
            state
        },
        execution_delay,
        CONSENSUS_DELTA,
        chain_config,
        SeqNum(100),
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();

    state_configs
        .into_iter()
        .enumerate()
        .map(|(idx, state_builder)| {
            let validators = state_builder.locked_epoch_validators[0].clone();
            let state_read = state_builder.state_read.clone();
            SimNodeBuilder {
                id: NodeId::new(state_builder.key.pubkey()),
                state_builder,
                router_scheduler: NoSerRouterConfig::new(all_peers.clone()).build(),
                val_set_updater: MockValSetUpdaterNop::new(validators.validators, epoch_length),
                txpool_executor: MockTxPoolExecutor::new(create_block_policy(), state_read.clone())
                    .with_chain_params(&CHAIN_PARAMS)
                    .with_byzantine_config(txpool_byzantine_config(idx)),
                ledger: MockEthLedger::new(state_read.clone()),
                statesync_executor: MockStateSyncExecutor::new(state_read),
                timestamp_period: DEFAULT_TIMESTAMP_PERIOD,
            }
        })
        .collect()
}

pub fn generate_eth_swarm(
    num_nodes: u16,
    existing_accounts: impl IntoIterator<Item = Address>,
    txpool_byzantine_config: impl Fn(usize) -> ByzantineConfig,
) -> Nodes<EthSwarm> {
    let existing_accounts: BTreeMap<Address, AccountState> = existing_accounts
        .into_iter()
        .map(|acc| (acc, AccountState::max_balance()))
        .collect();
    generate_eth_swarm_with_accounts(num_nodes, existing_accounts, txpool_byzantine_config, false)
}

pub fn verify_transactions_in_ledger(
    swarm: &Nodes<EthSwarm>,
    node_ids: Vec<ID<PubKey>>,
    txns: Vec<TxEnvelope>,
) -> bool {
    let txns: HashSet<_> = HashSet::from_iter(txns.iter().map(|t| *t.tx_hash()));
    for node_id in node_ids {
        let state = swarm.states().get(&node_id).unwrap();
        let mut txns_to_see = txns.clone();
        for (round, block) in state.executor.ledger().get_finalized_blocks() {
            for txn in &block.body().execution_body.transactions {
                let txn_hash = txn.tx_hash();
                if txns_to_see.contains(txn_hash) {
                    txns_to_see.remove(txn_hash);
                } else {
                    println!(
                        "Unexpected transaction in block round {}. SeqNum: {}, NodeID: {}, TxnHash: {}, Nonce: {}",
                        round.0, block.get_seq_num().0, node_id, txn_hash, txn.nonce()
                    );
                    return false;
                }
            }
        }

        if !txns_to_see.is_empty() {
            println!(
                "Expected transactions don't exist. NodeID: {}, TxnHashes: {:?}",
                node_id, txns_to_see
            );
            return false;
        }
    }

    true
}

/// [`verify_transactions_in_ledger`] for a swarm driven by the `monad-sim` harness.
pub fn verify_transactions_in_sim(swarm: &SimSwarm<EthSwarm>, txns: Vec<TxEnvelope>) -> bool {
    let txns: HashSet<_> = HashSet::from_iter(txns.iter().map(|t| *t.tx_hash()));
    for node_id in swarm.node_ids() {
        let mut txns_to_see = txns.clone();
        let consistent = swarm.with_node(node_id, |node| {
            for (round, block) in node.ledger().get_finalized_blocks() {
                for txn in &block.body().execution_body.transactions {
                    let txn_hash = txn.tx_hash();
                    if txns_to_see.contains(txn_hash) {
                        txns_to_see.remove(txn_hash);
                    } else {
                        println!(
                            "Unexpected transaction in block round {}. SeqNum: {}, NodeID: {}, TxnHash: {}, Nonce: {}",
                            round.0, block.get_seq_num().0, node_id, txn_hash, txn.nonce()
                        );
                        return false;
                    }
                }
            }
            txns_to_see.is_empty()
        });
        if !consistent {
            return false;
        }
    }

    true
}
