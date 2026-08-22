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

//! Two-node consensus over a BLS signature collection, ported onto the
//! `monad-sim` harness (mirrors the legacy `two_nodes_bls.rs`, which it does
//! not replace).
//!
//! NOTE: the legacy test additionally round-trips every emitted `MonadEvent`
//! through RLP and serde on each step. That is serialization coverage,
//! independent of the simulation engine; the new harness drives events
//! internally and has no per-event hook. The round-trip coverage stays in the
//! (still-present) legacy test; it should be extracted into a standalone
//! serialization test before the legacy engine is deleted.

use std::{collections::BTreeSet, time::Duration};

use monad_bls::BlsSignatureCollection;
use monad_chain_config::{
    revision::{ChainParams, MockChainRevision},
    MockChainConfig,
};
use monad_consensus_types::{
    block::{MockExecutionProtocol, PassthruBlockPolicy},
    block_validator::MockValidator,
};
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_execution_state_read::{InMemoryState, InMemoryStateInner};
use monad_mock_swarm::{
    sim::{Network, SimNodeBuilder, SimSwarm, DEFAULT_TIMESTAMP_PERIOD},
    sim_verify::SimVerifier,
    swarm::make_state_configs,
    swarm_relation::SwarmRelation,
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_secp::SecpSignature;
use monad_sim::{time::secs, Time};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_transformer::GenericTransformerPipeline;
use monad_types::{NodeId, SeqNum};
use monad_updaters::{
    ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

struct BLSSwarm;
impl SwarmRelation for BLSSwarm {
    type SignatureType = SecpSignature;
    type SignatureCollectionType =
        BlsSignatureCollection<CertificateSignaturePubKey<Self::SignatureType>>;
    type ExecutionProtocolType = MockExecutionProtocol;
    type ExecutionStateReadType = InMemoryState<Self::SignatureType, Self::SignatureCollectionType>;
    type BlockPolicyType = PassthruBlockPolicy;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;

    type TransportMessage = VerifiedMonadMessage<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;

    type BlockValidator = MockValidator;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type Ledger =
        MockLedger<Self::SignatureType, Self::SignatureCollectionType, Self::ExecutionProtocolType>;

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

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(5),
};

fn bls_sim(num_nodes: u16, delta: Duration) -> SimSwarm<BLSSwarm> {
    let state_configs = make_state_configs::<BLSSwarm>(
        num_nodes,
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(SeqNum(4)),
        SeqNum(4),
        delta,
        MockChainConfig::new(&CHAIN_PARAMS),
        SeqNum(100),
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|config| NodeId::new(config.key.pubkey()))
        .collect();

    let builders: Vec<SimNodeBuilder<BLSSwarm>> = state_configs
        .into_iter()
        .map(|config| {
            let state_read = config.state_read.clone();
            let validators = config.locked_epoch_validators[0].clone();
            SimNodeBuilder {
                id: NodeId::new(config.key.pubkey()),
                state_builder: config,
                router_scheduler: NoSerRouterConfig::new(all_peers.clone()).build(),
                val_set_updater: MockValSetUpdaterNop::new(validators.validators, SeqNum(2000)),
                txpool_executor: MockTxPoolExecutor::default().with_chain_params(&CHAIN_PARAMS),
                ledger: MockLedger::new(state_read.clone()),
                statesync_executor: MockStateSyncExecutor::new(state_read),
                timestamp_period: DEFAULT_TIMESTAMP_PERIOD,
            }
        })
        .collect();

    SimSwarm::from_builders(0, builders, |_| Network::reliable(delta))
}

#[test]
fn two_nodes_bls() {
    let delta = Duration::from_millis(20);
    let mut swarm = bls_sim(2, delta);

    assert!(
        swarm.run_until_blocks(100, Time(0) + secs(60)),
        "did not reach 100 blocks"
    );

    let ids = swarm.node_ids();
    let mut verifier = SimVerifier::new();
    verifier
        .tick_exact(Time(0) + Duration::from_millis(4_060))
        .ledger_min_len(98)
        .metrics_happy_path(&ids, &swarm);
    verifier.assert(&swarm);
    swarm.assert_agreement();
}
