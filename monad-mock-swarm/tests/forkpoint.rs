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

use std::{collections::BTreeSet, time::Duration};

use itertools::Itertools;
use monad_chain_config::{
    revision::{ChainParams, MockChainRevision},
    MockChainConfig,
};
use monad_consensus_types::validator_data::ValidatorSetDataWithEpoch;
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
    NopSignature,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_types::EthExecutionProtocol;
use monad_mock_swarm::{
    mock::TimestamperConfig, mock_swarm::SwarmBuilder, node::NodeBuilder,
    swarm_relation::SwarmRelation, terminator::UntilTerminator,
};
use monad_multi_sig::MultiSig;
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_state_backend::{InMemoryState, InMemoryStateInner};
use monad_testutil::swarm::make_state_configs;
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer, ID};
use monad_types::{Balance, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
use monad_updaters::{
    ledger::{MockLedger, MockableLedger},
    statesync::MockStateSyncExecutor,
    txpool::MockTxPoolExecutor,
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use rayon::prelude::*;

pub struct ForkpointSwarm;
impl SwarmRelation for ForkpointSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type ExecutionProtocolType = EthExecutionProtocol;
    type StateBackendType = InMemoryState<Self::SignatureType, Self::SignatureCollectionType>;
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
        Self::StateBackendType,
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
    vote_pace: Duration::from_millis(0),

    validate_system_txs: true,
    eip_7702: true,
};

#[test]
fn test_forkpoint_restart_f_simple_blocksync() {
    let epoch_length = SeqNum(200);
    let statesync_threshold = SeqNum(100);
    let statesync_service_window = SeqNum::MAX;

    let blocks_before_failure = SeqNum(10);
    let time_before_new_forkpoint = SeqNum(0);
    let recovery_time = SeqNum(statesync_threshold.0 / 2);
    let finalization_delay = SeqNum(0);
    forkpoint_restart_f(
        blocks_before_failure,
        time_before_new_forkpoint,
        recovery_time,
        epoch_length,
        statesync_threshold,
        statesync_service_window,
        finalization_delay,
    );
}

// execution is delayed longer than state_root_delay
// ensure that we don't statesync
#[test]
fn test_forkpoint_restart_f_delayed_execution_no_statesync() {
    let epoch_length = SeqNum(200);
    let statesync_threshold = SeqNum(100);
    let statesync_service_window = SeqNum::MAX;

    let blocks_before_failure = SeqNum(10);
    let time_before_new_forkpoint = SeqNum(0);
    let recovery_time = SeqNum(statesync_threshold.0 / 2);
    let finalization_delay = SeqNum(8); // state_root_delay is 4
    forkpoint_restart_f(
        blocks_before_failure,
        time_before_new_forkpoint,
        recovery_time,
        epoch_length,
        statesync_threshold,
        statesync_service_window,
        finalization_delay,
    );
}

#[test]
fn test_forkpoint_restart_f_simple_statesync() {
    let epoch_length = SeqNum(200);
    let statesync_threshold = SeqNum(100);
    let statesync_service_window = SeqNum::MAX;

    let blocks_before_failure = SeqNum(10);
    let time_before_new_forkpoint = SeqNum(statesync_threshold.0 * 3 / 2);
    let recovery_time = time_before_new_forkpoint;
    let finalization_delay = SeqNum(0);
    forkpoint_restart_f(
        blocks_before_failure,
        time_before_new_forkpoint,
        recovery_time,
        epoch_length,
        statesync_threshold,
        statesync_service_window,
        finalization_delay,
    );
}

// statesync_service_window is less than recovery_time
// the restarted forkpoint is more than recovery_time away from tip
//
// so this test only passes if the statesync target refreshing works in
// monad-state/src/statesync.rs
#[test]
fn test_forkpoint_restart_f_target_reset_statesync() {
    let epoch_length = SeqNum(200);
    let statesync_threshold = SeqNum(100);
    let statesync_service_window = SeqNum(50);

    let blocks_before_failure = SeqNum(10);
    let time_before_new_forkpoint = SeqNum(10);
    let recovery_time = SeqNum(statesync_threshold.0 * 3 / 2);
    let finalization_delay = SeqNum(0);
    forkpoint_restart_f(
        blocks_before_failure,
        time_before_new_forkpoint,
        recovery_time,
        epoch_length,
        statesync_threshold,
        statesync_service_window,
        finalization_delay,
    );
}

#[test]
fn test_forkpoint_restart_f_epoch_boundary_statesync() {
    let epoch_length = SeqNum(200);
    let statesync_threshold = SeqNum(100);
    let statesync_service_window = SeqNum::MAX;

    let blocks_before_failure = SeqNum(275);
    let time_before_new_forkpoint = SeqNum(statesync_threshold.0 * 3 / 2);
    let recovery_time = time_before_new_forkpoint;
    let finalization_delay = SeqNum(0);
    forkpoint_restart_f(
        blocks_before_failure,
        time_before_new_forkpoint,
        recovery_time,
        epoch_length,
        statesync_threshold,
        statesync_service_window,
        finalization_delay,
    );
}

// This test takes too long to run. Ignore for PR CI runs
#[ignore]
#[test]
fn test_forkpoint_restart_f() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(32)
        .build()
        .unwrap();
    let epoch_length = SeqNum(200);
    let statesync_threshold = SeqNum(100);
    let statesync_service_window = SeqNum::MAX;
    let time_before_new_forkpoint = SeqNum(0);
    let finalization_delay = SeqNum(0);
    // Epoch 1 and 2 are populated on genesis
    // This covers the case with generating validator set for epoch 3
    for before in 10..epoch_length.0 * 3 {
        let blocks_before = SeqNum(before);
        let recovery_range: Vec<u64> = (0..(statesync_threshold.0)).collect();
        pool.install(|| {
            recovery_range.par_iter().for_each(|&recovery| {
                let recovery_time = SeqNum(recovery);
                forkpoint_restart_f(
                    blocks_before,
                    time_before_new_forkpoint,
                    recovery_time,
                    epoch_length,
                    statesync_threshold,
                    statesync_service_window,
                    finalization_delay,
                );
            })
        });
    }
}

/// A network of 4 nodes produces `block_before_failure` blocks, before 1 out of
/// 4 node restarts. During the restart, the remaining network produces
/// `recovery_time` blocks. Assert that the node can catch up using local
/// forkpoint and blocksync
fn forkpoint_restart_f(
    blocks_before_failure: SeqNum,
    time_before_new_forkpoint: SeqNum,
    recovery_time: SeqNum,
    epoch_length: SeqNum,
    statesync_threshold: SeqNum,
    statesync_service_window: SeqNum,
    finalization_delay: SeqNum,
) {
    assert!(time_before_new_forkpoint <= recovery_time);

    let delta = Duration::from_millis(100);
    let state_root_delay = SeqNum(4);
    let chain_config =
        MockChainConfig::new_with_epoch_params(&CHAIN_PARAMS, epoch_length, Round(50));
    let state_configs = make_state_configs::<ForkpointSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        EthBlockValidator::default,
        || EthBlockPolicy::new(GENESIS_SEQ_NUM, state_root_delay.0),
        || InMemoryStateInner::genesis(Balance::MAX, state_root_delay),
        state_root_delay,
        delta,               // delta
        chain_config,        // chain config
        statesync_threshold, // state_sync_threshold
    );

    let create_block_policy = || EthBlockPolicy::new(GENESIS_SEQ_NUM, state_root_delay.0);

    // Enumerate different restarting node id to cover all the leader cases
    for (_i, restart_pubkey) in state_configs
        .iter()
        .enumerate()
        .map(|(i, c)| (i, c.key.pubkey()))
    {
        let restart_node_id = NodeId::new(restart_pubkey);

        // regenerate state config every iteration as they do not implement
        // Clone, due to KeyPair not implementing Clone
        let state_configs = make_state_configs::<ForkpointSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            EthBlockValidator::default,
            create_block_policy,
            || InMemoryStateInner::genesis(Balance::MAX, state_root_delay),
            state_root_delay,    // execution_delay
            delta,               // delta
            chain_config,        // chain config
            statesync_threshold, // state_sync_threshold
        );
        let validators = state_configs[0].locked_epoch_validators[0]
            .validators
            .clone();
        let state_configs_dup = make_state_configs::<ForkpointSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            EthBlockValidator::default,
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, state_root_delay.0),
            || InMemoryStateInner::genesis(Balance::MAX, state_root_delay),
            state_root_delay,    // execution_delay
            delta,               // delta
            chain_config,        // chain config
            statesync_threshold, // state_sync_threshold
        );

        let mut restart_builder = state_configs_dup
            .into_iter()
            .find(|s| s.key.pubkey() == restart_node_id.pubkey())
            .expect("Restart node exists");

        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();
        let swarm_config = SwarmBuilder::<ForkpointSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let state_backend = state_builder.state_backend.clone();
                    NodeBuilder::<ForkpointSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockValSetUpdaterNop::new(validators.clone(), epoch_length),
                        MockTxPoolExecutor::new(create_block_policy(), state_backend.clone())
                            .with_chain_params(&CHAIN_PARAMS),
                        MockLedger::new(state_backend.clone())
                            .with_finalization_delay(finalization_delay),
                        MockStateSyncExecutor::new(
                            state_backend,
                            validators
                                .0
                                .iter()
                                .map(|validator| validator.node_id)
                                .collect(),
                        )
                        .with_max_service_window(statesync_service_window),
                        vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                        vec![],
                        TimestamperConfig::default(),
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut swarm = swarm_config.build().can_fail_deliver();
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(blocks_before_failure.0 as usize))
            .is_some()
        {}

        // Process the commands from the latest event
        let tick = swarm.peek_tick().expect("event queue non-empty");
        while swarm
            .step_until(&mut UntilTerminator::new().until_tick(tick + Duration::from_nanos(1)))
            .is_some()
        {}

        // Remove the failing node from active nodes, run the remaining for recovery_time rounds
        let failed_node = swarm
            .remove_state(&ID::new(restart_node_id))
            .expect("node exists");

        let forkpoint = if time_before_new_forkpoint == SeqNum(0) {
            // Restart node from old forkpoint
            failed_node.get_forkpoint()
        } else {
            let forkpoint_block = blocks_before_failure + time_before_new_forkpoint;
            while swarm
                .step_until(&mut UntilTerminator::new().until_block(forkpoint_block.0 as usize))
                .is_some()
            {}
            // Restart node from fresh forkpoint
            swarm
                .states()
                .first_key_value()
                .unwrap()
                .1
                .get_forkpoint()
                .clone()
        };

        let recover_block = blocks_before_failure + recovery_time;
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(recover_block.0 as usize))
            .is_some()
        {}

        let network_current_epoch = swarm
            .states()
            .iter()
            .map(|(_id, node)| {
                node.state
                    .epoch_manager()
                    .get_epoch(
                        node.state
                            .consensus()
                            .expect("consensus is live")
                            .get_current_round(),
                    )
                    .expect("epoch exists")
            })
            .max()
            .expect("Network is non-empty");
        let failed_node_high_epoch = forkpoint
            .validator_sets
            .iter()
            .map(|vset| (vset.round, vset.epoch))
            .max_by(|x, y| x.0.cmp(&y.0))
            .map(|t| t.1)
            .expect("current epoch must be scheduled");
        restart_builder.locked_epoch_validators = forkpoint
            .validator_sets
            .iter()
            .map(|locked_epoch| ValidatorSetDataWithEpoch {
                epoch: locked_epoch.epoch,
                validators: validators.clone(),
            })
            .collect();
        restart_builder.forkpoint = forkpoint.clone();
        restart_builder.state_backend = failed_node.state.state_backend().clone();
        let restart_builder_state_backend = restart_builder.state_backend.clone();
        swarm.add_state(NodeBuilder::new(
            ID::new(restart_node_id),
            restart_builder,
            NoSerRouterConfig::new(all_peers.clone()).build(),
            MockValSetUpdaterNop::new(validators.clone(), epoch_length),
            MockTxPoolExecutor::new(create_block_policy(), restart_builder_state_backend.clone()),
            MockLedger::new(restart_builder_state_backend.clone())
                .with_finalization_delay(finalization_delay)
                .with_blocks(failed_node.executor.ledger()),
            MockStateSyncExecutor::new(
                restart_builder_state_backend,
                validators
                    .0
                    .iter()
                    .map(|validator| validator.node_id)
                    .collect(),
            ),
            vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
            vec![],
            TimestamperConfig::default(),
            42.try_into().unwrap(),
        ));

        // Run all nodes until 3 * epoch_length blocks are produced, making sure
        // epoch switching can happen normally
        let terminate_block = recover_block.0 as usize + epoch_length.0 as usize * 3;
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(terminate_block))
            .is_some()
        {}

        // Assert the restarting node is caught up with others ledger
        //
        // If it's failing because of triggering statesync, and the recovery
        // time is close to 100 blocks, it's fine. The exact number of blocks to
        // block sync depends on the leader schedule and when the node fails. So
        // we can be a little lenient on that
        let restarted_node = swarm
            .states()
            .get(&ID::new(restart_node_id))
            .expect("restarting node in swarm");

        let state_sync_triggered = restarted_node
            .state
            .metrics()
            .consensus_events
            .trigger_state_sync
            > 0;
        let state_sync_reset_target = restarted_node
            .state
            .metrics()
            .consensus_events
            .trigger_state_sync
            > 1;
        let should_reset_statesync_target =
            (recovery_time - time_before_new_forkpoint) >= statesync_service_window;
        let invalid_epoch_error = restarted_node
            .state
            .metrics()
            .validation_errors
            .invalid_epoch
            > 0;
        let close_to_threshold =
            SeqNum(statesync_threshold.0.saturating_sub(recovery_time.0)) < SeqNum(5);
        // epoch_cross_over means that the restarting node doesn't have the
        // epoch it's joining scheduled. It can't validate any message in the
        // new epoch and must go through out-of-band validator set syncing
        let epoch_cross_over = network_current_epoch > failed_node_high_epoch;
        let maybe_last_block = restarted_node
            .executor
            .ledger()
            .get_finalized_blocks()
            .values()
            .last();
        // SeqNum(terminate_block as u64 - 2): if all nodes are in sync, the
        // shortest ledger is at most 2 blocks behind the longest
        let restarted_node_caught_up = maybe_last_block
            .map(|fb| fb.header().seq_num >= SeqNum(terminate_block as u64 - 2))
            .unwrap_or(false);

        let test_result = restarted_node_caught_up
            && (!state_sync_triggered || close_to_threshold)
            && (!state_sync_reset_target || should_reset_statesync_target)
            && (!invalid_epoch_error || epoch_cross_over);

        assert!(
            test_result,
            "\
block_before_failure={:?},
recovery_time={:?},
epoch_length={:?},
restarted_node_caught_up={:?},
state_sync_triggered={:?},
close_to_threshold={:?},
invalid_epoch_error={:?},
epoch_cross_over={:?},
forkpoint={:?},
restarted_node_metrics={:#?}",
            blocks_before_failure,
            recovery_time,
            epoch_length,
            restarted_node_caught_up,
            state_sync_triggered,
            close_to_threshold,
            invalid_epoch_error,
            epoch_cross_over,
            forkpoint,
            restarted_node.state.metrics()
        );
    }
}

// This test takes too long to run. Ignore for PR CI runs
#[ignore]
#[test]
fn test_forkpoint_restart_below_all() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(32)
        .build()
        .unwrap();
    let epoch_length = SeqNum(200);
    let statesync_threshold = SeqNum(100);
    // Epoch 1 and 2 are populated on genesis
    // This covers the case with generating validator set for epoch 3
    let before_range: Vec<u64> = (10..epoch_length.0 * 3).collect();
    pool.install(|| {
        before_range.par_iter().for_each(|&before| {
            let blocks_before = SeqNum(before);
            forkpoint_restart_below_all(blocks_before, epoch_length, statesync_threshold);
        })
    });
}

/// A network of 4 nodes produces `blocks_before_failure` blocks, before 2 out
/// of 4 nodes restart. The remaining network is stuck on the current round.
/// After the node restarts, they requests blocks to complete their block tree,
/// timeout the current round, and return to normal
fn forkpoint_restart_below_all(
    blocks_before_failure: SeqNum,
    epoch_length: SeqNum,
    statesync_threshold: SeqNum,
) {
    let num_nodes = 4;
    let delta = Duration::from_millis(100);
    let state_root_delay = SeqNum(4);
    let chain_config =
        MockChainConfig::new_with_epoch_params(&CHAIN_PARAMS, epoch_length, Round(50));
    let state_configs = make_state_configs::<ForkpointSwarm>(
        num_nodes,
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        EthBlockValidator::default,
        || EthBlockPolicy::new(GENESIS_SEQ_NUM, state_root_delay.0),
        || InMemoryStateInner::genesis(Balance::MAX, state_root_delay),
        state_root_delay,    // execution_delay
        delta,               // delta
        chain_config,        // chain config
        statesync_threshold, // state_sync_threshold
    );
    let validators = state_configs[0].locked_epoch_validators[0]
        .validators
        .clone();

    let pubkey_iter = state_configs
        .iter()
        .enumerate()
        .map(|(i, c)| (i, c.key.pubkey()));
    let mut comb_iters = Vec::new();
    // restart any number of nodes in the range [f+1, 3f+1)
    for c in (num_nodes - 1) / 3 + 1..num_nodes {
        comb_iters.push(pubkey_iter.clone().combinations(c.into()));
    }

    let c_iter = comb_iters.into_iter().flatten();

    let create_block_policy = || EthBlockPolicy::new(GENESIS_SEQ_NUM, state_root_delay.0);

    for restart_pubkeys in c_iter {
        let restart_node_ids = restart_pubkeys
            .iter()
            .map(|(_, pubkey)| NodeId::new(*pubkey))
            .collect::<Vec<_>>();

        // regenerate state config every iteration as they do not implement
        // Clone, due to KeyPair not implementing Clone
        let state_configs = make_state_configs::<ForkpointSwarm>(
            num_nodes,
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            EthBlockValidator::default,
            create_block_policy,
            || InMemoryStateInner::genesis(Balance::MAX, state_root_delay),
            state_root_delay,    // execution_delay
            delta,               // delta
            chain_config,        // chain config
            statesync_threshold, // state_sync_threshold
        );
        let mut state_configs_dup = make_state_configs::<ForkpointSwarm>(
            num_nodes,
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            EthBlockValidator::default,
            create_block_policy,
            || InMemoryStateInner::genesis(Balance::MAX, state_root_delay),
            state_root_delay,    // execution_delay
            delta,               // delta
            chain_config,        // chain config
            statesync_threshold, // state_sync_threshold
        );

        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();
        let swarm_config = SwarmBuilder::<ForkpointSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let state_backend = state_builder.state_backend.clone();
                    NodeBuilder::<ForkpointSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockValSetUpdaterNop::new(validators.clone(), epoch_length),
                        MockTxPoolExecutor::new(create_block_policy(), state_backend.clone()),
                        MockLedger::new(state_backend.clone()),
                        MockStateSyncExecutor::new(
                            state_backend,
                            validators
                                .0
                                .iter()
                                .map(|validator| validator.node_id)
                                .collect(),
                        ),
                        vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                        vec![],
                        TimestamperConfig::default(),
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut swarm = swarm_config.build().can_fail_deliver();
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(blocks_before_failure.0 as usize))
            .is_some()
        {}

        // Process the commands of the latest event
        let tick = swarm.peek_tick().expect("event queue non-empty");
        while swarm
            .step_until(&mut UntilTerminator::new().until_tick(tick + Duration::from_nanos(1)))
            .is_some()
        {}

        let network_round = swarm
            .states()
            .iter()
            .map(|(_id, node)| {
                node.state
                    .consensus()
                    .expect("consensus is live")
                    .get_current_round()
            })
            .max()
            .expect("swarm non-empty");
        let network_tick = swarm.peek_tick().expect("event queue non-empty");

        // Remove the failing node from active nodes, run the remaining for recovery_time rounds
        let mut failed_nodes = Vec::new();
        for id in restart_node_ids {
            let failed_node = swarm.remove_state(&ID::new(id)).expect("node exists");
            failed_nodes.push(failed_node);
        }

        let terminate_tick = network_tick + delta * 100;
        // enough tick to drain the event queue and timeout
        while swarm
            .step_until(
                &mut UntilTerminator::new()
                    .until_round(network_round + Round(1))
                    .until_tick(terminate_tick),
            )
            .is_some()
        {}

        let network_round_after_failure = swarm
            .states()
            .iter()
            .map(|(_id, node)| {
                node.state
                    .consensus()
                    .expect("consensus is live")
                    .get_current_round()
            })
            .max()
            .expect("swarm non-empty");

        // + Round(1) to account for pending event(proposal) bumping the network
        //   round
        assert!(
            network_round_after_failure <= network_round + Round(1),
            "forkpoint restart config before {:?} epoch length {:?}",
            blocks_before_failure,
            epoch_length
        );

        // Restart nodes from forkpoint and join network
        for node in failed_nodes {
            let node_id = NodeId::new(node.state.pubkey());

            let builder_pos = state_configs_dup
                .iter()
                .position(|s| s.key.pubkey() == node.state.pubkey())
                .expect("node must exist");

            let mut builder = state_configs_dup.swap_remove(builder_pos);
            let forkpoint = node.get_forkpoint();

            builder.locked_epoch_validators = forkpoint
                .validator_sets
                .iter()
                .map(|locked_epoch| ValidatorSetDataWithEpoch {
                    epoch: locked_epoch.epoch,
                    validators: validators.clone(),
                })
                .collect();
            builder.forkpoint = forkpoint;
            let state_backend = builder.state_backend.clone();
            swarm.add_state(NodeBuilder::new(
                ID::new(node_id),
                builder,
                NoSerRouterConfig::new(all_peers.clone()).build(),
                MockValSetUpdaterNop::new(validators.clone(), epoch_length),
                MockTxPoolExecutor::new(create_block_policy(), state_backend.clone()),
                MockLedger::new(state_backend.clone()),
                MockStateSyncExecutor::new(
                    state_backend,
                    validators
                        .0
                        .iter()
                        .map(|validator| validator.node_id)
                        .collect(),
                ),
                vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                vec![],
                TimestamperConfig::default(),
                42.try_into().unwrap(),
            ));
        }

        // Run all nodes until 3 * epoch_length blocks are produced, making sure
        // epoch switching can happen normally

        // after recovery, in happy path, the network is committing a block
        // every 2 delta, hence `delta * 2 * epoch_length.0 * 3`

        // `delta * 100` is time for recovery
        let terminate_block = blocks_before_failure.0 as usize + epoch_length.0 as usize * 3;
        while swarm
            .step_until(
                &mut UntilTerminator::new()
                    .until_block(terminate_block)
                    .until_tick(
                        terminate_tick + delta * 2 * epoch_length.0 as u32 * 3 + delta * 100,
                    ),
            )
            .is_some()
        {}
        let min_ledger_len = swarm
            .states()
            .iter()
            .map(|(_id, node)| {
                node.executor
                    .ledger()
                    .get_finalized_blocks()
                    .values()
                    .last()
                    .map(|block| block.get_seq_num().0)
                    .unwrap_or_default()
            })
            .min()
            .expect("network non-empty");

        let max_ledger_len = swarm
            .states()
            .iter()
            .map(|(_id, node)| {
                node.executor
                    .ledger()
                    .get_finalized_blocks()
                    .values()
                    .last()
                    .map(|block| block.get_seq_num().0)
                    .unwrap_or_default()
            })
            .max()
            .expect("network non-empty");

        // Assert the network is making progress
        // TODO change this to max_ledger_len == terminate_block once until_block is by seq_num
        let network_progress_after_recovery = max_ledger_len >= terminate_block as u64;

        let network_in_sync = min_ledger_len + 2 >= max_ledger_len;

        assert!(
            network_progress_after_recovery && network_in_sync,
            "\
blocks_before_failure={:?},
epoch_length={:?},
network_progress_after_recovery={},
network_in_sync={},
max_ledger_len={},
terminate_block={}",
            blocks_before_failure,
            epoch_length,
            network_progress_after_recovery,
            network_in_sync,
            max_ledger_len,
            terminate_block,
        );
    }
}
