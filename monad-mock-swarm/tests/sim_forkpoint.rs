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

//! Forkpoint restart / recovery, ported onto the `monad-sim` harness (mirrors
//! the legacy `forkpoint.rs`). A node is removed mid-run and restarted from a
//! reconstructed forkpoint (via `SimSwarm::remove_node` / `add_node` /
//! `SimNode::get_forkpoint`), then must catch up to the rest of the swarm.
//!
//! The two `#[ignore]`'d exhaustive sweeps from the legacy
//! (`test_forkpoint_restart_f`, `test_forkpoint_restart_below_all`, run over a
//! rayon pool) are not ported; the focused configs below exercise the same
//! restart paths (simple blocksync, statesync, delayed execution, target reset,
//! epoch boundary).

#[cfg(test)]
mod test {
    use std::{collections::BTreeSet, time::Duration};

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
    use monad_execution_state_read::{InMemoryState, InMemoryStateInner};
    use monad_mock_swarm::{
        sim::{Network, SimNodeBuilder, SimSwarm, DEFAULT_TIMESTAMP_PERIOD},
        swarm::make_state_configs,
        swarm_relation::SwarmRelation,
    };
    use monad_multi_sig::MultiSig;
    use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
    use monad_sim::{time::secs, Time};
    use monad_state::{MonadMessage, VerifiedMonadMessage};
    use monad_transformer::GenericTransformerPipeline;
    use monad_types::{NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
    use monad_updaters::{
        ledger::{MockLedger, MockableLedger},
        statesync::MockStateSyncExecutor,
        txpool::MockTxPoolExecutor,
        val_set::MockValSetUpdaterNop,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    };

    pub struct ForkpointSwarm;
    impl SwarmRelation for ForkpointSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;
        type ExecutionProtocolType = EthExecutionProtocol;
        type ExecutionStateReadType =
            InMemoryState<Self::SignatureType, Self::SignatureCollectionType>;
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
        type Ledger = MockLedger<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >;

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
        vote_pace: Duration::from_millis(0),
    };

    const DELTA: Duration = Duration::from_millis(100);
    const STATE_ROOT_DELAY: SeqNum = SeqNum(4);

    fn forkpoint_restart_one(
        num_nodes: u16,
        blocks_before_failure: SeqNum,
        time_before_new_forkpoint: SeqNum,
        recovery_time: SeqNum,
        epoch_length: SeqNum,
        statesync_threshold: SeqNum,
        statesync_service_window: SeqNum,
        finalization_delay: SeqNum,
    ) {
        assert!(time_before_new_forkpoint <= recovery_time);
        let chain_config =
            MockChainConfig::new_with_epoch_params(&CHAIN_PARAMS, epoch_length, Round(50));
        let create_block_policy = || EthBlockPolicy::new(GENESIS_SEQ_NUM, STATE_ROOT_DELAY.0);

        // state configs are deterministic per call (keys aren't Clone), so we
        // regenerate them where needed and match nodes by pubkey.
        let configs = || {
            make_state_configs::<ForkpointSwarm>(
                num_nodes,
                ValidatorSetFactory::default,
                SimpleRoundRobin::default,
                EthBlockValidator::default,
                create_block_policy,
                || InMemoryStateInner::genesis(STATE_ROOT_DELAY),
                STATE_ROOT_DELAY,
                DELTA,
                chain_config,
                statesync_threshold,
            )
        };

        let restart_ids: Vec<NodeId<_>> = configs()
            .iter()
            .map(|c| NodeId::new(c.key.pubkey()))
            .collect();

        // enumerate the restarting node to cover the different leader cases
        for restart_node_id in restart_ids {
            let validators = configs()[0].locked_epoch_validators[0].validators.clone();
            let mut restart_builder = configs()
                .into_iter()
                .find(|s| s.key.pubkey() == restart_node_id.pubkey())
                .expect("restart node exists");

            let state_configs = configs();
            let all_peers: BTreeSet<_> = state_configs
                .iter()
                .map(|c| NodeId::new(c.key.pubkey()))
                .collect();
            let builders: Vec<SimNodeBuilder<ForkpointSwarm>> = state_configs
                .into_iter()
                .map(|state_builder| {
                    let state_read = state_builder.state_read.clone();
                    SimNodeBuilder {
                        id: NodeId::new(state_builder.key.pubkey()),
                        state_builder,
                        router_scheduler: NoSerRouterConfig::new(all_peers.clone()).build(),
                        val_set_updater: MockValSetUpdaterNop::new(
                            validators.clone(),
                            epoch_length,
                        ),
                        txpool_executor: MockTxPoolExecutor::new(
                            create_block_policy(),
                            state_read.clone(),
                        )
                        .with_chain_params(&CHAIN_PARAMS),
                        ledger: MockLedger::new(state_read.clone())
                            .with_finalization_delay(finalization_delay),
                        statesync_executor: MockStateSyncExecutor::new(state_read)
                            .with_max_service_window(statesync_service_window),
                        timestamp_period: DEFAULT_TIMESTAMP_PERIOD,
                    }
                })
                .collect();

            let mut swarm = SimSwarm::from_builders(0, builders, |_| Network::reliable(DELTA));
            let deadline = Time(0) + secs(6000);

            assert!(swarm.run_until_blocks(blocks_before_failure.0 as usize, deadline));

            // eject the failing node; read its forkpoint / ledger / state-backend
            let failed_node = swarm.remove_node(restart_node_id);

            let forkpoint = if time_before_new_forkpoint == SeqNum(0) {
                failed_node.get_forkpoint()
            } else {
                let forkpoint_block = blocks_before_failure + time_before_new_forkpoint;
                assert!(swarm.run_until_any_blocks(forkpoint_block.0 as usize, deadline));
                let any = swarm.node_ids()[0];
                swarm.with_node(any, |n| n.get_forkpoint())
            };

            let recover_block = blocks_before_failure + recovery_time;
            assert!(swarm.run_until_any_blocks(recover_block.0 as usize, deadline));

            // reconstruct the restart node from the forkpoint and re-add it
            restart_builder.locked_epoch_validators = forkpoint
                .validator_sets
                .iter()
                .map(|locked_epoch| ValidatorSetDataWithEpoch {
                    epoch: locked_epoch.epoch,
                    validators: validators.clone(),
                })
                .collect();
            restart_builder.forkpoint = forkpoint.clone();
            restart_builder.state_read = failed_node.state().state_read().clone();
            let backend = restart_builder.state_read.clone();
            swarm.add_node(SimNodeBuilder {
                id: restart_node_id,
                state_builder: restart_builder,
                router_scheduler: NoSerRouterConfig::new(all_peers.clone()).build(),
                val_set_updater: MockValSetUpdaterNop::new(validators.clone(), epoch_length),
                txpool_executor: MockTxPoolExecutor::new(create_block_policy(), backend.clone()),
                ledger: MockLedger::new(backend.clone())
                    .with_finalization_delay(finalization_delay)
                    .with_blocks(failed_node.ledger()),
                statesync_executor: MockStateSyncExecutor::new(backend),
                timestamp_period: DEFAULT_TIMESTAMP_PERIOD,
            });

            // run until 3 more epochs are produced so epoch switching exercises
            let terminate_block = recover_block.0 as usize + epoch_length.0 as usize * 3;
            assert!(swarm.run_until_any_blocks(terminate_block, deadline));

            // the restarted node caught up and is voting
            let metrics_voting = swarm.with_node(restart_node_id, |n| {
                n.metrics().consensus_events.created_vote.get() > 0
            });
            let caught_up = swarm.with_node(restart_node_id, |n| {
                n.ledger()
                    .get_finalized_blocks()
                    .values()
                    .last()
                    .map(|fb| fb.header().seq_num >= SeqNum(terminate_block as u64 - 2))
                    .unwrap_or(false)
            });
            assert!(
                caught_up && metrics_voting,
                "restart {restart_node_id}: caught_up={caught_up} voting={metrics_voting}"
            );
        }
    }

    #[test]
    fn test_quick_restart() {
        forkpoint_restart_one(
            20,
            SeqNum(10),
            SeqNum(0),
            SeqNum(0),
            SeqNum(3),
            SeqNum(5),
            SeqNum::MAX,
            SeqNum(0),
        );
    }

    #[test]
    fn test_forkpoint_restart_f_simple_blocksync() {
        forkpoint_restart_one(
            4,
            SeqNum(10),
            SeqNum(0),
            SeqNum(50),
            SeqNum(200),
            SeqNum(100),
            SeqNum::MAX,
            SeqNum(0),
        );
    }

    #[test]
    fn test_forkpoint_restart_f_delayed_execution_no_statesync() {
        forkpoint_restart_one(
            4,
            SeqNum(10),
            SeqNum(0),
            SeqNum(50),
            SeqNum(200),
            SeqNum(100),
            SeqNum::MAX,
            SeqNum(8),
        );
    }

    #[test]
    fn test_forkpoint_restart_f_simple_statesync() {
        // restart from a fresh forkpoint 150 blocks in (statesync_threshold*3/2)
        forkpoint_restart_one(
            4,
            SeqNum(10),
            SeqNum(150),
            SeqNum(150),
            SeqNum(200),
            SeqNum(100),
            SeqNum::MAX,
            SeqNum(0),
        );
    }

    #[test]
    fn test_forkpoint_restart_f_target_reset_statesync() {
        // service window (50) < recovery (150): the statesync target must reset
        forkpoint_restart_one(
            4,
            SeqNum(10),
            SeqNum(10),
            SeqNum(150),
            SeqNum(200),
            SeqNum(100),
            SeqNum(50),
            SeqNum(0),
        );
    }

    #[test]
    fn test_forkpoint_restart_f_epoch_boundary_statesync() {
        // restart spans an epoch boundary (blocks_before_failure 275, epoch 200)
        forkpoint_restart_one(
            4,
            SeqNum(275),
            SeqNum(150),
            SeqNum(150),
            SeqNum(200),
            SeqNum(100),
            SeqNum::MAX,
            SeqNum(0),
        );
    }
}
