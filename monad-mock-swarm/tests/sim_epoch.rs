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

//! Epoch / validator-set-change coverage, ported onto the `monad-sim` harness
//! (mirrors the legacy `epoch.rs`, which it does not replace). All four legacy
//! tests are ported: `validator_switching`, `schedule_and_advance_epoch`,
//! `verify_correct_leaders_in_epoch`, and `schedule_epoch_after_blocksync`.
//!
//! `schedule_epoch_after_blocksync` uses `SimSwarm::set_network` (mid-run
//! reconfiguration) to blackout a node for exactly the few blocks around the
//! epoch boundary — matching the legacy's event-triggered blackout, so the node
//! recovers via blocksync rather than statesync (and avoids a production
//! `todo!` reached only when a node falls many blocks behind across a boundary).

mod sim_common;

#[cfg(test)]
mod test {
    use std::{collections::BTreeSet, time::Duration};

    use monad_chain_config::{
        revision::{ChainParams, MockChainRevision},
        MockChainConfig,
    };
    use monad_consensus_types::{
        block::{MockExecutionProtocol, PassthruBlockPolicy},
        block_validator::MockValidator,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopSignature,
    };
    use monad_execution_state_read::{InMemoryState, InMemoryStateInner};
    use monad_mock_swarm::{
        sim::{Network, SimNodeBuilder, SimSwarm, DEFAULT_TIMESTAMP_PERIOD},
        sim_metric,
        sim_verify::SimVerifier,
        swarm::make_state_configs,
        swarm_relation::SwarmRelation,
    };
    use monad_multi_sig::MultiSig;
    use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
    use monad_sim::{time::secs, Time};
    use monad_state::{MonadMessage, Role, VerifiedMonadMessage};
    use monad_transformer::GenericTransformerPipeline;
    use monad_types::{Epoch, NodeId, Round, SeqNum};
    use monad_updaters::{
        ledger::{MockLedger, MockableLedger},
        statesync::MockStateSyncExecutor,
        txpool::MockTxPoolExecutor,
        val_set::MockValSetUpdaterSwap,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    };
    use test_case::test_case;

    use crate::sim_common::NoSerConfig;

    type Pk<S> = CertificateSignaturePubKey<<S as SwarmRelation>::SignatureType>;

    static EPOCH_LENGTH: SeqNum = SeqNum(1000);

    struct ValidatorSwapSwarm;
    impl SwarmRelation for ValidatorSwapSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;
        type ExecutionProtocolType = MockExecutionProtocol;
        type ExecutionStateReadType =
            InMemoryState<Self::SignatureType, Self::SignatureCollectionType>;
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

        type ValSetUpdater = MockValSetUpdaterSwap<
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

    /// Build a 4-node `ValidatorSwapSwarm` (consensus `delta`, network
    /// `latency`) and return it alongside the genesis validator order (which the
    /// swap updater rotates through across epochs).
    fn swap_swarm(
        epoch_length: SeqNum,
        epoch_start_delay: Round,
        delta: Duration,
        latency: Duration,
    ) -> (
        SimSwarm<ValidatorSwapSwarm>,
        Vec<NodeId<Pk<ValidatorSwapSwarm>>>,
    ) {
        let state_configs = make_state_configs::<ValidatorSwapSwarm>(
            4,
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            || MockValidator,
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(SeqNum(4)),
            SeqNum(4),
            delta,
            MockChainConfig::new_with_epoch_params(&CHAIN_PARAMS, epoch_length, epoch_start_delay),
            SeqNum(100),
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|config| NodeId::new(config.key.pubkey()))
            .collect();
        let genesis_validators: Vec<NodeId<Pk<ValidatorSwapSwarm>>> = state_configs[0]
            .locked_epoch_validators[0]
            .validators
            .0
            .iter()
            .map(|vdata| vdata.node_id)
            .collect();

        let builders: Vec<SimNodeBuilder<ValidatorSwapSwarm>> = state_configs
            .into_iter()
            .map(|config| {
                let state_read = config.state_read.clone();
                let validators = config.locked_epoch_validators[0].clone();
                SimNodeBuilder {
                    id: NodeId::new(config.key.pubkey()),
                    state_builder: config,
                    router_scheduler: NoSerRouterConfig::new(all_peers.clone()).build(),
                    val_set_updater: MockValSetUpdaterSwap::new(
                        validators.validators,
                        epoch_length,
                    ),
                    txpool_executor: MockTxPoolExecutor::default().with_chain_params(&CHAIN_PARAMS),
                    ledger: MockLedger::new(state_read.clone()),
                    statesync_executor: MockStateSyncExecutor::new(state_read),
                    timestamp_period: DEFAULT_TIMESTAMP_PERIOD,
                }
            })
            .collect();

        let swarm = SimSwarm::from_builders(0, builders, |_| Network::reliable(latency));
        (swarm, genesis_validators)
    }

    #[test_case(SeqNum(100), Round(10), 1000; "update_interval: 100, epoch_start_delay: 10")]
    #[test_case(SeqNum(500), Round(10), 5000; "update_interval: 500, epoch_start_delay: 10")]
    #[test_case(SeqNum(2000), Round(50), 20000; "update_interval: 2000, epoch_start_delay: 50")]
    fn validator_switching(epoch_length: SeqNum, epoch_start_delay: Round, until_block: usize) {
        let delta = Duration::from_millis(20);
        let (mut swarm, _) = swap_swarm(epoch_length, epoch_start_delay, delta, delta);

        assert!(
            swarm.run_until_blocks(until_block, Time(0) + secs(6000)),
            "did not reach {until_block} blocks"
        );

        let ids = swarm.node_ids();
        // NOTE (carried from the legacy test): validator switching is mimicked
        // with unstaked validators that still send messages, so metrics aren't
        // pure happy-path. Only the timeout counts are pinned.
        SimVerifier::new()
            .ledger_min_len(until_block)
            .metric_exact(&ids, sim_metric!(consensus_events.local_timeout), 1)
            .metric_exact(&ids, sim_metric!(consensus_events.remote_timeout_msg), 3)
            .assert(&swarm);
    }

    /* ---- epoch-manager inspection helpers (over the SimSwarm view) ---- */

    fn verify_nodes_in_epoch<S: SwarmRelation>(
        swarm: &SimSwarm<S>,
        ids: &[NodeId<Pk<S>>],
        epoch: Epoch,
    ) {
        assert!(!ids.is_empty());
        for &id in ids {
            swarm.with_node(id, |node| {
                let round = node
                    .state()
                    .consensus()
                    .expect("consensus is live")
                    .get_current_round();
                let current = node
                    .state()
                    .epoch_manager()
                    .get_epoch(round)
                    .expect("epoch exists");
                assert_eq!(current, epoch, "node {id} not in {epoch:?}");
            });
        }
    }

    /// Verify every node scheduled `expected` to start the right number of
    /// rounds after the boundary block, and that they agree on that round.
    fn verify_nodes_scheduled_epoch<S: SwarmRelation>(
        swarm: &SimSwarm<S>,
        ids: &[NodeId<Pk<S>>],
        boundary_block_num: SeqNum,
        expected: Epoch,
    ) -> Round {
        assert!(!ids.is_empty());
        let mut rounds = Vec::new();
        for &id in ids {
            let esr = swarm.with_node(id, |node| {
                let blocks = node.ledger().get_finalized_blocks();
                let boundary = blocks
                    .values()
                    .find(|b| b.get_seq_num() == boundary_block_num)
                    .expect("boundary block committed");
                let em = node.state().epoch_manager();
                let esr = boundary.get_block_round() + em.epoch_start_delay;
                assert_ne!(
                    em.get_epoch(esr - Round(1)).expect("epoch exists"),
                    expected
                );
                assert_eq!(em.get_epoch(esr).expect("epoch exists"), expected);
                esr
            });
            rounds.push(esr);
        }
        assert!(
            rounds.iter().all(|r| *r == rounds[0]),
            "nodes disagree on epoch start round: {rounds:?}"
        );
        rounds[0]
    }

    fn verify_nodes_not_schedule_epoch<S: SwarmRelation>(
        swarm: &SimSwarm<S>,
        ids: &[NodeId<Pk<S>>],
        expected: Epoch,
    ) {
        for &id in ids {
            swarm.with_node(id, |node| {
                assert!(
                    !node
                        .state()
                        .epoch_manager()
                        .epoch_starts
                        .contains_key(&expected),
                    "node {id} unexpectedly scheduled {expected:?}"
                );
            });
        }
    }

    #[test]
    fn schedule_and_advance_epoch() {
        let delta = Duration::from_millis(20);
        let mut swarm = NoSerConfig::new(4, delta, &CHAIN_PARAMS)
            .execution_delay(SeqNum::MAX)
            .genesis_seqnum(SeqNum::MAX)
            .epoch_length(EPOCH_LENGTH)
            .epoch_start_delay(Round(20))
            .swarm(|_| Network::reliable(delta));

        let boundary = EPOCH_LENGTH - SeqNum(1); // 999, the block that schedules epoch 2
        let ids = swarm.node_ids();

        // before the boundary: still epoch 1, epoch 2 not yet scheduled (run with
        // a margin so no node has committed the boundary block).
        assert!(swarm.run_until_blocks(boundary.0 as usize - 5, Time(0) + secs(600)));
        verify_nodes_in_epoch(&swarm, &ids, Epoch(1));
        verify_nodes_not_schedule_epoch(&swarm, &ids, Epoch(2));

        // every node commits the boundary block: still epoch 1, but epoch 2 scheduled
        assert!(swarm.run_until_blocks(boundary.0 as usize + 1, Time(0) + secs(600)));
        verify_nodes_in_epoch(&swarm, &ids, Epoch(1));
        let epoch_start_round = verify_nodes_scheduled_epoch(&swarm, &ids, boundary, Epoch(2));

        // advance into epoch 2 (a few rounds past the boundary so every node,
        // not just the leader, has crossed it)
        assert!(swarm.run_until_round(epoch_start_round + Round(5), Time(0) + secs(600)));
        verify_nodes_in_epoch(&swarm, &ids, Epoch(2));
    }

    #[test]
    fn verify_correct_leaders_in_epoch() {
        let delta = Duration::from_millis(40);
        let latency = Duration::from_millis(20);
        let (mut swarm, genesis) = swap_swarm(EPOCH_LENGTH, Round(20), delta, latency);
        let ids = swarm.node_ids();

        // the swap updater uses genesis validators for epoch 1, the first half
        // for epoch 2, the second half for epoch 3.
        let (epoch_2_vals, epoch_3_vals) = genesis.split_at(2);
        let epoch_2_vals = epoch_2_vals.to_vec();
        let epoch_3_vals = epoch_3_vals.to_vec();

        let boundary_1 = EPOCH_LENGTH - SeqNum(1); // 999
        assert!(swarm.run_until_blocks(boundary_1.0 as usize + 1, Time(0) + secs(600)));
        // epoch 1: all genesis validators are Validators
        for &v in &genesis {
            assert_eq!(
                swarm.with_node_mut(v, |n| n.state_mut().get_role()),
                Role::Validator
            );
        }
        verify_nodes_in_epoch(&swarm, &ids, Epoch(1));
        let epoch_2_start = verify_nodes_scheduled_epoch(&swarm, &ids, boundary_1, Epoch(2));

        // epoch 2: first half are Validators, second half become FullNodes
        assert!(swarm.run_until_round(epoch_2_start + Round(5), Time(0) + secs(600)));
        verify_nodes_in_epoch(&swarm, &ids, Epoch(2));
        for &v in &epoch_2_vals {
            assert_eq!(
                swarm.with_node_mut(v, |n| n.state_mut().get_role()),
                Role::Validator
            );
        }
        for &v in &epoch_3_vals {
            assert_eq!(
                swarm.with_node_mut(v, |n| n.state_mut().get_role()),
                Role::FullNode
            );
        }

        let boundary_2 = SeqNum(EPOCH_LENGTH.0 * 2) - SeqNum(1); // 1999
        assert!(swarm.run_until_blocks(boundary_2.0 as usize + 1, Time(0) + secs(1200)));
        verify_nodes_in_epoch(&swarm, &ids, Epoch(2));
        let epoch_3_start = verify_nodes_scheduled_epoch(&swarm, &ids, boundary_2, Epoch(3));

        // epoch 3: the second half are Validators, first half are FullNodes
        assert!(swarm.run_until_round(epoch_3_start + Round(5), Time(0) + secs(1200)));
        verify_nodes_in_epoch(&swarm, &ids, Epoch(3));
        for &v in &epoch_3_vals {
            assert_eq!(
                swarm.with_node_mut(v, |n| n.state_mut().get_role()),
                Role::Validator
            );
        }
        for &v in &epoch_2_vals {
            assert_eq!(
                swarm.with_node_mut(v, |n| n.state_mut().get_role()),
                Role::FullNode
            );
        }

        // every committed block's author belongs to that round's validator set
        for &id in &ids {
            swarm.with_node(id, |node| {
                for block in node.ledger().get_finalized_blocks().values() {
                    let author = block.get_author();
                    let round = block.get_block_round();
                    let set = if round < epoch_2_start {
                        &genesis
                    } else if round < epoch_3_start {
                        &epoch_2_vals
                    } else {
                        &epoch_3_vals
                    };
                    assert!(
                        set.iter().any(|v| v == author),
                        "block author {author} not in the active validator set"
                    );
                }
            });
        }

        let max_ledger = swarm.finalized_blocks().into_iter().max().unwrap() as u64;
        SimVerifier::new()
            .metric_exact(&ids, sim_metric!(blocksync_events.self_headers_request), 0)
            .metric_exact(&ids, sim_metric!(blocksync_events.self_payload_request), 0)
            .metric_min(
                &ids,
                sim_metric!(consensus_events.handle_proposal),
                max_ledger,
            )
            .metric_min(&ids, sim_metric!(consensus_events.created_vote), max_ledger)
            .metric_max(&ids, sim_metric!(consensus_events.local_timeout), 4)
            .assert(&swarm);
    }

    #[test]
    fn schedule_epoch_after_blocksync() {
        // A node blacked out across the epoch boundary defers scheduling the new
        // epoch until it blocksyncs the boundary block. Uses a smaller
        // epoch_length than the legacy (1000) so the boundary is reached in a few
        // seconds, making the fixed blackout window tractable; the behavior under
        // test is identical.
        let epoch_length = SeqNum(100);
        let delta = Duration::from_millis(20);
        let mut swarm = NoSerConfig::new(4, delta, &CHAIN_PARAMS)
            .execution_delay(SeqNum::MAX)
            .genesis_seqnum(SeqNum::MAX)
            .epoch_length(epoch_length)
            .epoch_start_delay(Round(20))
            .swarm(|_| Network::reliable(delta));
        let ids = swarm.node_ids();
        let blackout = ids[0];
        let running: Vec<_> = ids[1..].to_vec();
        let boundary = epoch_length - SeqNum(1); // 99

        // run to just before the boundary (all connected): epoch 1, none scheduled
        assert!(swarm.run_until_any_blocks(boundary.0 as usize - 2, Time(0) + secs(60)));
        verify_nodes_in_epoch(&swarm, &ids, Epoch(1));
        verify_nodes_not_schedule_epoch(&swarm, &ids, Epoch(2));

        // blackout the first node via mid-run reconfiguration (as the legacy
        // does): it misses only the few blocks around the boundary, so it later
        // recovers via blocksync rather than statesync.
        swarm.set_network(|ids| {
            let first = ids[0];
            Network::reliable(delta).partition(Time(0)..Time(i128::MAX), [vec![first]])
        });
        // running supermajority commits the boundary (run a little past so every
        // running node has it); the blacked-out node is frozen just short of it
        assert!(swarm.run_until_any_blocks(boundary.0 as usize + 3, Time(0) + secs(60)));
        verify_nodes_scheduled_epoch(&swarm, &running, boundary, Epoch(2));
        verify_nodes_not_schedule_epoch(&swarm, &[blackout], Epoch(2));

        // restore connectivity; the node blocksyncs the boundary and schedules epoch 2
        swarm.set_network(|_| Network::reliable(delta));
        assert!(swarm.run_until_blocks(boundary.0 as usize + 10, Time(0) + secs(60)));
        verify_nodes_scheduled_epoch(&swarm, &ids, boundary, Epoch(2));

        // the never-partitioned nodes never needed blocksync
        SimVerifier::new()
            .metric_exact(
                &running,
                sim_metric!(blocksync_events.self_headers_request),
                0,
            )
            .metric_exact(
                &running,
                sim_metric!(blocksync_events.self_payload_request),
                0,
            )
            .assert(&swarm);
    }
}
