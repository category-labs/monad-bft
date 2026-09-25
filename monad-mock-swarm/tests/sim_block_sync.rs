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

//! Blocksync-recovery coverage, ported onto the `monad-sim` harness (mirrors
//! the legacy `block_sync.rs`, which it does not replace).
//!
//! The legacy tests blackout nodes by mutating the transformer pipeline mid-run
//! (`Partition + Periodic(from,to) + Drop`); the new framework expresses the
//! same thing declaratively with a time-bounded `Network::partition(from..to,
//! groups)` — no mid-run mutation. After the window the partition heals and the
//! blacked-out nodes catch up via blocksync.
//!
//! Ported here: `black_out_recovery_with_block_sync` (6 cases),
//! `lack_of_progress`, `extreme_delay_recovery_with_block_sync`,
//! `bsync_timeout_recovery`.
//!
//! NOTE on `bsync_timeout_recovery`: the legacy test's middle phase (reconnected
//! but blocksync still banned) was effectively a no-op — its `until_tick(5s)`
//! fired immediately because the clock was already at 5s. The port here
//! implements the *intended* scenario with real time windows, which is stronger
//! coverage: blackout `[0,5s)`, then `[5s,10s)` reconnected but blocksync
//! dropped (the node can follow new rounds but can't backfill the gap), then
//! open — only then can it catch up.

mod sim_common;

#[cfg(test)]
mod test {
    use std::{collections::BTreeSet, time::Duration};

    use monad_chain_config::{revision::ChainParams, MockChainConfig};
    use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopSignature,
    };
    use monad_execution_state_read::InMemoryStateInner;
    use monad_mock_swarm::{
        sim::{Network, SimNodeBuilder, SimSwarm, DEFAULT_TIMESTAMP_PERIOD},
        sim_metric,
        sim_verify::SimVerifier,
        swarm::make_state_configs,
        swarm_relation::MonadMessageNoSerSwarm,
    };
    use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
    use monad_sim::{time::secs, Time};
    use monad_state::VerifiedMonadMessage;
    use monad_types::{NodeId, SeqNum};
    use monad_updaters::{
        ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
        val_set::MockValSetUpdaterNop,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    };
    use test_case::test_case;

    use crate::sim_common::{NoSerConfig, WindowedDelay};

    static CHAIN_PARAMS: ChainParams = ChainParams {
        tx_limit: 10_000,
        proposal_gas_limit: 300_000_000,
        proposal_byte_limit: 4_000_000,
        max_reserve_balance: 1_000_000_000_000_000_000,
        vote_pace: Duration::from_millis(5),
    };

    #[test_case(4, Duration::from_millis(100), Duration::from_millis(200), Duration::from_secs(4), 1; "test 1")]
    #[test_case(50, Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4), 10; "test 2")]
    #[test_case(50, Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4), 25; "test 3")]
    #[test_case(50, Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4), 50; "test 4")]
    #[test_case(10, Duration::from_secs(0), Duration::from_secs(2), Duration::from_secs(4), 3; "test 5")]
    #[test_case(10, Duration::from_secs(0), Duration::from_secs(10), Duration::from_secs(20), 3; "test 6")]
    fn black_out_recovery_with_block_sync(
        num_nodes: u16,
        from: Duration,
        to: Duration,
        until: Duration,
        black_out_cnt: usize,
    ) {
        let delta = Duration::from_millis(20);
        assert!(
            from < to
                && to < until
                && black_out_cnt <= (num_nodes as usize)
                && black_out_cnt >= 1
                && num_nodes >= 4
        );

        // high execution delay so the state root doesn't trigger
        let mut swarm = NoSerConfig::new(num_nodes, delta, &CHAIN_PARAMS)
            .execution_delay(SeqNum::MAX)
            .genesis_seqnum(SeqNum::MAX)
            .statesync_threshold(SeqNum(2000))
            .swarm(|ids| {
                // blackout the first `black_out_cnt` nodes (key-gen order, matching
                // the legacy `state_configs.iter().take(black_out_cnt)`) by
                // partitioning them off from the rest during [from, to).
                let blacked: Vec<_> = ids[..black_out_cnt].to_vec();
                Network::reliable(delta).partition((Time(0) + from)..(Time(0) + to), [blacked])
            });

        swarm.run_until(Time(0) + until);

        let ids = swarm.node_ids();
        let running: Vec<_> = ids[black_out_cnt..].to_vec();
        let ledger_len = swarm.finalized_blocks().into_iter().max().unwrap_or(0) as u64;

        let mut verifier = SimVerifier::new();
        verifier
            // every node (incl. recovered) commits at least 20 blocks
            .ledger_min_len(20)
            // the never-partitioned nodes never need blocksync
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
            .metric_min(
                &running,
                sim_metric!(consensus_events.handle_proposal),
                ledger_len,
            )
            .metric_min(
                &running,
                sim_metric!(consensus_events.created_vote),
                ledger_len,
            );
        verifier.assert(&swarm);
    }

    #[test]
    fn extreme_delay_recovery_with_block_sync() {
        // A few messages to/from the first node are delayed ~800ms during the
        // window [1s, 2s); the node recovers via blocksync afterwards.
        let delta = Duration::from_millis(50);
        let mut swarm = NoSerConfig::new(4, delta, &CHAIN_PARAMS)
            .execution_delay(SeqNum::MAX)
            .genesis_seqnum(SeqNum::MAX)
            .swarm(|ids| {
                Network::custom(WindowedDelay {
                    slow: ids[0],
                    delta,
                    window: (Time(0) + secs(1))..(Time(0) + secs(2)),
                    extra: Duration::from_millis(800),
                })
            });

        swarm.run_until(Time(0) + secs(8));

        let ids = swarm.node_ids();
        let running: Vec<_> = ids[1..].to_vec();
        let ledger_len = swarm.finalized_blocks().into_iter().max().unwrap_or(0) as u64;

        let mut verifier = SimVerifier::new();
        verifier
            .ledger_min_len(20)
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
            .metric_min(
                &running,
                sim_metric!(consensus_events.handle_proposal),
                ledger_len,
            )
            .metric_min(
                &running,
                sim_metric!(consensus_events.created_vote),
                ledger_len,
            );
        verifier.assert(&swarm);
    }

    #[test]
    fn lack_of_progress() {
        let delta = Duration::from_millis(50);
        let mut swarm = NoSerConfig::new(4, delta, &CHAIN_PARAMS)
            .execution_delay(SeqNum::MAX)
            .genesis_seqnum(SeqNum::MAX)
            .swarm(|ids| {
                // the first node is partitioned off forever — it can never make
                // progress (the legacy asserts this via a ProgressTerminator that
                // times out; here we assert the stuck node stays at zero).
                let stuck = vec![ids[0]];
                Network::reliable(delta).partition(Time(0)..Time(i128::MAX), [stuck])
            });

        swarm.run_until(Time(0) + secs(3));

        let ids = swarm.node_ids();
        let stuck = swarm.with_node(ids[0], |n| n.finalized_blocks());
        assert_eq!(stuck, 0, "partitioned node should make no progress");
        // the remaining supermajority keeps committing
        for &id in &ids[1..] {
            assert!(
                swarm.with_node(id, |n| n.finalized_blocks()) > 0,
                "connected node {id} made no progress"
            );
        }
    }

    /// A 4-node `MonadMessageNoSerSwarm` (its transport is `VerifiedMonadMessage`,
    /// so the network model can match on message type).
    fn mm_swarm(
        delta: Duration,
        network: impl FnOnce(
            &[NodeId<CertificateSignaturePubKey<NopSignature>>],
        ) -> Network<MonadMessageNoSerSwarm>,
    ) -> SimSwarm<MonadMessageNoSerSwarm> {
        let state_configs = make_state_configs::<MonadMessageNoSerSwarm>(
            4,
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            || MockValidator,
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(SeqNum::MAX),
            SeqNum::MAX,
            delta,
            MockChainConfig::new(&CHAIN_PARAMS),
            SeqNum(1000),
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|config| NodeId::new(config.key.pubkey()))
            .collect();
        let builders: Vec<SimNodeBuilder<MonadMessageNoSerSwarm>> = state_configs
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
        SimSwarm::from_builders(0, builders, network)
    }

    #[test]
    fn bsync_timeout_recovery() {
        let delta = Duration::from_millis(50);
        let blackout_end = Time(0) + secs(5);
        let blocksync_ban_end = Time(0) + secs(10);

        let mut swarm = mm_swarm(delta, |ids| {
            let first = ids[0];
            Network::reliable(delta)
                // phase 1: first node fully partitioned off
                .partition(Time(0)..blackout_end, [vec![first]])
                // phases 1+2: blocksync messages dropped everywhere, so even once
                // reconnected the first node cannot backfill the gap
                .drop_if(move |link, msg| {
                    link.now < blocksync_ban_end
                        && matches!(
                            msg,
                            VerifiedMonadMessage::BlockSyncRequest(_)
                                | VerifiedMonadMessage::BlockSyncResponse(_)
                        )
                })
        });

        let ids = swarm.node_ids();
        let first = ids[0];
        let running: Vec<_> = ids[1..].to_vec();

        // phase 1: others make progress, the partitioned node commits nothing
        swarm.run_until(blackout_end);
        assert_eq!(
            swarm.with_node(first, |n| n.finalized_blocks()),
            0,
            "blacked-out node should commit nothing"
        );
        for &id in &running {
            assert!(swarm.with_node(id, |n| n.finalized_blocks()) >= 10);
        }

        // phase 2: reconnected, but blocksync still banned — still can't backfill
        swarm.run_until(blocksync_ban_end);
        assert_eq!(
            swarm.with_node(first, |n| n.finalized_blocks()),
            0,
            "node cannot backfill while blocksync is banned"
        );

        // phase 3: blocksync allowed — the node catches up
        swarm.run_until(Time(0) + secs(30));
        SimVerifier::new().ledger_min_len(10).assert(&swarm);
        swarm.assert_agreement();
    }
}
