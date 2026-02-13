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

use std::{collections::BTreeMap, fmt::Debug, time::Duration};

use monad_consensus_types::metrics::Metrics;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_transformer::ID;
use monad_types::Round;
use monad_updaters::ledger::MockableLedger;

use crate::{mock_swarm::Nodes, swarm_relation::SwarmRelation};

type FetchMetricFunction = fn(&Metrics) -> u64;
type MetricName = &'static str;

#[macro_export]
macro_rules! fetch_metric {
    ( $( $k:ident ).+ ) => {{
        (stringify!($($k).+), |s: &Metrics| { s.$($k).+ })
    }};
}

#[derive(Debug, PartialEq, Eq)]
enum ExpectedTick {
    /// Exact tick timestamp
    Exact(Duration),
    /// Range(tick, delta)
    /// Expects swarm tick in the range [tick - delta, tick + delta]
    Range(Duration, Duration),
    /// No expected
    None,
}

#[derive(Debug)]
enum ExpectedMetric {
    // Exact metric value
    Exact(u64),
    // Expect metric is in the range [lower, upper]
    Range(u64, u64),
    // Minimum metric value
    Minimum(u64),
    // Maximum metric value
    Maximum(u64),
}

pub struct MockSwarmVerifier<S: SwarmRelation> {
    tick: ExpectedTick,
    metrics: BTreeMap<
        (ID<CertificateSignaturePubKey<S::SignatureType>>, MetricName),
        (FetchMetricFunction, ExpectedMetric),
    >,
}

impl<S: SwarmRelation> Default for MockSwarmVerifier<S> {
    fn default() -> Self {
        Self {
            tick: ExpectedTick::None,
            metrics: BTreeMap::new(),
        }
    }
}

impl<S: SwarmRelation> std::fmt::Debug for MockSwarmVerifier<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_map = BTreeMap::new();

        for ((id, metric_name), (_, expected)) in &self.metrics {
            debug_map.insert((id, metric_name), expected);
        }

        f.debug_struct("MockSwarmVerifier")
            .field("tick", &self.tick)
            .field("metrics", &debug_map)
            .finish()
    }
}

impl<S: SwarmRelation> MockSwarmVerifier<S> {
    pub fn tick_exact(mut self, tick: Duration) -> Self {
        if self.tick != ExpectedTick::None {
            panic!("Tick verify already set");
        }
        self.tick = ExpectedTick::Exact(tick);
        self
    }

    pub fn tick_range(mut self, mean_tick: Duration, delta: Duration) -> Self {
        if self.tick != ExpectedTick::None {
            panic!("Tick verify already set");
        }
        self.tick = ExpectedTick::Range(mean_tick, delta);
        self
    }

    pub fn metric_exact(
        &mut self,
        node_ids: &Vec<ID<CertificateSignaturePubKey<S::SignatureType>>>,
        fetch_metric: (MetricName, FetchMetricFunction),
        value: u64,
    ) -> &mut Self {
        for node_id in node_ids {
            self.metrics.insert(
                (*node_id, fetch_metric.0),
                (fetch_metric.1, ExpectedMetric::Exact(value)),
            );
        }
        self
    }

    pub fn metric_range(
        &mut self,
        node_ids: &Vec<ID<CertificateSignaturePubKey<S::SignatureType>>>,
        fetch_metric: (MetricName, FetchMetricFunction),
        lower: u64,
        upper: u64,
    ) -> &mut Self {
        for node_id in node_ids {
            self.metrics.insert(
                (*node_id, fetch_metric.0),
                (fetch_metric.1, ExpectedMetric::Range(lower, upper)),
            );
        }
        self
    }

    pub fn metric_minimum(
        &mut self,
        node_ids: &Vec<ID<CertificateSignaturePubKey<S::SignatureType>>>,
        fetch_metric: (MetricName, FetchMetricFunction),
        minimum: u64,
    ) -> &mut Self {
        for node_id in node_ids {
            self.metrics.insert(
                (*node_id, fetch_metric.0),
                (fetch_metric.1, ExpectedMetric::Minimum(minimum)),
            );
        }
        self
    }

    pub fn metric_maximum(
        &mut self,
        node_ids: &Vec<ID<CertificateSignaturePubKey<S::SignatureType>>>,
        fetch_metric: (MetricName, FetchMetricFunction),
        maximum: u64,
    ) -> &mut Self {
        for node_id in node_ids {
            self.metrics.insert(
                (*node_id, fetch_metric.0),
                (fetch_metric.1, ExpectedMetric::Maximum(maximum)),
            );
        }
        self
    }

    // Happy path metrics for a node should be independent of the path taken by its peers
    pub fn metrics_happy_path(
        &mut self,
        node_ids: &Vec<ID<CertificateSignaturePubKey<S::SignatureType>>>,
        swarm: &Nodes<S>,
    ) {
        let num_nodes_total = swarm.states.len() as u64;
        // TODO: add stake awareness
        let super_majority_nodes = num_nodes_total * 2 / 3 + 1;
        let max_byzantine_nodes = num_nodes_total - super_majority_nodes;

        // initial local timeout
        self.metric_exact(node_ids, fetch_metric!(consensus_events.local_timeout), 1)
            .metric_exact(
                node_ids,
                fetch_metric!(consensus_events.failed_txn_validation),
                0,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(consensus_events.invalid_proposal_round_leader),
                0,
            )
            // should not miss a block in between
            .metric_exact(
                node_ids,
                fetch_metric!(consensus_events.out_of_order_proposals),
                0,
            )
            // initial TC. If the node is in the happy path, it should never create a TC otherwise
            .metric_exact(node_ids, fetch_metric!(consensus_events.created_tc), 1)
            .metric_exact(
                node_ids,
                fetch_metric!(consensus_events.rx_execution_lagging),
                0,
            )
            // first proposal in Round 2 with TC
            .metric_maximum(
                node_ids,
                fetch_metric!(consensus_events.proposal_with_tc),
                1,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(consensus_events.failed_verify_randao_reveal_sig),
                0,
            )
            // blocksync metrics:
            // should not request blocksync
            .metric_exact(
                node_ids,
                fetch_metric!(blocksync_events.self_headers_request),
                0,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(blocksync_events.self_payload_request),
                0,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(blocksync_events.headers_response_successful),
                0,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(blocksync_events.headers_response_failed),
                0,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(blocksync_events.headers_response_unexpected),
                0,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(blocksync_events.headers_validation_failed),
                0,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(blocksync_events.payload_response_successful),
                0,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(blocksync_events.payload_response_failed),
                0,
            )
            .metric_exact(
                node_ids,
                fetch_metric!(blocksync_events.payload_response_unexpected),
                0,
            );

        for node_id in node_ids {
            let node = swarm.states.get(node_id).unwrap();
            let peer_id = node_id.get_peer_id();
            let ledger = node.executor.ledger().get_finalized_blocks();
            let ledger_len = ledger.len() as u64;
            // ledger should have genesis block
            assert!(ledger_len > 0);

            let blocks_proposed: Vec<_> = ledger
                .values()
                .filter(|b| b.get_author() == peer_id)
                .collect();
            // number of blocks authored in the ledger <= number of rounds as leader
            // NOTE: '<=' is used since blocks can be rejected
            let num_blocks_authored = blocks_proposed.len() as u64;

            // should handle proposal for all blocks in ledger
            self.metric_minimum(
                &vec![*node_id],
                fetch_metric!(consensus_events.handle_proposal),
                ledger_len,
            );
            // should vote for every block in the ledger
            self.metric_minimum(
                &vec![*node_id],
                fetch_metric!(consensus_events.created_vote),
                ledger_len,
            );
            // votes from f peers after receiving 2f+1 votes as a leader
            // 2x because votes are sent to current and next leader
            // NOTE: malicious votes should be rejected before reaching consensus
            self.metric_maximum(
                node_ids,
                fetch_metric!(consensus_events.old_vote_received),
                2 * (num_blocks_authored + 1) * max_byzantine_nodes,
            );
            // votes from 2f+1 peers as a leader
            // except if the node is a leader in round 2 when it receives timeouts instead
            self.metric_minimum(
                &vec![*node_id],
                fetch_metric!(consensus_events.vote_received),
                (num_blocks_authored.saturating_sub(1)) * super_majority_nodes,
            );
            // should create a QC everytime the node is a leader
            // except for block produced in round 2 which uses TC from round 1
            self.metric_minimum(
                &vec![*node_id],
                fetch_metric!(consensus_events.created_qc),
                num_blocks_authored.saturating_sub(1),
            );
            // a node processes an old QC (generated by itself) when it receives it
            // in the proposal for next round
            self.metric_minimum(
                &vec![*node_id],
                fetch_metric!(consensus_events.process_old_qc),
                num_blocks_authored,
            );
            // should broadcast proposals for all blocks authored in ledger
            self.metric_minimum(
                &vec![*node_id],
                fetch_metric!(consensus_events.broadcasting_proposal),
                num_blocks_authored,
            );
        }
    }
}

impl<S: SwarmRelation> MockSwarmVerifier<S> {
    pub fn verify(&self, swarm: &Nodes<S>) -> bool {
        let mut verification_passed = true;

        let actual_tick = swarm.tick;
        match self.tick {
            ExpectedTick::Exact(tick) => {
                if actual_tick != tick {
                    eprintln!(
                        "Tick verify error expected={:?} actual={:?}",
                        tick, swarm.tick
                    );
                    verification_passed = false;
                }
            }
            ExpectedTick::Range(mean, delta) => {
                let lower = mean.saturating_sub(delta);
                let upper = mean.saturating_add(delta);
                if actual_tick < lower || actual_tick > upper {
                    eprintln!(
                        "Tick verify error expected=[{:?},{:?}] actual={:?}",
                        lower, upper, actual_tick
                    );
                    verification_passed = false;
                }
            }
            ExpectedTick::None => {}
        }

        for ((node_id, metric_name), (fetch_metric, expected_metric)) in self.metrics.iter() {
            let node = swarm.states.get(node_id).unwrap();
            let actual_metric = fetch_metric(node.state.metrics());
            match expected_metric {
                ExpectedMetric::Exact(metric) => {
                    if actual_metric != *metric {
                        eprintln!(
                            "Metric verify error: node_id: {}, metric: {}, expected={:?} actual={:?}",
                            node_id, metric_name, metric, actual_metric
                        );
                        verification_passed = false;
                    }
                }
                ExpectedMetric::Range(lower, upper) => {
                    if actual_metric < *lower || actual_metric > *upper {
                        eprintln!(
                            "Metric verify error: node_id: {}, metric: {}, expected=[{:?},{:?}] actual={:?}",
                            node_id, metric_name, lower, upper, actual_metric
                        );
                        verification_passed = false;
                    }
                }
                ExpectedMetric::Minimum(minimum) => {
                    if actual_metric < *minimum {
                        eprintln!(
                            "Metric verify error: node_id: {}, metric: {}, expected>={:?} actual={:?}",
                            node_id, metric_name, minimum, actual_metric
                        );
                        verification_passed = false;
                    }
                }
                ExpectedMetric::Maximum(maximum) => {
                    if actual_metric > *maximum {
                        eprintln!(
                            "Metric verify error: node_id: {}, metric: {}, expected<={:?} actual={:?}",
                            node_id, metric_name, maximum, actual_metric
                        );
                        verification_passed = false;
                    }
                }
            }
        }

        verification_passed
    }
}

/// Computes the tick number for a happy path NoSer-like swarm to finish on a
/// certain round. For the NoSer-like swarm, message is delivered exactly after
/// exactly delta, regardless of message size
pub fn happy_path_tick_by_round(round: Round, delta: Duration) -> Duration {
    // (1 <timeout> + (epoch_start_round - 1 <the leader enters before delivered
    // to other nodes>) * 2 <round trip>) * delta
    delta * (1 + (round.0 - 1) * 2) as u32
}

/// Computes the tick number for a happy path NoSer-like swarm to finish when
/// any nodes has committed given number of blocks
pub fn happy_path_tick_by_block(block: usize, delta: Duration) -> Duration {
    // (1 <timeout> + (epoch_start_round + 1 <leader forms a QC-of-QC to commit>) *
    // 2 <round trip>) * delta
    delta * (1 + (block + 2) * 2) as u32
}
