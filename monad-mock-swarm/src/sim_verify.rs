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

//! Assertions for swarms driven through the `monad-sim` harness.
//!
//! This is the new framework's self-contained replacement for the legacy
//! `MockSwarmVerifier` / `swarm_ledger_verification`. It drives a [`SimSwarm`]
//! purely through its public surface ([`SimSwarm::with_node`],
//! [`SimSwarm::node_ids`], [`SimSwarm::current_tick`]) and depends on none of
//! the legacy engine.
//!
//! Collect expectations on a [`SimVerifier`] and call [`SimVerifier::assert`]
//! (or [`SimVerifier::check`] for the `Result`). Metric selectors are written
//! with the [`sim_metric!`] macro, e.g.
//! `verifier.metric_min(&ids, sim_metric!(consensus_events.created_vote), n)`.

use std::collections::BTreeMap;

use monad_consensus_types::metrics::Metrics;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_sim::Time;
use monad_types::NodeId;
use monad_updaters::ledger::MockableLedger;

use crate::{sim::SimSwarm, swarm_relation::SwarmRelation};

type Pk<S> = CertificateSignaturePubKey<<S as SwarmRelation>::SignatureType>;
type MetricSelector = Box<dyn Fn(&Metrics) -> u64>;

/// Builds a `(name, selector)` pair for a [`Metrics`] field, for the
/// `metric_*` methods on [`SimVerifier`]. The path is the field access minus
/// the trailing `.get()`, e.g. `sim_metric!(consensus_events.local_timeout)`.
#[macro_export]
macro_rules! sim_metric {
    ( $( $k:ident ).+ ) => {
        (stringify!($($k).+), |m| m.$($k).+.get())
    };
}

enum ExpectedTick {
    Exact(Time),
    Range(Time, Time),
}

#[derive(Clone, Debug)]
enum ExpectedMetric {
    Exact(u64),
    Range(u64, u64),
    Minimum(u64),
    Maximum(u64),
}

/// Accumulates tick / ledger / per-node-metric expectations, then checks them
/// against a [`SimSwarm`].
pub struct SimVerifier<S: SwarmRelation> {
    tick: Option<ExpectedTick>,
    min_ledger_len: Option<usize>,
    metrics: BTreeMap<(NodeId<Pk<S>>, &'static str), (MetricSelector, ExpectedMetric)>,
}

impl<S: SwarmRelation> Default for SimVerifier<S> {
    fn default() -> Self {
        Self {
            tick: None,
            min_ledger_len: None,
            metrics: BTreeMap::new(),
        }
    }
}

impl<S: SwarmRelation> SimVerifier<S> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Expect the final tick to equal `tick` exactly.
    pub fn tick_exact(&mut self, tick: Time) -> &mut Self {
        self.tick = Some(ExpectedTick::Exact(tick));
        self
    }

    /// Expect the final tick to fall within `[lower, upper]`.
    pub fn tick_range(&mut self, lower: Time, upper: Time) -> &mut Self {
        self.tick = Some(ExpectedTick::Range(lower, upper));
        self
    }

    /// Expect every node's ledger to hold at least `min` finalized blocks.
    pub fn ledger_min_len(&mut self, min: usize) -> &mut Self {
        self.min_ledger_len = Some(min);
        self
    }

    pub fn metric_exact<F: Fn(&Metrics) -> u64 + Clone + 'static>(
        &mut self,
        node_ids: &[NodeId<Pk<S>>],
        metric: (&'static str, F),
        value: u64,
    ) -> &mut Self {
        self.insert(node_ids, metric, ExpectedMetric::Exact(value))
    }

    pub fn metric_range<F: Fn(&Metrics) -> u64 + Clone + 'static>(
        &mut self,
        node_ids: &[NodeId<Pk<S>>],
        metric: (&'static str, F),
        lower: u64,
        upper: u64,
    ) -> &mut Self {
        self.insert(node_ids, metric, ExpectedMetric::Range(lower, upper))
    }

    pub fn metric_min<F: Fn(&Metrics) -> u64 + Clone + 'static>(
        &mut self,
        node_ids: &[NodeId<Pk<S>>],
        metric: (&'static str, F),
        minimum: u64,
    ) -> &mut Self {
        self.insert(node_ids, metric, ExpectedMetric::Minimum(minimum))
    }

    pub fn metric_max<F: Fn(&Metrics) -> u64 + Clone + 'static>(
        &mut self,
        node_ids: &[NodeId<Pk<S>>],
        metric: (&'static str, F),
        maximum: u64,
    ) -> &mut Self {
        self.insert(node_ids, metric, ExpectedMetric::Maximum(maximum))
    }

    fn insert<F: Fn(&Metrics) -> u64 + Clone + 'static>(
        &mut self,
        node_ids: &[NodeId<Pk<S>>],
        metric: (&'static str, F),
        expected: ExpectedMetric,
    ) -> &mut Self {
        let (name, selector) = metric;
        // Last write for a given (id, name) wins, matching the legacy map.
        for &id in node_ids {
            self.metrics
                .insert((id, name), (Box::new(selector.clone()), expected.clone()));
        }
        self
    }

    /// Check all expectations, collecting every failure.
    pub fn check(&self, swarm: &SimSwarm<S>) -> Result<(), Vec<String>> {
        let mut failures = Vec::new();

        if let Some(expected) = &self.tick {
            let actual = swarm.current_tick();
            match expected {
                ExpectedTick::Exact(t) => {
                    if actual != *t {
                        failures.push(format!("tick: expected {:?}, got {:?}", t, actual));
                    }
                }
                ExpectedTick::Range(lower, upper) => {
                    if actual < *lower || actual > *upper {
                        failures.push(format!(
                            "tick: expected [{:?}, {:?}], got {:?}",
                            lower, upper, actual
                        ));
                    }
                }
            }
        }

        if let Some(min) = self.min_ledger_len {
            for id in swarm.node_ids() {
                let len = swarm.with_node(id, |node| node.ledger().get_finalized_blocks().len());
                if len < min {
                    failures.push(format!(
                        "ledger: node {} has {} finalized blocks, expected >= {}",
                        id, len, min
                    ));
                }
            }
        }

        for ((id, name), (selector, expected)) in &self.metrics {
            let actual = swarm.with_node(*id, |node| selector(node.metrics()));
            let ok = match expected {
                ExpectedMetric::Exact(v) => actual == *v,
                ExpectedMetric::Range(l, u) => actual >= *l && actual <= *u,
                ExpectedMetric::Minimum(v) => actual >= *v,
                ExpectedMetric::Maximum(v) => actual <= *v,
            };
            if !ok {
                failures.push(format!(
                    "metric {} on node {}: expected {:?}, got {}",
                    name, id, expected, actual
                ));
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }

    /// Check all expectations, panicking with every failure if any fail.
    pub fn assert(&self, swarm: &SimSwarm<S>) {
        if let Err(failures) = self.check(swarm) {
            panic!("SimVerifier failed:\n  {}", failures.join("\n  "));
        }
    }

    /// Happy-path consensus/blocksync expectations for the given nodes,
    /// independent of the path their peers take. Mirrors the legacy
    /// `MockSwarmVerifier::metrics_happy_path`. Call after running the swarm
    /// (it reads each node's ledger to derive the per-node bounds).
    pub fn metrics_happy_path(
        &mut self,
        node_ids: &[NodeId<Pk<S>>],
        swarm: &SimSwarm<S>,
    ) -> &mut Self {
        let num_nodes_total = swarm.node_ids().len() as u64;
        // TODO: add stake awareness
        let super_majority_nodes = num_nodes_total * 2 / 3 + 1;
        let max_byzantine_nodes = num_nodes_total - super_majority_nodes;

        // initial local timeout
        self.metric_exact(node_ids, sim_metric!(consensus_events.local_timeout), 1)
            .metric_exact(
                node_ids,
                sim_metric!(consensus_events.failed_txn_validation),
                0,
            )
            .metric_exact(
                node_ids,
                sim_metric!(consensus_events.invalid_proposal_round_leader),
                0,
            )
            // should not miss a block in between
            .metric_exact(
                node_ids,
                sim_metric!(consensus_events.out_of_order_proposals),
                0,
            )
            // initial TC. In the happy path a node should never create a TC otherwise.
            .metric_exact(node_ids, sim_metric!(consensus_events.created_tc), 1)
            .metric_exact(
                node_ids,
                sim_metric!(consensus_events.rx_execution_lagging),
                0,
            )
            // first proposal in Round 2 with TC
            .metric_max(node_ids, sim_metric!(consensus_events.proposal_with_tc), 1)
            .metric_exact(
                node_ids,
                sim_metric!(consensus_events.failed_verify_randao_reveal_sig),
                0,
            )
            // blocksync metrics: should not request blocksync
            .metric_exact(
                node_ids,
                sim_metric!(blocksync_events.self_headers_request),
                0,
            )
            .metric_exact(
                node_ids,
                sim_metric!(blocksync_events.self_payload_request),
                0,
            )
            .metric_exact(
                node_ids,
                sim_metric!(blocksync_events.headers_response_successful),
                0,
            )
            .metric_exact(
                node_ids,
                sim_metric!(blocksync_events.headers_response_failed),
                0,
            )
            .metric_exact(
                node_ids,
                sim_metric!(blocksync_events.headers_response_unexpected),
                0,
            )
            .metric_exact(
                node_ids,
                sim_metric!(blocksync_events.headers_validation_failed),
                0,
            )
            .metric_exact(
                node_ids,
                sim_metric!(blocksync_events.payload_response_successful),
                0,
            )
            .metric_exact(
                node_ids,
                sim_metric!(blocksync_events.payload_response_failed),
                0,
            )
            .metric_exact(
                node_ids,
                sim_metric!(blocksync_events.payload_response_unexpected),
                0,
            );

        for &node_id in node_ids {
            let (ledger_len, num_blocks_authored) = swarm.with_node(node_id, |node| {
                let ledger = node.ledger().get_finalized_blocks();
                let ledger_len = ledger.len() as u64;
                // ledger should have genesis block
                assert!(ledger_len > 0);
                // number of blocks authored in the ledger <= number of rounds as
                // leader. NOTE: '<=' since blocks can be rejected.
                let authored = ledger
                    .values()
                    .filter(|b| b.get_author() == &node_id)
                    .count() as u64;
                (ledger_len, authored)
            });

            // should handle a proposal for all blocks in the ledger
            self.metric_min(
                &[node_id],
                sim_metric!(consensus_events.handle_proposal),
                ledger_len,
            )
            // should vote for every block in the ledger
            .metric_min(
                &[node_id],
                sim_metric!(consensus_events.created_vote),
                ledger_len,
            )
            // votes from f peers after receiving 2f+1 votes as a leader; 2x
            // because votes are sent to the current and next leader
            .metric_max(
                node_ids,
                sim_metric!(consensus_events.old_vote_received),
                2 * (num_blocks_authored + 1) * max_byzantine_nodes,
            )
            // votes from 2f+1 peers as a leader, except if the node is leader in
            // round 2 when it receives timeouts instead
            .metric_min(
                &[node_id],
                sim_metric!(consensus_events.vote_received),
                num_blocks_authored.saturating_sub(1) * super_majority_nodes,
            )
            // should create a QC every time the node is a leader, except for the
            // block produced in round 2 which uses the TC from round 1
            .metric_min(
                &[node_id],
                sim_metric!(consensus_events.created_qc),
                num_blocks_authored.saturating_sub(1),
            )
            // a node processes an old QC (generated by itself) when it receives
            // it in the proposal for the next round
            .metric_min(
                &[node_id],
                sim_metric!(consensus_events.process_old_qc),
                num_blocks_authored,
            )
            // should create proposals for all blocks authored in the ledger
            .metric_min(
                &[node_id],
                sim_metric!(consensus_events.creating_proposal),
                num_blocks_authored,
            );
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use monad_sim::{time::secs, Time};

    use super::*;
    use crate::sim::build_swarm;

    #[test]
    fn happy_path_verifier_passes() {
        let mut swarm = build_swarm(4, 0);
        assert!(swarm.run_until_blocks(20, Time(0) + secs(10)));
        let ids = swarm.node_ids();

        let mut verifier = SimVerifier::new();
        verifier.ledger_min_len(1).metrics_happy_path(&ids, &swarm);
        verifier.assert(&swarm);
    }

    #[test]
    fn detects_metric_mismatch() {
        let mut swarm = build_swarm(4, 0);
        assert!(swarm.run_until_blocks(10, Time(0) + secs(10)));
        let ids = swarm.node_ids();

        // local_timeout is 1 on the happy path; expecting 999 must fail.
        let mut verifier = SimVerifier::new();
        verifier.metric_exact(&ids, sim_metric!(consensus_events.local_timeout), 999);
        assert!(verifier.check(&swarm).is_err());
    }
}
