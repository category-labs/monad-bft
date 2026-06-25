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

//! Shared `NoSerSwarm` test wiring for the `monad-sim` ports. Self-contained
//! (no legacy engine): builds [`SimNodeBuilder`]s and custom [`NetworkModel`]s.

#![allow(dead_code)]

use std::{collections::BTreeSet, ops::Range, time::Duration};

use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey, PubKey};
use monad_execution_state_read::InMemoryStateInner;
use monad_mock_swarm::{
    sim::{Link, Network, NetworkModel, SimNodeBuilder, SimSwarm, DEFAULT_TIMESTAMP_PERIOD},
    swarm::make_state_configs,
    swarm_relation::{NoSerSwarm, SwarmRelation},
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_sim::Time;
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{
    ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;

pub type NoSerPk = CertificateSignaturePubKey<<NoSerSwarm as SwarmRelation>::SignatureType>;
type NoSerTransport = <NoSerSwarm as SwarmRelation>::TransportMessage;

/// A configurable `NoSerSwarm` builder for the sim ports. Construct with
/// [`NoSerConfig::new`], override knobs fluently, then [`builders`] or [`swarm`].
pub struct NoSerConfig {
    pub num_nodes: u16,
    pub delta: Duration,
    pub chain_params: &'static ChainParams,
    pub execution_delay: SeqNum,
    pub genesis_seqnum: SeqNum,
    pub statesync_threshold: SeqNum,
    pub epoch_length: SeqNum,
    /// When set, the chain config uses epoch params (epoch boundaries every
    /// `epoch_length` blocks, new epoch starting `epoch_start_delay` rounds
    /// after the boundary block).
    pub epoch_start_delay: Option<Round>,
    pub seed: u64,
}

impl NoSerConfig {
    pub fn new(num_nodes: u16, delta: Duration, chain_params: &'static ChainParams) -> Self {
        Self {
            num_nodes,
            delta,
            chain_params,
            execution_delay: SeqNum(4),
            genesis_seqnum: SeqNum(4),
            statesync_threshold: SeqNum(100),
            epoch_length: SeqNum(2000),
            epoch_start_delay: None,
            seed: 0,
        }
    }

    pub fn execution_delay(mut self, v: SeqNum) -> Self {
        self.execution_delay = v;
        self
    }

    pub fn genesis_seqnum(mut self, v: SeqNum) -> Self {
        self.genesis_seqnum = v;
        self
    }

    pub fn statesync_threshold(mut self, v: SeqNum) -> Self {
        self.statesync_threshold = v;
        self
    }

    pub fn epoch_length(mut self, v: SeqNum) -> Self {
        self.epoch_length = v;
        self
    }

    pub fn epoch_start_delay(mut self, v: Round) -> Self {
        self.epoch_start_delay = Some(v);
        self
    }

    pub fn seed(mut self, v: u64) -> Self {
        self.seed = v;
        self
    }

    pub fn builders(&self) -> Vec<SimNodeBuilder<NoSerSwarm>> {
        let genesis = self.genesis_seqnum;
        let chain_config = match self.epoch_start_delay {
            Some(delay) => {
                MockChainConfig::new_with_epoch_params(self.chain_params, self.epoch_length, delay)
            }
            None => MockChainConfig::new(self.chain_params),
        };
        let state_configs = make_state_configs::<NoSerSwarm>(
            self.num_nodes,
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            || MockValidator,
            || PassthruBlockPolicy,
            move || InMemoryStateInner::genesis(genesis),
            self.execution_delay,
            self.delta,
            chain_config,
            self.statesync_threshold,
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|config| NodeId::new(config.key.pubkey()))
            .collect();
        let epoch_length = self.epoch_length;
        let chain_params = self.chain_params;
        state_configs
            .into_iter()
            .map(|config| {
                let state_read = config.state_read.clone();
                let validators = config.locked_epoch_validators[0].clone();
                SimNodeBuilder {
                    id: NodeId::new(config.key.pubkey()),
                    state_builder: config,
                    router_scheduler: NoSerRouterConfig::new(all_peers.clone()).build(),
                    val_set_updater: MockValSetUpdaterNop::new(validators.validators, epoch_length),
                    txpool_executor: MockTxPoolExecutor::default().with_chain_params(chain_params),
                    ledger: MockLedger::new(state_read.clone()),
                    statesync_executor: MockStateSyncExecutor::new(state_read),
                    timestamp_period: DEFAULT_TIMESTAMP_PERIOD,
                }
            })
            .collect()
    }

    pub fn swarm(
        &self,
        network: impl FnOnce(&[NodeId<NoSerPk>]) -> Network<NoSerSwarm>,
    ) -> SimSwarm<NoSerSwarm> {
        SimSwarm::from_builders(self.seed, self.builders(), network)
    }
}

/// Deterministic per-link latency = `max * (xor_of_pubkey_bytes / 255)`,
/// matching the legacy `XorLatencyTransformer`.
pub struct XorLatency {
    pub max: Duration,
}

impl NetworkModel<NoSerSwarm> for XorLatency {
    fn deliveries(
        &mut self,
        link: &Link<NoSerSwarm>,
        _message: &NoSerTransport,
        _rng: &mut ChaChaRng,
    ) -> Vec<Duration> {
        let mut ck: u8 = 0;
        for b in link.from.pubkey().bytes() {
            ck ^= b;
        }
        for b in link.to.pubkey().bytes() {
            ck ^= b;
        }
        vec![self.max.mul_f32(ck as f32 / u8::MAX as f32)]
    }
}

/// Base `delta` latency, plus `extra` for messages crossing the `slow` node's
/// boundary while `window` is active. Matches the legacy
/// `Partition(slow) + Periodic(window) + Latency(extra)` pipeline.
pub struct WindowedDelay {
    pub slow: NodeId<NoSerPk>,
    pub delta: Duration,
    pub window: Range<Time>,
    pub extra: Duration,
}

impl NetworkModel<NoSerSwarm> for WindowedDelay {
    fn deliveries(
        &mut self,
        link: &Link<NoSerSwarm>,
        _message: &NoSerTransport,
        _rng: &mut ChaChaRng,
    ) -> Vec<Duration> {
        let crossing = (link.from == self.slow) != (link.to == self.slow);
        let extra = if crossing && self.window.contains(&link.now) {
            self.extra
        } else {
            Duration::ZERO
        };
        vec![self.delta + extra]
    }
}

/// Order in which [`ReplayNetwork`] bursts held messages.
#[derive(Clone, Copy)]
pub enum ReplayOrder {
    Forward,
    Reverse,
    Random(u64),
}

/// Isolates `node` by holding every message crossing its boundary until
/// `release`, then delivering the held messages in a tight burst at `release`,
/// ordered per [`ReplayOrder`]. Everything else gets the base `delta` latency.
/// Models the legacy inbound `Partition(node) + Replay(release, order)`.
pub struct ReplayNetwork {
    node: NodeId<NoSerPk>,
    release: Time,
    delta: Duration,
    order: ReplayOrder,
    rng: ChaChaRng,
    held: u64,
}

impl ReplayNetwork {
    pub fn new(node: NodeId<NoSerPk>, release: Time, delta: Duration, order: ReplayOrder) -> Self {
        let seed = match order {
            ReplayOrder::Random(seed) => seed,
            _ => 0,
        };
        Self {
            node,
            release,
            delta,
            order,
            rng: ChaChaRng::seed_from_u64(seed),
            held: 0,
        }
    }
}

impl NetworkModel<NoSerSwarm> for ReplayNetwork {
    fn deliveries(
        &mut self,
        link: &Link<NoSerSwarm>,
        _message: &NoSerTransport,
        _rng: &mut ChaChaRng,
    ) -> Vec<Duration> {
        let crossing = (link.from == self.node) != (link.to == self.node);
        if crossing && link.now < self.release {
            // Held: deliver at `release`, with a sub-ms offset that orders the
            // burst. (Same delivery instant, different offsets -> the scheduler
            // processes them in offset order.)
            let to_release = (self.release.0 - link.now.0) as u64;
            let idx = self.held % 1_000_000;
            self.held += 1;
            let offset = match self.order {
                ReplayOrder::Forward => idx,
                ReplayOrder::Reverse => 1_000_000 - idx,
                ReplayOrder::Random(_) => self.rng.gen_range(0..1_000_000),
            };
            vec![Duration::from_nanos(to_release + offset)]
        } else {
            vec![self.delta]
        }
    }
}

/// A [`Network`] that holds and replays one node's messages — see
/// [`ReplayNetwork`].
pub fn replay_network(
    node: NodeId<NoSerPk>,
    release: Time,
    delta: Duration,
    order: ReplayOrder,
) -> Network<NoSerSwarm> {
    Network::custom(ReplayNetwork::new(node, release, delta, order))
}
