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

//! Layer B: drive real `MonadState` nodes (Layer C) through the generic
//! `monad-sim` engine (Layer A).
//!
//! Each node is registered as a `monad-sim` process and driven entirely by
//! scheduled steps: events feed `state.update`, and the resulting commands are
//! dispatched in-process. Timers become [`CancelToken`]s; the
//! ledger / txpool / valset / statesync / loopback executors are reused
//! unmodified via `exec` + drain; outbound messages are routed through a network
//! process ([`SimNet`]) whose behaviour is a declarative [`Network`] model.
//!
//! [`SimNode`] / [`SimNet`] / [`Network`] are generic over [`SwarmRelation`].
//! [`SimSwarm`] is the harness — construction, bounded run control, and
//! between-step inspection. [`SimSwarm::from_builders`] is the generic entry
//! point; [`build_swarm`] / [`build_swarm_with`] are `NoSerSwarm` conveniences.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    ops::Range,
    time::Duration,
};

use bytes::Bytes;
use futures::{executor::block_on, StreamExt};
use monad_consensus_types::metrics::Metrics;
use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey};
use monad_executor::Executor;
use monad_executor_glue::{
    Command, Message, MonadEvent, RouterCommand, TimeoutVariant, TimerCommand,
};
use monad_router_scheduler::{RouterEvent, RouterScheduler};
use monad_sim::{CancelToken, Ctx, Handle, Simulation, StepLabel, Time};
use monad_state::{Forkpoint, MonadStateBuilder, VerifiedMonadMessage};
use monad_types::{NodeId, Round, GENESIS_ROUND};
use monad_updaters::{
    config_file::MockConfigFile, ledger::MockableLedger, loopback::LoopbackExecutor,
    statesync::MockableStateSync, txpool::MockableTxPool, val_set::MockableValSetUpdater,
};
use monad_validator::validator_set::BoxedValidatorSetTypeFactory;
use rand::{distributions::Distribution, Rng, SeedableRng};
use rand_chacha::ChaChaRng;

use crate::swarm_relation::{
    DebugSwarmRelation, NoSerSwarm, SwarmRelation, SwarmRelationStateType,
};

type Pk<S> = CertificateSignaturePubKey<<S as SwarmRelation>::SignatureType>;
type Ev<S> = MonadEvent<
    <S as SwarmRelation>::SignatureType,
    <S as SwarmRelation>::SignatureCollectionType,
    <S as SwarmRelation>::ExecutionProtocolType,
>;
type Cmd<S> = Command<
    Ev<S>,
    VerifiedMonadMessage<
        <S as SwarmRelation>::SignatureType,
        <S as SwarmRelation>::SignatureCollectionType,
        <S as SwarmRelation>::ExecutionProtocolType,
    >,
    <S as SwarmRelation>::SignatureType,
    <S as SwarmRelation>::SignatureCollectionType,
    <S as SwarmRelation>::ExecutionProtocolType,
    <S as SwarmRelation>::BlockPolicyType,
    <S as SwarmRelation>::ExecutionStateReadType,
    <S as SwarmRelation>::ChainConfigType,
    <S as SwarmRelation>::ChainRevisionType,
>;
type StateBuilder<S> = MonadStateBuilder<
    <S as SwarmRelation>::SignatureType,
    <S as SwarmRelation>::SignatureCollectionType,
    <S as SwarmRelation>::ExecutionProtocolType,
    <S as SwarmRelation>::BlockPolicyType,
    <S as SwarmRelation>::ExecutionStateReadType,
    <S as SwarmRelation>::ValidatorSetTypeFactory,
    <S as SwarmRelation>::LeaderElection,
    <S as SwarmRelation>::BlockValidator,
    <S as SwarmRelation>::ChainConfigType,
    <S as SwarmRelation>::ChainRevisionType,
>;
type Transport<S> = <S as SwarmRelation>::TransportMessage;

type LatencyFn = Box<dyn FnMut(&mut ChaChaRng) -> Duration>;
type DropFn<S> = Box<dyn Fn(&Link<S>, &Transport<S>) -> bool>;
type Partition<S> = (Range<Time>, Vec<BTreeSet<NodeId<Pk<S>>>>);
type NodeEntry<S> = (NodeId<Pk<S>>, Handle<SimNode<S>>);

fn dur(t: Time) -> Duration {
    Duration::from_nanos(t.0 as u64)
}

/// Default wall-clock estimate period for a node.
pub const DEFAULT_TIMESTAMP_PERIOD: Duration = Duration::from_millis(10);

/// Same-time priority classes for a node's scheduled steps, mirroring
/// production's `ParentExecutor::poll_next` order (lower runs first at a tick).
/// Under `TieBreak::Fifo` this makes a node drain ready internal work before it
/// services a same-tick network message (`RX`) — production's anti-starvation
/// ordering, and the opposite of the legacy sim's network-first tie-break. Under
/// `TieBreak::Randomized` these are ignored and same-tick steps are fully
/// shuffled (see `monad_sim::TieBreak`). `ControlPanel` (1) and `ConfigLoader`
/// (9) have no simulation analogue.
///
/// The values keep the *relative* order of `poll_next`; the absolute numbers
/// (and the gaps) don't matter beyond that. NOTE: production's order is itself
/// provisional — `poll_next` carries TODOs to deprioritize txpool ingestion and
/// to prioritize consensus (router) messages. If those land, update these
/// classes to match. Verified against `monad-updaters/src/parent.rs`.
mod prio {
    pub const TIMER: u32 = 0;
    pub const LEDGER: u32 = 2;
    pub const TXPOOL: u32 = 3;
    pub const VAL_SET: u32 = 4;
    pub const TIMESTAMP: u32 = 5;
    pub const LOOPBACK: u32 = 6;
    /// Network inbound (`Router`): serviced after ready internal work.
    pub const RX: u32 = 7;
    pub const STATESYNC: u32 = 8;
}

/// Configuration for one node in a [`SimSwarm`].
///
/// The new framework's node descriptor: it carries only what the simulation
/// drives, keyed by [`NodeId`] directly, with no transformer pipelines (the
/// network model lives in [`Network`] instead). Construct it with a struct
/// literal; [`SimNodeBuilder::debug`] type-erases it into [`DebugSwarmRelation`].
pub struct SimNodeBuilder<S: SwarmRelation> {
    pub id: NodeId<Pk<S>>,
    pub state_builder: StateBuilder<S>,
    pub router_scheduler: S::RouterScheduler,
    pub val_set_updater: S::ValSetUpdater,
    pub txpool_executor: S::TxPoolExecutor,
    pub ledger: S::Ledger,
    pub statesync_executor: S::StateSyncExecutor,
    pub timestamp_period: Duration,
}

impl<S: SwarmRelation> SimNodeBuilder<S> {
    /// Box this builder into the debugger's type-erased [`DebugSwarmRelation`],
    /// proving the simulation is generic over the swarm relation.
    pub fn debug(self) -> SimNodeBuilder<DebugSwarmRelation>
    where
        S: SwarmRelation<
            SignatureType = <DebugSwarmRelation as SwarmRelation>::SignatureType,
            SignatureCollectionType = <DebugSwarmRelation as SwarmRelation>::SignatureCollectionType,
            ExecutionProtocolType = <DebugSwarmRelation as SwarmRelation>::ExecutionProtocolType,
            TransportMessage = <DebugSwarmRelation as SwarmRelation>::TransportMessage,
            BlockPolicyType = <DebugSwarmRelation as SwarmRelation>::BlockPolicyType,
            BlockValidator = <DebugSwarmRelation as SwarmRelation>::BlockValidator,
            ExecutionStateReadType = <DebugSwarmRelation as SwarmRelation>::ExecutionStateReadType,
            ChainConfigType = <DebugSwarmRelation as SwarmRelation>::ChainConfigType,
            ChainRevisionType = <DebugSwarmRelation as SwarmRelation>::ChainRevisionType,
        >,
        S::RouterScheduler: Sync,
        S::Ledger: Sync,
    {
        SimNodeBuilder {
            id: self.id,
            state_builder: MonadStateBuilder {
                validator_set_factory: BoxedValidatorSetTypeFactory::new(
                    self.state_builder.validator_set_factory,
                ),
                leader_election: Box::new(self.state_builder.leader_election),
                block_validator: self.state_builder.block_validator,
                block_policy: self.state_builder.block_policy,
                state_read: self.state_builder.state_read,
                key: self.state_builder.key,
                certkey: self.state_builder.certkey,
                beneficiary: self.state_builder.beneficiary,
                forkpoint: self.state_builder.forkpoint,
                locked_epoch_validators: self.state_builder.locked_epoch_validators,
                block_sync_override_peers: self.state_builder.block_sync_override_peers,
                maybe_blocksync_rng_seed: self.state_builder.maybe_blocksync_rng_seed,
                consensus_config: self.state_builder.consensus_config,
                whitelisted_statesync_nodes: self.state_builder.whitelisted_statesync_nodes,
                statesync_expand_to_group: self.state_builder.statesync_expand_to_group,
                _phantom: PhantomData,
            },
            router_scheduler: Box::new(self.router_scheduler),
            val_set_updater: Box::new(self.val_set_updater),
            txpool_executor: Box::new(self.txpool_executor),
            ledger: Box::new(self.ledger),
            statesync_executor: Box::new(self.statesync_executor),
            timestamp_period: self.timestamp_period,
        }
    }
}

/// A single consensus node as a `monad-sim` process.
pub struct SimNode<S: SwarmRelation> {
    id: NodeId<Pk<S>>,
    state: SwarmRelationStateType<S>,

    router: S::RouterScheduler,
    ledger: S::Ledger,
    txpool: S::TxPoolExecutor,
    val_set: S::ValSetUpdater,
    statesync: S::StateSyncExecutor,
    loopback: LoopbackExecutor<Ev<S>>,
    config_file:
        MockConfigFile<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType>,

    /// One armed timer per variant; re-arming or resetting cancels the previous.
    timers: HashMap<TimeoutVariant, CancelToken>,

    // Wiring, set after the node and network are spawned.
    me: Option<Handle<SimNode<S>>>,
    net: Option<Handle<SimNet<S>>>,

    timestamp_period: Duration,
}

impl<S: SwarmRelation> SimNode<S> {
    fn me(&self) -> Handle<SimNode<S>> {
        self.me.expect("node not wired")
    }

    fn net(&self) -> Handle<SimNet<S>> {
        self.net.expect("node not wired")
    }

    /// Feed one event to the state machine and dispatch the resulting commands.
    fn handle_event(&mut self, event: Ev<S>, ctx: &mut Ctx) {
        let commands = self.state.update(event);
        self.dispatch(commands, ctx);
    }

    fn dispatch(&mut self, commands: Vec<Cmd<S>>, ctx: &mut Ctx) {
        let (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            config_file_cmds,
            val_set_cmds,
            _timestamp_cmds,
            txpool_cmds,
            _control_panel_cmds,
            loopback_cmds,
            statesync_cmds,
            _config_reload_cmds,
        ) = <Cmd<S>>::split_commands(commands);

        for command in timer_cmds {
            match command {
                TimerCommand::Schedule {
                    duration,
                    variant,
                    on_timeout,
                } => {
                    if let Some(previous) = self.timers.remove(&variant) {
                        previous.cancel();
                    }
                    let me = self.me();
                    let token = ctx.schedule_after(
                        me,
                        duration,
                        StepLabel::source("timer").priority(prio::TIMER),
                        move |node, ctx| {
                            node.timers.remove(&variant);
                            node.handle_event(on_timeout, ctx);
                        },
                    );
                    self.timers.insert(variant, token);
                }
                TimerCommand::ScheduleReset(variant) => {
                    if let Some(previous) = self.timers.remove(&variant) {
                        previous.cancel();
                    }
                }
            }
        }

        self.ledger.exec(ledger_cmds);
        self.txpool.exec(txpool_cmds);
        self.val_set.exec(val_set_cmds);
        self.loopback.exec(loopback_cmds);
        self.statesync.exec(statesync_cmds);
        self.config_file.exec(config_file_cmds);

        let now = dur(ctx.now());
        for command in router_cmds {
            match command {
                RouterCommand::Publish { target, message }
                | RouterCommand::PublishWithPriority {
                    target, message, ..
                } => {
                    self.router.send_outbound(now, target, message);
                }
                _ => {}
            }
        }

        self.drain_router(ctx);
        self.drain_executors(ctx);
    }

    /// Schedule an internal reaction as a zero-delay step on this node, so it
    /// flows through the engine queue rather than being handled synchronously.
    /// The step is tagged with its source's priority class (see [`prio`]), so
    /// internal reactions interleave per the scheduler's ordering (see the
    /// design doc, "Event ordering & interleaving").
    fn schedule_event(&self, ctx: &mut Ctx, source: &'static str, event: Ev<S>) {
        let priority = match source {
            "ledger" => prio::LEDGER,
            "txpool" => prio::TXPOOL,
            "val_set" => prio::VAL_SET,
            "loopback" => prio::LOOPBACK,
            "rx" => prio::RX,
            "statesync" => prio::STATESYNC,
            other => unreachable!("unclassified internal step source: {other}"),
        };
        let me = self.me();
        let label = StepLabel::source(source).priority(priority);
        ctx.schedule(me, ctx.now(), label, move |node, ctx| {
            node.handle_event(event, ctx)
        });
    }

    /// Drain router events: hand outbound messages to the network process, and
    /// schedule inbound messages for the state machine (zero-delay step).
    fn drain_router(&mut self, ctx: &mut Ctx) {
        let now = ctx.now();
        while let Some(event) = self.router.step_until(dur(now)) {
            match event {
                RouterEvent::Tx(to, transport) => {
                    let from = self.id;
                    let net = self.net();
                    ctx.schedule(net, now, StepLabel::source("route"), move |net, ctx| {
                        net.deliver(ctx, from, to, transport)
                    });
                }
                RouterEvent::Rx(from, message) => {
                    self.schedule_event(ctx, "rx", message.event(from));
                }
            }
        }
    }

    /// Drain the executors that emit events when ready, scheduling each as a
    /// zero-delay step. Unlike a synchronous drain, a node's internal reaction
    /// cascade unfolds across same-tick engine steps and can interleave with
    /// other same-tick steps — finer-grained, and observable in the step log.
    fn drain_executors(&mut self, ctx: &mut Ctx) {
        while self.ledger.ready() {
            if let Some(event) = block_on(self.ledger.next()) {
                self.schedule_event(ctx, "ledger", event);
            }
        }
        while self.txpool.ready() {
            if let Some(event) = block_on(self.txpool.next()) {
                self.schedule_event(ctx, "txpool", event);
            }
        }
        while self.val_set.ready() {
            if let Some(event) = block_on(self.val_set.next()) {
                self.schedule_event(ctx, "val_set", event);
            }
        }
        while self.loopback.ready() {
            if let Some(event) = block_on(self.loopback.next()) {
                self.schedule_event(ctx, "loopback", event);
            }
        }
        while let Some(event) = self.statesync.pop() {
            self.schedule_event(ctx, "statesync", event);
        }
    }

    /// A message arrives from `from`: hand it to the router and process it.
    fn receive(&mut self, from: NodeId<Pk<S>>, transport: Transport<S>, ctx: &mut Ctx) {
        self.router.process_inbound(dur(ctx.now()), from, transport);
        self.drain_router(ctx);
    }

    /// Periodic wall-clock estimate, re-scheduling itself every period.
    fn timestamp_tick(&mut self, ctx: &mut Ctx) {
        self.handle_event(
            MonadEvent::TimestampUpdateEvent(dur(ctx.now()).as_nanos()),
            ctx,
        );
        let me = self.me();
        ctx.schedule_after(
            me,
            self.timestamp_period,
            StepLabel::source("timestamp").priority(prio::TIMESTAMP),
            |node, ctx| node.timestamp_tick(ctx),
        );
    }

    /// Number of committed blocks (excluding genesis).
    pub fn finalized_blocks(&self) -> usize {
        self.ledger.get_finalized_blocks().len()
    }

    /// Current consensus round, or [`GENESIS_ROUND`] before consensus starts.
    pub fn current_round(&self) -> Round {
        self.state
            .consensus()
            .map_or(GENESIS_ROUND, |consensus| consensus.get_current_round())
    }

    /// Consensus metrics for this node.
    pub fn metrics(&self) -> &Metrics {
        self.state.metrics()
    }

    /// The node's full consensus state (epoch manager, role, live consensus,
    /// …), for inspection between steps. Mirrors the legacy `Node::state` field.
    pub fn state(&self) -> &SwarmRelationStateType<S> {
        &self.state
    }

    /// Mutable access to the node's consensus state, for inspection that needs
    /// `&mut` (e.g. [`MonadState::get_role`]).
    pub fn state_mut(&mut self) -> &mut SwarmRelationStateType<S> {
        &mut self.state
    }

    /// This node's committed ledger.
    pub fn ledger(&self) -> &S::Ledger {
        &self.ledger
    }

    /// The latest forkpoint the node has checkpointed, for restarting it. Panics
    /// if none has been generated yet.
    pub fn get_forkpoint(
        &self,
    ) -> Forkpoint<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType> {
        self.config_file
            .checkpoint
            .clone()
            .expect("no forkpoint generated")
            .into()
    }
}

/* ************************************************************************** */
/* Network model */

/// Per-message context handed to a [`NetworkModel`].
pub struct Link<S: SwarmRelation> {
    pub now: Time,
    pub from: NodeId<Pk<S>>,
    pub to: NodeId<Pk<S>>,
}

/// Decides the fate of a single message: the delays at which copies are
/// delivered to `link.to`. An empty result drops the message; more than one
/// duplicates it. Reordering needs no special handling — a smaller delay simply
/// arrives first.
pub trait NetworkModel<S: SwarmRelation> {
    fn deliveries(
        &mut self,
        link: &Link<S>,
        message: &Transport<S>,
        rng: &mut ChaChaRng,
    ) -> Vec<Duration>;
}

/// A declarative network model: a base latency plus optional, independent
/// conditions. Anything left unconfigured behaves reliably.
pub struct Conditions<S: SwarmRelation> {
    latency: LatencyFn,
    loss: f64,
    partitions: Vec<Partition<S>>,
    drop_if: Option<DropFn<S>>,
}

impl<S: SwarmRelation> NetworkModel<S> for Conditions<S> {
    fn deliveries(
        &mut self,
        link: &Link<S>,
        message: &Transport<S>,
        rng: &mut ChaChaRng,
    ) -> Vec<Duration> {
        if let Some(drop_if) = &self.drop_if {
            if drop_if(link, message) {
                return Vec::new();
            }
        }
        let split = self.partitions.iter().any(|(window, groups)| {
            window.contains(&link.now)
                && groups
                    .iter()
                    .any(|group| group.contains(&link.from) != group.contains(&link.to))
        });
        if split {
            return Vec::new();
        }
        if self.loss > 0.0 && rng.gen::<f64>() < self.loss {
            return Vec::new();
        }
        vec![(self.latency)(rng)]
    }
}

/// Declarative builder for a swarm's network behaviour. Start from
/// [`reliable`](Network::reliable) or [`with_latency`](Network::with_latency)
/// and layer conditions, or supply a [`custom`](Network::custom) model.
pub enum Network<S: SwarmRelation> {
    Conditions(Conditions<S>),
    Custom(Box<dyn NetworkModel<S>>),
}

impl<S: SwarmRelation> Default for Network<S> {
    fn default() -> Self {
        Network::reliable(Duration::from_millis(1))
    }
}

impl<S: SwarmRelation> Network<S> {
    /// Constant-latency, lossless network.
    pub fn reliable(latency: Duration) -> Self {
        Network::Conditions(Conditions {
            latency: Box::new(move |_| latency),
            loss: 0.0,
            partitions: Vec::new(),
            drop_if: None,
        })
    }

    /// Latency sampled per message from `distribution`.
    pub fn with_latency(distribution: impl Distribution<Duration> + 'static) -> Self {
        let mut network = Network::reliable(Duration::ZERO);
        network.conditions().latency = Box::new(move |rng| distribution.sample(rng));
        network
    }

    /// A fully custom model.
    pub fn custom(model: impl NetworkModel<S> + 'static) -> Self {
        Network::Custom(Box::new(model))
    }

    /// Drop each message independently with probability `probability`.
    pub fn loss(mut self, probability: f64) -> Self {
        self.conditions().loss = probability;
        self
    }

    /// While `window` is active, drop messages crossing between the given groups
    /// (a network split). Nodes absent from every group stay fully connected.
    pub fn partition(
        mut self,
        window: Range<Time>,
        groups: impl IntoIterator<Item = impl IntoIterator<Item = NodeId<Pk<S>>>>,
    ) -> Self {
        let groups = groups
            .into_iter()
            .map(|group| group.into_iter().collect())
            .collect();
        self.conditions().partitions.push((window, groups));
        self
    }

    /// Drop messages for which `predicate` holds.
    pub fn drop_if(
        mut self,
        predicate: impl Fn(&Link<S>, &Transport<S>) -> bool + 'static,
    ) -> Self {
        self.conditions().drop_if = Some(Box::new(predicate));
        self
    }

    /// Panics if called on a [`custom`](Network::custom) model.
    fn conditions(&mut self) -> &mut Conditions<S> {
        match self {
            Network::Conditions(conditions) => conditions,
            Network::Custom(_) => panic!("cannot layer conditions onto a custom network model"),
        }
    }

    fn into_model(self) -> Box<dyn NetworkModel<S>> {
        match self {
            Network::Conditions(conditions) => Box::new(conditions),
            Network::Custom(model) => model,
        }
    }
}

/// The network as a process: a routing table plus a [`NetworkModel`].
pub struct SimNet<S: SwarmRelation> {
    routes: BTreeMap<NodeId<Pk<S>>, Handle<SimNode<S>>>,
    model: Box<dyn NetworkModel<S>>,
    rng: ChaChaRng,
}

impl<S: SwarmRelation> SimNet<S> {
    /// Apply the network model to a message and schedule the surviving deliveries.
    fn deliver(
        &mut self,
        ctx: &mut Ctx,
        from: NodeId<Pk<S>>,
        to: NodeId<Pk<S>>,
        transport: Transport<S>,
    ) {
        let link = Link {
            now: ctx.now(),
            from,
            to,
        };
        let delays = self.model.deliveries(&link, &transport, &mut self.rng);
        // The recipient may have been removed from the swarm (node restart);
        // drop messages to it.
        let Some(&dst) = self.routes.get(&to) else {
            return;
        };
        match delays.as_slice() {
            [] => {}
            [delay] => {
                let at = ctx.now() + *delay;
                ctx.schedule(dst, at, StepLabel::source("deliver"), move |node, ctx| {
                    node.receive(from, transport, ctx)
                });
            }
            _ => {
                for delay in delays {
                    let at = ctx.now() + delay;
                    let copy = transport.clone();
                    ctx.schedule(dst, at, StepLabel::source("deliver"), move |node, ctx| {
                        node.receive(from, copy, ctx)
                    });
                }
            }
        }
    }
}

/* ************************************************************************** */
/* Harness */

/// A running swarm: the [`Simulation`] plus its node handles. Mirrors the parts
/// of the legacy `Nodes` engine that tests need — bounded run control and
/// ledger inspection — over the `monad-sim` runner.
pub struct SimSwarm<S: SwarmRelation> {
    sim: Simulation,
    nodes: Vec<NodeEntry<S>>,
    net: Handle<SimNet<S>>,
}

impl<S: SwarmRelation> SimSwarm<S> {
    pub fn sim(&mut self) -> &mut Simulation {
        &mut self.sim
    }

    pub fn node_ids(&self) -> Vec<NodeId<Pk<S>>> {
        self.nodes.iter().map(|(id, _)| *id).collect()
    }

    /// Replace the network model between runs (mid-run reconfiguration). Like the
    /// legacy `update_outbound_pipeline_for_all`, this affects messages sent
    /// after the call; steps already scheduled keep their original delivery.
    pub fn set_network(&mut self, network: impl FnOnce(&[NodeId<Pk<S>>]) -> Network<S>) {
        let ids: Vec<NodeId<Pk<S>>> = self.node_ids();
        let model = network(&ids).into_model();
        let net = self.net;
        self.sim.with_mut(net, |n| n.model = model);
    }

    /// Run every step scheduled at or before `deadline`.
    pub fn run_until(&mut self, deadline: Time) {
        self.sim.run_until_time(deadline);
    }

    /// Run until every node has committed `target` blocks or `deadline` passes.
    /// Returns whether the target was reached (`false` on timeout, e.g. a stalled
    /// network).
    pub fn run_until_blocks(&mut self, target: usize, deadline: Time) -> bool {
        let nodes = self.nodes.clone();
        self.sim.run_while(|sim| {
            sim.now() < deadline
                && nodes
                    .iter()
                    .any(|(_, h)| sim.with(*h, SimNode::finalized_blocks) < target)
        });
        self.finalized_blocks().into_iter().min().unwrap_or(0) >= target
    }

    /// Run until *some* node has committed `target` blocks or `deadline` passes
    /// (mirrors the legacy `until_block` terminator). Use this instead of
    /// [`run_until_blocks`](Self::run_until_blocks) when a node is expected to lag
    /// permanently (byzantine / blacked-out). Returns whether the target was
    /// reached.
    pub fn run_until_any_blocks(&mut self, target: usize, deadline: Time) -> bool {
        let nodes = self.nodes.clone();
        self.sim.run_while(|sim| {
            sim.now() < deadline
                && nodes
                    .iter()
                    .map(|(_, h)| sim.with(*h, SimNode::finalized_blocks))
                    .max()
                    .unwrap_or(0)
                    < target
        });
        self.finalized_blocks().into_iter().max().unwrap_or(0) >= target
    }

    /// Run until some node reaches `target` round or `deadline` passes. Returns
    /// whether the target was reached (`false` on timeout, e.g. a stalled network).
    pub fn run_until_round(&mut self, target: Round, deadline: Time) -> bool {
        let nodes = self.nodes.clone();
        self.sim.run_while(|sim| {
            sim.now() < deadline
                && nodes
                    .iter()
                    .map(|(_, h)| sim.with(*h, SimNode::current_round))
                    .max()
                    .unwrap_or(GENESIS_ROUND)
                    < target
        });
        self.current_rounds()
            .into_iter()
            .max()
            .unwrap_or(GENESIS_ROUND)
            >= target
    }

    /// Time of the next scheduled step, or `None` if the queue is empty.
    pub fn peek_tick(&self) -> Option<Time> {
        self.sim.peek_time()
    }

    /// The current simulation time.
    pub fn current_tick(&self) -> Time {
        self.sim.now()
    }

    /// Committed block count per node.
    pub fn finalized_blocks(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .map(|(_, h)| self.sim.with(*h, SimNode::finalized_blocks))
            .collect()
    }

    /// Current consensus round per node.
    pub fn current_rounds(&self) -> Vec<Round> {
        self.nodes
            .iter()
            .map(|(_, h)| self.sim.with(*h, SimNode::current_round))
            .collect()
    }

    /// Submit a transaction to a node's mempool (as the test harness would).
    pub fn send_transaction(&mut self, id: NodeId<Pk<S>>, tx: Bytes) {
        let handle = self.handle(id);
        self.sim
            .with_mut(handle, |node| node.txpool.send_transaction(tx));
    }

    /// Read a node's state (ledger, consensus, metrics, ...).
    pub fn with_node<R>(&self, id: NodeId<Pk<S>>, f: impl FnOnce(&SimNode<S>) -> R) -> R {
        let handle = self.handle(id);
        self.sim.with(handle, f)
    }

    /// Mutably access a node between steps — for inspection that needs `&mut`
    /// (e.g. `MonadState::get_role`). Does not advance the simulation.
    pub fn with_node_mut<R>(
        &mut self,
        id: NodeId<Pk<S>>,
        f: impl FnOnce(&mut SimNode<S>) -> R,
    ) -> R {
        let handle = self.handle(id);
        self.sim.with_mut(handle, f)
    }

    /// Remove a node from a running swarm and return it, so its ledger /
    /// forkpoint / state-backend can be read before restarting it. Messages to
    /// the node are dropped afterwards; steps already queued on it are skipped.
    pub fn remove_node(&mut self, id: NodeId<Pk<S>>) -> SimNode<S> {
        let pos = self
            .nodes
            .iter()
            .position(|(node_id, _)| *node_id == id)
            .expect("unknown node id");
        let (_, handle) = self.nodes.remove(pos);
        let net = self.net;
        self.sim.with_mut(net, |n| {
            n.routes.remove(&id);
        });
        self.sim.despawn(handle)
    }

    /// Add a node to a running swarm (e.g. a restarted node): wire it into the
    /// network and dispatch its initial commands at the current time.
    pub fn add_node(&mut self, builder: SimNodeBuilder<S>) {
        let SimNodeBuilder {
            id,
            state_builder,
            router_scheduler,
            val_set_updater,
            txpool_executor,
            ledger,
            statesync_executor,
            timestamp_period,
        } = builder;
        assert!(
            !self.nodes.iter().any(|(node_id, _)| *node_id == id),
            "node {id} already present"
        );
        let net = self.net;
        let (state, init_commands) = state_builder.build();
        let handle = self.sim.spawn(SimNode {
            id,
            state,
            router: router_scheduler,
            ledger,
            txpool: txpool_executor,
            val_set: val_set_updater,
            statesync: statesync_executor,
            loopback: LoopbackExecutor::default(),
            config_file: MockConfigFile::default(),
            timers: HashMap::new(),
            me: None,
            net: Some(net),
            timestamp_period,
        });
        self.sim.with_mut(handle, |n| n.me = Some(handle));
        self.sim.with_mut(net, |n| {
            n.routes.insert(id, handle);
        });
        self.nodes.push((id, handle));

        let now = self.sim.now();
        self.sim
            .schedule(handle, now, StepLabel::source("init"), move |n, ctx| {
                n.dispatch(init_commands, ctx);
                n.timestamp_tick(ctx);
            });
    }

    /// Panics if `id` is not a node in this swarm.
    fn handle(&self, id: NodeId<Pk<S>>) -> Handle<SimNode<S>> {
        self.nodes
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, handle)| *handle)
            .expect("unknown node id")
    }

    /// Assert that every node agrees on the common prefix of committed blocks.
    pub fn assert_agreement(&self) {
        let ledgers: Vec<_> = self
            .nodes
            .iter()
            .map(|(_, h)| {
                self.sim
                    .with(*h, |n| n.ledger.get_finalized_blocks().clone())
            })
            .collect();
        let min_len = ledgers.iter().map(BTreeMap::len).min().unwrap_or(0);
        let reference: Vec<_> = ledgers[0].iter().take(min_len).collect();
        for ledger in &ledgers {
            assert!(
                ledger.iter().take(min_len).collect::<Vec<_>>() == reference,
                "ledgers disagree on the committed prefix"
            );
        }
    }

    /// Build a running swarm from pre-built node components and kick it off. This
    /// is the generic construction path: each [`SimNodeBuilder`] already carries
    /// its node's executors, so any [`SwarmRelation`] works (e.g. the debugger's
    /// boxed relation). Network behaviour comes from `network`, which receives the
    /// node ids.
    pub fn from_builders(
        seed: u64,
        builders: Vec<SimNodeBuilder<S>>,
        network: impl FnOnce(&[NodeId<Pk<S>>]) -> Network<S>,
    ) -> SimSwarm<S> {
        let ids: Vec<NodeId<Pk<S>>> = builders.iter().map(|b| b.id).collect();

        let mut sim = Simulation::new(seed);
        let net = sim.spawn(SimNet {
            routes: BTreeMap::new(),
            model: network(&ids).into_model(),
            rng: ChaChaRng::seed_from_u64(seed),
        });

        let mut routes = BTreeMap::new();
        let mut pending = Vec::new();
        for builder in builders {
            let SimNodeBuilder {
                id,
                state_builder,
                router_scheduler,
                val_set_updater,
                txpool_executor,
                ledger,
                statesync_executor,
                timestamp_period,
            } = builder;
            let (state, init_commands) = state_builder.build();

            let handle = sim.spawn(SimNode {
                id,
                state,
                router: router_scheduler,
                ledger,
                txpool: txpool_executor,
                val_set: val_set_updater,
                statesync: statesync_executor,
                loopback: LoopbackExecutor::default(),
                config_file: MockConfigFile::default(),
                timers: HashMap::new(),
                me: None,
                net: None,
                timestamp_period,
            });
            sim.with_mut(handle, |n| {
                n.me = Some(handle);
                n.net = Some(net);
            });
            routes.insert(id, handle);
            pending.push((id, handle, init_commands));
        }
        sim.with_mut(net, |n| n.routes = routes);

        let nodes = pending
            .iter()
            .map(|(id, handle, _)| (*id, *handle))
            .collect();
        for (_, handle, init_commands) in pending {
            sim.schedule(handle, Time(0), StepLabel::source("init"), move |n, ctx| {
                n.dispatch(init_commands, ctx);
                n.timestamp_tick(ctx);
            });
        }

        SimSwarm { sim, nodes, net }
    }
}

/* ************************************************************************** */
/* NoSerSwarm construction */

/// Build a `NoSerSwarm` swarm with the default (reliable) network.
pub fn build_swarm(num_nodes: u16, seed: u64) -> SimSwarm<NoSerSwarm> {
    build_swarm_with(num_nodes, seed, |_| Network::default())
}

/// Build a `num_nodes`-validator `NoSerSwarm` simulation and kick it off.
/// `network` receives the node ids (so partitions can reference specific nodes)
/// and returns the network behaviour.
pub fn build_swarm_with(
    num_nodes: u16,
    seed: u64,
    network: impl FnOnce(&[NodeId<Pk<NoSerSwarm>>]) -> Network<NoSerSwarm>,
) -> SimSwarm<NoSerSwarm> {
    SimSwarm::from_builders(seed, noser_builders(num_nodes), network)
}

/// Construct `NoSerSwarm` node builders with mock executors and the default
/// chain config.
fn noser_builders(num_nodes: u16) -> Vec<SimNodeBuilder<NoSerSwarm>> {
    use monad_chain_config::{revision::ChainParams, MockChainConfig};
    use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
    use monad_execution_state_read::InMemoryStateInner;
    use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
    use monad_types::SeqNum;
    use monad_updaters::{
        ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
        val_set::MockValSetUpdaterNop,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    };

    use crate::swarm::make_state_configs;

    static CHAIN_PARAMS: ChainParams = ChainParams {
        tx_limit: 10_000,
        proposal_gas_limit: 300_000_000,
        proposal_byte_limit: 4_000_000,
        max_reserve_balance: 1_000_000_000_000_000_000,
        vote_pace: Duration::from_millis(5),
    };
    let delta = Duration::from_millis(100);

    let state_configs = make_state_configs::<NoSerSwarm>(
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
    let all_peers: BTreeSet<NodeId<Pk<NoSerSwarm>>> = state_configs
        .iter()
        .map(|config| NodeId::new(config.key.pubkey()))
        .collect();

    state_configs
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
        .collect()
}

#[cfg(test)]
mod tests {
    use monad_sim::{
        dist::normal_duration,
        time::{millis, secs},
    };

    use super::*;

    #[test]
    fn generic_over_swarm_relation() {
        use crate::swarm_relation::DebugSwarmRelation;

        // The same nodes, boxed into the debugger's relation, run through the
        // generic `from_builders` path — proving the sim is not tied to NoSerSwarm.
        let builders = noser_builders(4)
            .into_iter()
            .map(SimNodeBuilder::debug)
            .collect();
        let mut swarm =
            SimSwarm::<DebugSwarmRelation>::from_builders(0, builders, |_| Network::default());
        swarm.run_until(Time(0) + secs(2));
        assert!(swarm.finalized_blocks().iter().all(|&n| n >= 5));
    }

    #[test]
    fn happy_path_tick_and_metrics() {
        let mut swarm = build_swarm(4, 0);
        assert!(swarm.run_until_blocks(20, Time(0) + secs(10)));

        swarm.assert_agreement();
        for id in swarm.node_ids() {
            swarm.with_node(id, |node| {
                let committed = node.finalized_blocks() as u64;
                let m = node.metrics();
                // happy path: voted on / handled at least every committed block,
                // no blocksync, no out-of-order proposals.
                assert!(m.consensus_events.created_vote.get() >= committed);
                assert!(m.consensus_events.handle_proposal.get() >= committed);
                assert_eq!(m.blocksync_events.self_headers_request.get(), 0);
                assert_eq!(m.consensus_events.out_of_order_proposals.get(), 0);
            });
        }

        // The scheduler is deterministic, so the final tick is an exact value;
        // no `tick_range` tolerance is needed.
        assert_eq!(swarm.current_tick(), Time(0) + millis(104));
    }

    #[test]
    fn single_validator_commits_blocks() {
        let mut swarm = build_swarm(1, 0);
        swarm.run_until(Time(0) + secs(2));
        assert!(swarm.finalized_blocks()[0] >= 10);
    }

    #[test]
    fn multi_validator_commits_and_agrees() {
        let mut swarm = build_swarm(4, 0);
        assert!(swarm.run_until_blocks(10, Time(0) + secs(2)));
        swarm.assert_agreement();
    }

    #[test]
    fn run_until_round_reaches_target() {
        use monad_types::Round;

        let mut swarm = build_swarm(4, 0);
        assert!(swarm.run_until_round(Round(20), Time(0) + secs(2)));
        assert!(swarm.current_rounds().iter().any(|&r| r >= Round(20)));
        swarm.assert_agreement();
    }

    #[test]
    fn progresses_under_jittery_latency() {
        let mut swarm = build_swarm_with(4, 0, |_| {
            Network::with_latency(normal_duration(millis(2), millis(1)))
        });
        swarm.run_until(Time(0) + secs(2));
        assert!(swarm.finalized_blocks().iter().all(|&n| n >= 5));
    }

    #[test]
    fn even_split_partition_halts_progress() {
        // A 2|2 split: neither side has a supermajority (3 of 4), so no blocks
        // can commit while the partition holds.
        let mut swarm = build_swarm_with(4, 0, |ids| {
            Network::reliable(millis(1)).partition(
                Time(0)..Time(i128::MAX),
                [vec![ids[0], ids[1]], vec![ids[2], ids[3]]],
            )
        });
        swarm.run_until(Time(0) + secs(2));
        assert!(
            swarm.finalized_blocks().iter().all(|&n| n == 0),
            "expected no progress under an even split, got {:?}",
            swarm.finalized_blocks()
        );
    }
}
