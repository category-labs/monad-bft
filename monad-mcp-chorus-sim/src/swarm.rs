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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use chorus::{
    CadenceOutput, CadenceRuntime, CadenceWireMsg, Conductor, Runtime, SlotConsensus,
    SlotLifecycle, SlotManager,
    proposing::ProposalPlanner,
    slot::chorus::{ChorusDAEvent, ProposalDAEvent},
};
use monad_mcp_chorus::stub as chorus;
use monad_mcp_common::{Slot, Timestamp, stub::NodeId};
use monad_sim::{RunOutcome, StepLabel};
use monad_sim_swarm::{Network, Swarm};

use crate::{
    da::{DaAnnouncement, MockDa},
    node::{ProposerHarness, SimMessage, SimNode, time_of, to_timestamp},
};

type CadenceNode<S, C> = SimNode<CadenceRuntime<S, C>>;

pub struct CadenceSwarmBuilder<S: SlotConsensus, C: Conductor> {
    seed: u64,
    network: Network<NodeId, SimMessage<CadenceWireMsg<S, C>>>,
    nodes: Vec<(NodeId, CadenceNode<S, C>)>,
    track_logs: bool,
    log: FinalizationLog,
}

impl<S: SlotConsensus, C: Conductor> CadenceSwarmBuilder<S, C> {
    pub fn new() -> Self {
        Self {
            seed: 0,
            network: Network::default(),
            nodes: Vec::new(),
            track_logs: true,
            log: FinalizationLog::new(),
        }
    }

    pub fn set_latency(&mut self, latency: Duration) {
        self.network = Network::reliable(latency);
    }

    pub fn set_network(&mut self, network: Network<NodeId, SimMessage<CadenceWireMsg<S, C>>>) {
        self.network = network;
    }

    pub fn set_seed(&mut self, seed: u64) {
        self.seed = seed;
    }

    pub fn track_logs(&mut self, track: bool) {
        self.track_logs = track;
    }

    pub fn add_node(
        &mut self,
        id: NodeId,
        conductor: C,
        slot_config: S::Config,
        slot_context: S::Context,
    ) where
        S: 'static,
        C: 'static,
        C::Message: Clone,
        CadenceRuntime<S, C>: Send,
        CadenceWireMsg<S, C>: Send,
    {
        let slot_manager = SlotManager::new(slot_config, slot_context);
        let runtime = CadenceRuntime::new(slot_manager, conductor);

        let mut record = self.track_logs.then(|| self.log.allocate(id));
        let on_output = move |now: Timestamp, output: CadenceOutput<S>| {
            let CadenceOutput::Finalized(slot, _) = output else {
                return;
            };
            if let Some(record) = &mut record {
                record(now, slot);
            }
        };

        let node = SimNode::new(id, runtime, on_output);
        self.nodes.push((id, node));
    }

    /// Like [`Self::add_node`], for a node that also runs the proposal
    /// machinery: `planner` decides when to seal, `da` is the node's mock DA
    /// layer (the same instance the node reports availability from, so
    /// consensus sees what the planner submits and the network delivers).
    /// `observer` receives each finalization alongside the built-in wiring —
    /// tests use it to record finalization data.
    // one parameter per wired component; a config struct would just move
    // the count elsewhere
    #[allow(clippy::too_many_arguments)]
    pub fn add_proposer_node(
        &mut self,
        id: NodeId,
        conductor: C,
        slot_config: S::Config,
        slot_context: S::Context,
        planner: ProposalPlanner,
        da: Arc<MockDa>,
        mut observer: impl FnMut(Timestamp, Slot, &S::FinalizationData) + Send + 'static,
    ) where
        S: SlotConsensus<LocalIn = ChorusDAEvent> + 'static,
        C: 'static,
        C::Message: Clone,
        CadenceRuntime<S, C>: Send,
        CadenceWireMsg<S, C>: Send,
    {
        let planner = Arc::new(Mutex::new(planner));
        let facts = planner.clone();

        let slot_manager = SlotManager::new(slot_config, slot_context);
        let runtime = CadenceRuntime::new(slot_manager, conductor);

        let mut record = self.track_logs.then(|| self.log.allocate(id));
        let on_output = move |now: Timestamp, output: CadenceOutput<S>| match output {
            CadenceOutput::Lifecycle(event) => tell_planner(&facts, now, event),
            CadenceOutput::Local(..) | CadenceOutput::CommitOptimistic(_) => {}
            CadenceOutput::Finalized(slot, data) => {
                if let Some(record) = &mut record {
                    record(now, slot);
                }
                observer(now, slot, &data);
            }
        };

        // The mock layer makes a proposal available as a whole, so one
        // announcement reports both the signed header and its decoding.
        let da_events = Box::new(|announcement: &DaAnnouncement| {
            let DaAnnouncement { lane, header } = announcement;
            let slot = announcement.slot();
            vec![
                (
                    slot,
                    ChorusDAEvent {
                        j: *lane,
                        event: ProposalDAEvent::HeaderSeen(header.clone()),
                    },
                ),
                (
                    slot,
                    ChorusDAEvent {
                        j: *lane,
                        event: ProposalDAEvent::Decoded(announcement.pid()),
                    },
                ),
            ]
        });

        let harness = ProposerHarness {
            planner,
            da,
            da_events,
            seal_alarm: None,
        };
        let node = SimNode::with_proposer(id, runtime, on_output, harness);
        self.nodes.push((id, node));
    }

    pub fn build(self) -> CadenceSwarm<S, C>
    where
        S: 'static,
        C: 'static,
        C::Message: Clone,
        CadenceRuntime<S, C>: Send,
        CadenceWireMsg<S, C>: Send,
    {
        CadenceSwarm {
            swarm: build_sim_swarm(self.seed, self.network, self.nodes),
            log: self.log,
        }
    }
}

// Forwards the slot lifecycle the proposal planner consumes; the owning
// SimNode polls the planner's seals and arms its alarm (see node.rs).
fn tell_planner(planner: &Mutex<ProposalPlanner>, now: Timestamp, event: SlotLifecycle) {
    let mut planner = planner.lock().expect("planner poisoned");
    match event {
        SlotLifecycle::Opened { slot, deadline } => planner.handle_slot_open(now, slot, deadline),
        SlotLifecycle::CapAdvance { cap } => planner.handle_cap_advance(now, cap),
        SlotLifecycle::Completed { .. } => {}
    }
}

/// Wire nodes into a simulated network and schedule each node's init step
pub(crate) fn build_sim_swarm<R>(
    seed: u64,
    network: Network<NodeId, SimMessage<R::Message>>,
    nodes: Vec<(NodeId, SimNode<R>)>,
) -> Swarm<SimNode<R>>
where
    R: Runtime + Send + 'static,
    R::Message: Clone + Send + 'static,
{
    let mut swarm = Swarm::build(seed, network, nodes);
    for id in swarm.node_ids() {
        let handle = swarm.handle(&id).expect("node just built");
        swarm.sim().schedule(
            handle,
            time_of(Timestamp::GENESIS),
            StepLabel::source("init"),
            |node, ctx| node.init(ctx),
        );
    }
    swarm
}

impl<S: SlotConsensus, C: Conductor> Default for CadenceSwarmBuilder<S, C> {
    fn default() -> Self {
        Self::new()
    }
}

// A built swarm: run control in cadence ticks, plus the finalization
// logs collected from the nodes added with tracking enabled.
pub struct CadenceSwarm<S, C>
where
    S: SlotConsensus + 'static,
    C: Conductor + 'static,
    C::Message: Clone,
    CadenceRuntime<S, C>: Send,
    CadenceWireMsg<S, C>: Send,
{
    swarm: Swarm<CadenceNode<S, C>>,
    log: FinalizationLog,
}

impl<S, C> CadenceSwarm<S, C>
where
    S: SlotConsensus + 'static,
    C: Conductor + 'static,
    C::Message: Clone,
    CadenceRuntime<S, C>: Send,
    CadenceWireMsg<S, C>: Send,
{
    pub fn now(&self) -> Timestamp {
        to_timestamp(self.swarm.simulation().now())
    }

    pub fn run_until(&mut self, at: Timestamp) -> RunOutcome {
        self.swarm.run_until_time(time_of(at))
    }

    pub fn run_to_completion(&mut self) -> RunOutcome {
        self.swarm.run_to_completion()
    }

    pub fn log(&self) -> &FinalizationLog {
        &self.log
    }

    pub fn swarm(&self) -> &Swarm<CadenceNode<S, C>> {
        &self.swarm
    }

    pub fn swarm_mut(&mut self) -> &mut Swarm<CadenceNode<S, C>> {
        &mut self.swarm
    }
}

// Per-node finalization histories, shared with the output handlers of the
// nodes.
type PerNodeLog = Arc<Mutex<Vec<(Timestamp, Slot)>>>;
pub struct FinalizationLog(HashMap<NodeId, PerNodeLog>);

impl FinalizationLog {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn allocate(&mut self, node: NodeId) -> impl FnMut(Timestamp, Slot) + Send + 'static {
        let log = self.0.entry(node).or_default().clone();
        move |at: Timestamp, slot: Slot| log.lock().expect("not poisoned").push((at, slot))
    }

    pub fn get_finalization_times(&self, node: NodeId) -> Vec<Timestamp> {
        self.node_log(node)
            .lock()
            .expect("not poisoned")
            .iter()
            .map(|(at, _)| *at)
            .collect()
    }

    pub fn get_finalized_slots(&self, node: NodeId) -> Vec<Slot> {
        let log = self.node_log(node).lock().expect("not poisoned");
        log.iter().map(|(_, slot)| *slot).collect()
    }

    fn node_log(&self, node: NodeId) -> &PerNodeLog {
        self.0.get(&node).expect("unknown node")
    }
}

impl Default for FinalizationLog {
    fn default() -> Self {
        Self::new()
    }
}
