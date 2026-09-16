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
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use chorus::{
    CadenceRuntime, Conductor, FinalizationObserver, Runtime, SlotConsensus, SlotManager,
    proposing::ProposalPlanner,
    slot::chorus::{ChorusDAEvent, ProposalDAEvent},
    types::{NodeId, Slot, Timestamp},
};
use monad_mcp_chorus::stub as chorus;
use monad_sim::{RunOutcome, StepLabel};
use monad_sim_swarm::{Network, Swarm};

use crate::{
    da::{DaAnnouncement, MockDa},
    node::{ProposerHarness, SimMessage, SimNode, time_of, to_timestamp},
};

pub struct CadenceSwarmBuilder<M, E> {
    seed: u64,
    network: Network<NodeId, SimMessage<M>>,
    nodes: Vec<(NodeId, SimNode<M, E>)>,
    track_logs: bool,
    log: FinalizationLog,
}

impl<M, E> CadenceSwarmBuilder<M, E> {
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

    pub fn set_seed(&mut self, seed: u64) {
        self.seed = seed;
    }

    pub fn track_logs(&mut self, track: bool) {
        self.track_logs = track;
    }

    pub fn add_node<S, C>(
        &mut self,
        id: NodeId,
        conductor: C,
        slot_config: S::Config,
        slot_context: S::Context,
    ) where
        M: Clone + 'static,
        S: SlotConsensus + 'static,
        C: Conductor + 'static,
        E: 'static,
        CadenceRuntime<S, C>: Runtime<M, DAEvent = E>,
    {
        let slot_manager = SlotManager::new(slot_config, slot_context);
        let mut runtime = CadenceRuntime::<S, C>::new(slot_manager, conductor);

        if self.track_logs {
            runtime.on_finalization(self.log.allocate(id));
        }

        self.add_generic_node(id, runtime);
    }

    pub fn add_generic_node(&mut self, id: NodeId, runtime: impl Runtime<M, DAEvent = E> + 'static)
    where
        M: Clone + 'static,
        E: 'static,
    {
        let node = SimNode::new(id, runtime);
        self.nodes.push((id, node));
    }

    pub fn build(self) -> CadenceSwarm<M, E>
    where
        M: Clone + 'static,
        E: 'static,
    {
        CadenceSwarm {
            swarm: build_sim_swarm(self.seed, self.network, self.nodes),
            log: self.log,
        }
    }
}

// The proposal machinery is Chorus-shaped: the planner consumes Chorus
// finalization facts, and the mock DA layer reports availability as Chorus
// DA events.
impl<M> CadenceSwarmBuilder<M, ChorusDAEvent> {
    /// Like [`Self::add_node`], for a node that also runs the proposal
    /// machinery: `planner` decides when to seal, `da` is the node's mock DA
    /// layer (the same instance the node reports availability from, so
    /// consensus sees what the planner submits and the network delivers).
    /// `observer` receives the runtime facts alongside the built-in wiring —
    /// tests use it to record finalization data.
    // one parameter per wired component; a config struct would just move
    // the count elsewhere
    #[allow(clippy::too_many_arguments)]
    pub fn add_proposer_node<S, C, O>(
        &mut self,
        id: NodeId,
        conductor: C,
        slot_config: S::Config,
        slot_context: S::Context,
        planner: ProposalPlanner,
        da: Arc<MockDa>,
        observer: O,
    ) where
        M: Clone + 'static,
        S: SlotConsensus<DAEvent = ChorusDAEvent> + 'static,
        C: Conductor + 'static,
        CadenceRuntime<S, C>: Runtime<M, DAEvent = ChorusDAEvent>,
        O: FinalizationObserver<S::OptimisticCommitData, S::FinalizationData> + Send + 'static,
    {
        let planner = Arc::new(Mutex::new(planner));
        let facts = PlannerFacts(planner.clone());

        let slot_manager = SlotManager::new(slot_config, slot_context);
        let mut runtime = CadenceRuntime::<S, C>::new(slot_manager, conductor);
        if self.track_logs {
            runtime.on_finalization((self.log.allocate(id), (facts, observer)));
        } else {
            runtime.on_finalization((facts, observer));
        }

        // The mock layer makes a proposal available as a whole, so one
        // announcement reports both the signed header and its decoding.
        let da_events = Box::new(|announcement: &DaAnnouncement| {
            let DaAnnouncement { index, header } = announcement;
            vec![
                ChorusDAEvent {
                    j: *index,
                    event: ProposalDAEvent::HeaderSeen(header.clone()),
                },
                ChorusDAEvent {
                    j: *index,
                    event: ProposalDAEvent::Decoded(announcement.root()),
                },
            ]
        });

        let harness = ProposerHarness {
            planner,
            da,
            da_events,
            seal_alarm: None,
        };
        let node = SimNode::with_proposer(id, runtime, harness);
        self.nodes.push((id, node));
    }
}

/// Wire nodes into a simulated network and schedule each node's init step
pub(crate) fn build_sim_swarm<M: Clone + 'static, E: 'static>(
    seed: u64,
    network: Network<NodeId, SimMessage<M>>,
    nodes: Vec<(NodeId, SimNode<M, E>)>,
) -> Swarm<SimNode<M, E>> {
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

impl<M, E> Default for CadenceSwarmBuilder<M, E> {
    fn default() -> Self {
        Self::new()
    }
}

// A built swarm: run control in cadence ticks, plus the finalization
// logs collected from the nodes added with tracking enabled.
pub struct CadenceSwarm<M, E>
where
    M: Clone + 'static,
    E: 'static,
{
    swarm: Swarm<SimNode<M, E>>,
    log: FinalizationLog,
}

impl<M, E> CadenceSwarm<M, E>
where
    M: Clone + 'static,
    E: 'static,
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

    pub fn swarm(&self) -> &Swarm<SimNode<M, E>> {
        &self.swarm
    }

    pub fn swarm_mut(&mut self) -> &mut Swarm<SimNode<M, E>> {
        &mut self.swarm
    }
}

// Forwards the runtime facts the proposal planner consumes; the owning
// SimNode polls the planner's seals and arms its alarm (see node.rs).
struct PlannerFacts(Arc<Mutex<ProposalPlanner>>);

impl<OD, FD> FinalizationObserver<OD, FD> for PlannerFacts {
    fn handle_finalization(&mut self, _now: Timestamp, _slot: Slot, _data: &FD) {}

    fn handle_slots_opened(&mut self, now: Timestamp, slots: &BTreeMap<Slot, Timestamp>) {
        let mut planner = self.0.lock().expect("planner poisoned");
        for (&slot, &deadline) in slots {
            planner.handle_slot_open(now, slot, deadline);
        }
    }

    fn handle_chain_advance(&mut self, now: Timestamp, cap: Slot) {
        self.0
            .lock()
            .expect("planner poisoned")
            .handle_cap_advance(now, cap);
    }
}

// Per-node finalization histories, shared with the observers planted in
// the runtimes.
type PerNodeLog = Arc<Mutex<Vec<(Timestamp, Slot)>>>;
pub struct FinalizationLog(HashMap<NodeId, PerNodeLog>);

impl FinalizationLog {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn allocate<OD, FD>(
        &mut self,
        node: NodeId,
    ) -> impl FinalizationObserver<OD, FD> + 'static {
        let log = self.0.entry(node).or_default().clone();
        move |at: Timestamp, slot: Slot, _: &FD| log.lock().expect("not poisoned").push((at, slot))
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
