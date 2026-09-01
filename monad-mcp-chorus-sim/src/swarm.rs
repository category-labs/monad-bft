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
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use chorus::{
    CadenceRuntime, Conductor, FinalizationObserver, Runtime, SlotConsensus, SlotManager,
    proposing::ProposalPlanner,
    types::{NodeId, Slot, Timestamp},
};
use monad_mcp_chorus::stub as chorus;
use monad_sim::{RunOutcome, StepLabel};
use monad_sim_swarm::{Network, Swarm};

use crate::{
    da::MockDa,
    node::{ProposerHarness, SimMessage, SimNode, time_of, to_timestamp},
};

pub struct CadenceSwarmBuilder<M> {
    seed: u64,
    network: Network<NodeId, SimMessage<M>>,
    nodes: Vec<(NodeId, SimNode<M>)>,
    track_logs: bool,
    log: FinalizationLog,
}

impl<M> CadenceSwarmBuilder<M> {
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
        CadenceRuntime<S, C>: Runtime<M>,
    {
        let slot_manager = SlotManager::new(slot_config, slot_context);
        let mut runtime = CadenceRuntime::<S, C>::new(slot_manager, conductor);

        if self.track_logs {
            runtime.on_finalization(self.log.allocate(id));
        }

        self.add_generic_node(id, runtime);
    }

    /// Like [`Self::add_node`], for a node that also runs the proposal
    /// machinery: `planner` decides when to seal, `da` is the node's mock DA
    /// layer (the same instance handed to the slot context, so consensus
    /// reads what the planner submits and the network delivers).
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
        S: SlotConsensus + 'static,
        C: Conductor + 'static,
        CadenceRuntime<S, C>: Runtime<M>,
        O: FinalizationObserver<S::OptimisticCommitData, S::FinalizationData> + 'static,
    {
        let planner = Rc::new(RefCell::new(planner));
        let facts = PlannerFacts(planner.clone());

        let slot_manager = SlotManager::new(slot_config, slot_context);
        let mut runtime = CadenceRuntime::<S, C>::new(slot_manager, conductor);
        if self.track_logs {
            runtime.on_finalization((self.log.allocate(id), (facts, observer)));
        } else {
            runtime.on_finalization((facts, observer));
        }

        let harness = ProposerHarness { planner, da };
        let node = SimNode::with_proposer(id, runtime, harness);
        self.nodes.push((id, node));
    }

    pub fn add_generic_node(&mut self, id: NodeId, runtime: impl Runtime<M> + 'static)
    where
        M: Clone + 'static,
    {
        let node = SimNode::new(id, runtime);
        self.nodes.push((id, node));
    }

    pub fn build(self) -> CadenceSwarm<M>
    where
        M: Clone + 'static,
    {
        let mut swarm = Swarm::build(self.seed, self.network, self.nodes);
        for id in swarm.node_ids() {
            let handle = swarm.handle(&id).expect("node just built");
            swarm.sim().schedule(
                handle,
                time_of(Timestamp::GENESIS),
                StepLabel::source("init"),
                |node, ctx| node.init(ctx),
            );
        }
        CadenceSwarm {
            swarm,
            log: self.log,
        }
    }
}

impl<M> Default for CadenceSwarmBuilder<M> {
    fn default() -> Self {
        Self::new()
    }
}

// A built swarm: run control in cadence ticks, plus the finalization
// logs collected from the nodes added with tracking enabled.
pub struct CadenceSwarm<M>
where
    M: Clone + 'static,
{
    swarm: Swarm<SimNode<M>>,
    log: FinalizationLog,
}

impl<M> CadenceSwarm<M>
where
    M: Clone + 'static,
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

    pub fn swarm(&self) -> &Swarm<SimNode<M>> {
        &self.swarm
    }

    pub fn swarm_mut(&mut self) -> &mut Swarm<SimNode<M>> {
        &mut self.swarm
    }
}

// Forwards the runtime facts the proposal planner consumes; the planner's
// resulting requests are executed by the owning SimNode (see node.rs).
struct PlannerFacts(Rc<RefCell<ProposalPlanner>>);

impl<OD, FD> FinalizationObserver<OD, FD> for PlannerFacts {
    fn handle_finalization(&mut self, _now: Timestamp, _slot: Slot, _data: &FD) {}

    fn handle_slots_opened(&mut self, _now: Timestamp, slots: &BTreeMap<Slot, Timestamp>) {
        self.0.borrow_mut().handle_slots_opened(slots);
    }

    fn handle_chain_advance(&mut self, now: Timestamp, cap: Slot) {
        self.0.borrow_mut().handle_chain_advance(now, cap);
    }
}

// Per-node finalization histories, shared with the observers planted in
// the runtimes.
type PerNodeLog = Rc<RefCell<Vec<(Timestamp, Slot)>>>;
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
        move |at: Timestamp, slot: Slot, _: &FD| log.borrow_mut().push((at, slot))
    }

    pub fn get_finalization_times(&self, node: NodeId) -> Vec<Timestamp> {
        self.node_log(node)
            .borrow()
            .iter()
            .map(|(at, _)| *at)
            .collect()
    }

    pub fn get_finalized_slots(&self, node: NodeId) -> Vec<Slot> {
        let log = self.node_log(node).borrow();
        log.iter().map(|(_, slot)| *slot).collect()
    }

    fn node_log(&self, node: NodeId) -> &Rc<RefCell<Vec<(Timestamp, Slot)>>> {
        self.0.get(&node).expect("unknown node")
    }
}

impl Default for FinalizationLog {
    fn default() -> Self {
        Self::new()
    }
}
