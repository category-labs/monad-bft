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

use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};

use chorus::{
    CadenceRuntime, Conductor, DataAvailability, FinalizationObserver, NodeRuntime, Runtime,
    SlotConsensus, SlotManager,
    types::{NodeId, Slot, Timestamp},
};
use monad_mcp_chorus::stub as chorus;
use monad_sim::{RunOutcome, StepLabel};
use monad_sim_swarm::{Network, Swarm};

use crate::node::{SimNode, time_of, to_timestamp};

pub struct CadenceSwarmBuilder<M> {
    seed: u64,
    network: Network<NodeId, M>,
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

    pub fn set_network(&mut self, network: Network<NodeId, M>) {
        self.network = network;
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

    /// A node whose data-availability layer is `da`. The returned log keeps
    /// the certificate behind each finalization, which the swarm's own log
    /// does not.
    pub fn add_node_with_da<S, C, A>(
        &mut self,
        id: NodeId,
        conductor: C,
        slot_config: S::Config,
        slot_context: S::Context,
        da: A,
    ) -> SlotLog<S::FinalizationData>
    where
        M: Clone + 'static,
        S: SlotConsensus + 'static,
        C: Conductor + 'static,
        A: DataAvailability<S> + 'static,
        NodeRuntime<S, C, A>: Runtime<M>,
    {
        let slot_manager = SlotManager::new(slot_config, slot_context);
        let mut runtime = CadenceRuntime::<S, C>::new(slot_manager, conductor);

        let mut shared = self.track_logs.then(|| {
            self.log
                .allocate::<S::OptimisticCommitData, S::FinalizationData>(id)
        });
        let log = SlotLog::default();
        let recorder = log.clone();
        runtime.on_finalization(
            move |at: Timestamp, slot: Slot, data: &S::FinalizationData| {
                if let Some(shared) = &mut shared {
                    shared.handle_finalization(at, slot, data);
                }
                recorder.borrow_mut().push((at, slot, data.clone()));
            },
        );

        self.add_generic_node(id, NodeRuntime::new(runtime, da));
        log
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
        CadenceSwarm {
            swarm: build_sim_swarm(self.seed, self.network, self.nodes),
            log: self.log,
        }
    }
}

/// Wire nodes into a simulated network and schedule each node's init step
pub(crate) fn build_sim_swarm<M: Clone + 'static>(
    seed: u64,
    network: Network<NodeId, M>,
    nodes: Vec<(NodeId, SimNode<M>)>,
) -> Swarm<SimNode<M>> {
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

/// One node's finalizations with the data behind them
pub type SlotLog<FD> = Rc<RefCell<Vec<(Timestamp, Slot, FD)>>>;

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
