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

use cadence::{
    CadenceOutput, CadenceRuntime, CadenceWireMsg, Conductor, Runtime, SlotConsensus, SlotManager,
    slot::chorus::ChorusDAEvent,
};
use monad_mcp_cadence::stub as cadence;
use monad_mcp_common::{
    Slot, Timestamp,
    stub::{NodeId, ValidatorData},
};
use monad_mcp_proposing::stub::{FixedLeadPlanner, ProposingRuntime};
use monad_sim::{RunOutcome, StepLabel};
use monad_sim_swarm::{Network, Swarm};

use crate::{
    da::MockDa,
    node::{ProposerHarness, SimMessage, SimNode, time_of, to_timestamp},
    proposing::MockSource,
};

type CadenceNode<S, C> = SimNode<CadenceRuntime<S, C>>;

pub struct CadenceSwarmBuilder<S: SlotConsensus, C: Conductor> {
    seed: u64,
    network: Network<NodeId, SimMessage<CadenceWireMsg<S, C>>>,
    nodes: Vec<(NodeId, CadenceNode<S, C>)>,
    validators: Option<Arc<ValidatorData>>,
    track_logs: bool,
    log: FinalizationLog,
}

impl<S: SlotConsensus, C: Conductor> CadenceSwarmBuilder<S, C> {
    pub fn new() -> Self {
        Self {
            seed: 0,
            network: Network::default(),
            nodes: Vec::new(),
            validators: None,
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

    /// The table that resolves a recipient named by its validator index.
    pub fn set_validators(&mut self, validators: Arc<ValidatorData>) {
        self.validators = Some(validators);
    }

    // the runtime of each node needs the table
    fn validators(&self) -> Arc<ValidatorData> {
        let validators = self.validators.clone();
        validators.expect("set_validators comes before a node")
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
        let runtime = CadenceRuntime::new(slot_manager, conductor, self.validators());

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
    /// layer (the node reports availability from it, so consensus sees what
    /// the planner submits and the network delivers).
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
        planner: FixedLeadPlanner,
        da: MockDa,
        mut observer: impl FnMut(Timestamp, Slot, &S::FinalizationData) + Send + 'static,
    ) where
        S: SlotConsensus<LocalIn = ChorusDAEvent> + 'static,
        C: 'static,
        C::Message: Clone,
        CadenceRuntime<S, C>: Send,
        CadenceWireMsg<S, C>: Send,
    {
        let proposing = ProposingRuntime::new(planner, MockSource::new(id));

        let slot_manager = SlotManager::new(slot_config, slot_context);
        let runtime = CadenceRuntime::new(slot_manager, conductor, self.validators());

        let mut record = self.track_logs.then(|| self.log.allocate(id));
        let on_output = move |now: Timestamp, output: CadenceOutput<S>| match output {
            CadenceOutput::Lifecycle(_) => {}
            CadenceOutput::Local(..) | CadenceOutput::CommitOptimistic(_) => {}
            CadenceOutput::Finalized(slot, data) => {
                if let Some(record) = &mut record {
                    record(now, slot);
                }
                observer(now, slot, &data);
            }
        };

        let harness = ProposerHarness {
            proposing,
            da,
            lifecycle: |output| match output {
                CadenceOutput::Lifecycle(event) => Some(*event),
                _ => None,
            },
            da_input: |slot, event| (slot, event),
            seal_alarm: None,
        };
        let node = SimNode::with_proposer(id, runtime, on_output, harness);
        self.nodes.push((id, node));
    }

    pub fn build(mut self) -> CadenceSwarm<S, C>
    where
        S: 'static,
        C: 'static,
        C::Message: Clone,
        CadenceRuntime<S, C>: Send,
        CadenceWireMsg<S, C>: Send,
    {
        if let Some(validators) = &self.validators {
            for (_, node) in &mut self.nodes {
                node.set_validators(validators.clone());
            }
        }
        CadenceSwarm {
            swarm: build_sim_swarm(self.seed, self.network, self.nodes),
            log: self.log,
        }
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
