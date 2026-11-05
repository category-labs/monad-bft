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

//! The fallback path's agreement protocol on a simulated network, with no
//! Chorus around it. Each node is one [`MonadMvba`] whose fallback input is
//! pre-formed -- or absent, for a validator the fast path left nothing for
//!
//! Broadcasts reach the sender too, which is what supplies a node its own vote:
//! the state machine counts every vote off the wire, its own included

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use cadence::slot::fallback::{EnterFallbackCert, Metablock, MvbaRuntime, monad_mvba};
use monad_mcp_cadence::stub as cadence;
use monad_mcp_common::{Timestamp, stub::NodeId};
use monad_sim::{RunOutcome, StepLabel, Time};
use monad_sim_swarm::{Network, Swarm};

use crate::{
    node::{SimMessage, SimNode, time_of},
    swarm::build_sim_swarm,
};

// the `V = Metablock` instantiation, the one the fallback path runs
pub type MonadMvba = monad_mvba::MonadMvba<Metablock, EnterFallbackCert>;
pub type Message = monad_mvba::MvbaMessage<Metablock, EnterFallbackCert>;

type MvbaNode = SimNode<MvbaRuntime<MonadMvba, Metablock>>;
type Input = (Metablock, Option<EnterFallbackCert>);

/// What one node decided, and when
#[derive(Clone, Debug)]
pub struct Decision {
    pub at: Time,
    pub block: Metablock,
}

/// What one node reported: its first decision, and whether a later report
/// disagreed with it, which integrity forbids
#[derive(Default)]
struct DecisionState {
    first: Option<Decision>,
    conflict_decision: bool,
}

/// Per-node decisions, shared with the output handlers of the nodes
#[derive(Default)]
struct DecisionLog(HashMap<NodeId, Arc<Mutex<DecisionState>>>);

impl DecisionLog {
    fn new() -> Self {
        Self::default()
    }

    fn record_decision(&mut self, node: NodeId) -> impl FnMut(Timestamp, Metablock) + 'static {
        let state = self.0.entry(node).or_default().clone();
        move |at: Timestamp, block: Metablock| {
            let mut state = state.lock().expect("not poisoned");
            match &state.first {
                None => {
                    state.first = Some(Decision {
                        at: time_of(at),
                        block,
                    })
                }
                Some(first) => {
                    if first.block != block {
                        state.conflict_decision = true;
                    }
                }
            }
        }
    }

    fn decision_of(&self, node: &NodeId) -> Option<Decision> {
        self.0
            .get(node)?
            .lock()
            .expect("not poisoned")
            .first
            .clone()
    }

    fn conflicted(&self, node: &NodeId) -> bool {
        self.0
            .get(node)
            .is_some_and(|state| state.lock().expect("not poisoned").conflict_decision)
    }
}

/// Assembles MVBA instances over one network, each with a start of its own:
/// the four inputs can be handed over at four different times
pub struct MvbaSwarmBuilder {
    seed: u64,
    network: Network<NodeId, SimMessage<Message>>,
    nodes: Vec<(NodeId, MvbaNode)>,
    inputs: BTreeMap<NodeId, Metablock>,
    starts: Vec<(NodeId, Time, Input)>,
    log: DecisionLog,
}

impl MvbaSwarmBuilder {
    pub fn new() -> Self {
        Self {
            seed: 0,
            network: Network::default(),
            nodes: Vec::new(),
            inputs: BTreeMap::new(),
            starts: Vec::new(),
            log: DecisionLog::new(),
        }
    }

    pub fn set_seed(&mut self, seed: u64) -> &mut Self {
        self.seed = seed;
        self
    }

    pub fn set_network(&mut self, network: Network<NodeId, SimMessage<Message>>) -> &mut Self {
        self.network = network;
        self
    }

    /// `start` is when this node hands its input to its state machine
    pub fn add_node(
        &mut self,
        id: NodeId,
        mvba: MonadMvba,
        block: Metablock,
        cert: Option<EnterFallbackCert>,
        start: Time,
    ) -> &mut Self {
        self.inputs.insert(id, block.clone());
        self.starts.push((id, start, (block, cert)));
        self.push(id, mvba)
    }

    /// Add a validator with no fallback input, which therefore only listens.
    /// Having nothing to hand over, it needs no start
    pub fn add_listener(&mut self, id: NodeId, mvba: MonadMvba) -> &mut Self {
        self.push(id, mvba)
    }

    fn push(&mut self, id: NodeId, mvba: MonadMvba) -> &mut Self {
        let runtime = MvbaRuntime::new(mvba);
        let on_output = self.log.record_decision(id);
        self.nodes.push((id, SimNode::new(id, runtime, on_output)));
        self
    }

    pub fn build(self) -> MvbaSwarm {
        let mut swarm = build_sim_swarm(self.seed, self.network, self.nodes);
        for (id, start, input) in self.starts {
            let handle = swarm.handle(&id).expect("node just built");
            swarm
                .sim()
                .schedule(handle, start, StepLabel::source("input"), |node, ctx| {
                    node.handle_input(input, ctx)
                });
        }
        MvbaSwarm {
            swarm,
            inputs: self.inputs,
            log: self.log,
        }
    }
}

impl Default for MvbaSwarmBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A built swarm: run control, plus the decisions reached and inputs started from
pub struct MvbaSwarm {
    swarm: Swarm<MvbaNode>,
    inputs: BTreeMap<NodeId, Metablock>,
    log: DecisionLog,
}

impl MvbaSwarm {
    pub fn swarm_mut(&mut self) -> &mut Swarm<MvbaNode> {
        &mut self.swarm
    }

    /// Every node's input, including nodes since removed; listeners have none
    pub fn inputs(&self) -> &BTreeMap<NodeId, Metablock> {
        &self.inputs
    }

    /// What each node in the swarm has decided so far
    pub fn decisions(&self) -> BTreeMap<NodeId, Decision> {
        self.swarm
            .node_ids()
            .into_iter()
            .filter_map(|id| self.log.decision_of(&id).map(|decision| (id, decision)))
            .collect()
    }

    pub fn decision_of(&self, node: &NodeId) -> Option<Decision> {
        self.log.decision_of(node)
    }

    /// Whether every node in the swarm has decided
    pub fn all_decided(&self) -> bool {
        self.swarm
            .node_ids()
            .iter()
            .all(|id| self.log.decision_of(id).is_some())
    }

    /// Whether any node ever reported a second, different decision
    pub fn any_conflicted(&self) -> bool {
        self.swarm
            .node_ids()
            .iter()
            .any(|id| self.log.conflicted(id))
    }

    /// Run until every node has decided or `deadline` passes; whether all did
    pub fn run_until_all_decided(&mut self, deadline: Time) -> bool {
        loop {
            if self.all_decided() {
                return true;
            }
            match self.swarm.simulation().peek_time() {
                Some(at) if at <= deadline => {}
                // nothing left to run, or nothing left before the deadline
                _ => return self.all_decided(),
            }
            if self.swarm.sim().next_step().is_none() {
                return self.all_decided();
            }
        }
    }

    /// Run until `done` holds, giving up at `deadline`
    pub fn run_until_or(
        &mut self,
        deadline: Time,
        mut done: impl FnMut(&Self) -> bool,
    ) -> RunOutcome {
        loop {
            if done(self) {
                return RunOutcome::Stopped;
            }
            match self.swarm.simulation().peek_time() {
                Some(at) if at <= deadline => {}
                _ => return RunOutcome::Drained,
            }
            if self.swarm.sim().next_step().is_none() {
                return RunOutcome::Drained;
            }
        }
    }
}

/// A time `millis` after the start of the simulation
pub fn at_millis(millis: u64) -> Time {
    Time(0) + Duration::from_millis(millis)
}
