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

//! The slot fallback on a simulated network, with no Chorus around it. Each
//! node is one [`SlotFallback`] behind the stock [`MvbaRuntime`], observed by a
//! [`SlotFallbackRuntime`]. Which certificate decided a node is read off the
//! wire: a `DummyDecision` or a `CommitQc` broadcast in the same step as the
//! decision

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use cadence::{
    Outbound, Runtime,
    slot::fallback::{
        EnterFallbackCert, Entry, Metablock, MvbaRuntime,
        dummy_view::DummyViewMessage,
        monad_mvba::MvbaMessage,
        slot_fallback::{CertifiedMetablock, SlotCommitProof, SlotFallback, SlotFallbackMessage},
    },
};
use monad_mcp_cadence::stub as cadence;
use monad_mcp_common::{Lanes, Timestamp, TimestampDelta, stub::NodeId};
use monad_sim::{RunOutcome, StepLabel, Time};
use monad_sim_swarm::{Network, Swarm};

use crate::{
    node::{SimMessage, SimNode, time_of},
    swarm::build_sim_swarm,
};

type Inner = MvbaRuntime<SlotFallback, Metablock>;
type Input = (Metablock, Option<EnterFallbackCert>);
type SlotFallbackNode = SimNode<SlotFallbackRuntime>;
/// A message and its destination, `None` for a broadcast
type Addressed = (Option<NodeId>, SlotFallbackMessage);

/// Rewrites one message a node is about to send, into what it actually sends:
/// nothing drops it, several forge extra messages beside or instead of it
pub type OutboundHook = dyn FnMut(Time, SlotFallbackMessage) -> Vec<SlotFallbackMessage> + Send;

/// One message a node handed to the network
#[derive(Clone, Debug)]
pub struct Sent {
    pub at: Time,
    /// `None` for a broadcast
    pub to: Option<NodeId>,
    pub message: SlotFallbackMessage,
    /// Sent again by the resend loop, not by the protocol
    pub resent: bool,
}

/// One message a node took off the network
#[derive(Clone, Debug)]
pub struct Received {
    pub at: Time,
    pub from: NodeId,
    pub message: SlotFallbackMessage,
}

/// What one node decided, when, and the certificate it broadcast for it
#[derive(Clone, Debug)]
pub struct SlotDecision {
    pub at: Time,
    pub block: Metablock,
    /// `None` only if the node decided without broadcasting a certificate
    pub proof: Option<SlotCommitProof>,
}

#[derive(Default)]
struct NodeLog {
    sent: Vec<Sent>,
    received: Vec<Received>,
    first: Option<SlotDecision>,
    conflict_decision: bool,
}

type SharedLog = Arc<Mutex<NodeLog>>;

/// A node's [`MvbaRuntime`], observed. With a resend interval it delivers
/// every message the node sent again at that interval until the node decides,
/// and from then on only the certificate it decided by -- the `Repeater`'s
/// contract, kept out of protocol code. Silent after deciding, it hands nothing
/// more to the state machine, as Chorus does once it finalizes
pub struct SlotFallbackRuntime {
    inner: Inner,
    hook: Option<Box<OutboundHook>>,
    resend: Option<TimestampDelta>,
    silent_after_decide: bool,
    next_resend: Option<Timestamp>,
    history: Vec<Addressed>,
    certificate: Option<(SlotCommitProof, Addressed)>,
    decided: bool,
    outbox: VecDeque<Outbound<SlotFallbackMessage>>,
    outputs: VecDeque<SlotDecision>,
    log: SharedLog,
}

impl SlotFallbackRuntime {
    fn new(
        inner: Inner,
        hook: Option<Box<OutboundHook>>,
        resend: Option<TimestampDelta>,
        silent_after_decide: bool,
        log: SharedLog,
    ) -> Self {
        Self {
            inner,
            hook,
            resend,
            silent_after_decide,
            next_resend: None,
            history: Vec::new(),
            certificate: None,
            decided: false,
            outbox: VecDeque::new(),
            outputs: VecDeque::new(),
            log,
        }
    }

    // moves what the state machine produced into this wrapper's queues
    fn pull(&mut self, now: Timestamp) {
        while let Some(outbound) = self.inner.poll_msg() {
            let outbound = match outbound {
                Outbound::Broadcast(message) => (None, message),
                Outbound::Unicast(to, message) => (Some(to), message),
            };
            if self.certificate.is_none()
                && let Some(proof) = certificate_of(&outbound.1)
            {
                self.certificate = Some((proof, outbound.clone()));
            }
            if let (Some(interval), None) = (self.resend, self.next_resend) {
                self.next_resend = Some(now + interval);
            }
            self.history.push(outbound.clone());
            self.emit(now, outbound, false);
        }
        while let Some(block) = self.inner.poll_output() {
            self.decided = true;
            self.outputs.push_back(SlotDecision {
                at: time_of(now),
                block,
                proof: self.certificate.as_ref().map(|(proof, _)| proof.clone()),
            });
        }
    }

    fn emit(&mut self, now: Timestamp, (to, message): Addressed, resent: bool) {
        let at = time_of(now);
        let messages = match &mut self.hook {
            Some(hook) => hook(at, message),
            None => vec![message],
        };
        let mut log = self.log.lock().expect("not poisoned");
        for message in messages {
            log.sent.push(Sent {
                at,
                to,
                message: message.clone(),
                resent,
            });
            self.outbox.push_back(match to {
                None => Outbound::Broadcast(message),
                Some(to) => Outbound::Unicast(to, message),
            });
        }
    }

    fn resend_due(&mut self, now: Timestamp) {
        let (Some(interval), Some(due)) = (self.resend, self.next_resend) else {
            return;
        };
        if due > now {
            return;
        }
        let again: Vec<_> = match (&self.certificate, self.decided) {
            (Some((_, certificate)), true) => vec![certificate.clone()],
            (None, true) => Vec::new(),
            (_, false) => self.history.clone(),
        };
        for outbound in again {
            self.emit(now, outbound, true);
        }
        self.next_resend = Some(now + interval);
    }

    fn silent(&self) -> bool {
        self.silent_after_decide && self.decided
    }
}

/// What a Pre-Prepare carries, as the scenarios read it
#[derive(Clone, Debug)]
pub struct Proposal {
    pub view: u64,
    /// Carries `J = TC_{v-1}`, which every view but the first does
    pub justified: bool,
    pub value: CertifiedMetablock,
}

// The readers below look inside MVBA messages whose payload types the core
// keeps `pub(crate)`; a pattern cannot even name those variants from here.

pub fn proposal(message: &SlotFallbackMessage) -> Option<Proposal> {
    let SlotFallbackMessage::Mvba(MvbaMessage::PrePrepare(msg)) = message else {
        return None;
    };
    Some(Proposal {
        view: msg.view.get(),
        justified: msg.tc.is_some(),
        value: msg.value.clone(),
    })
}

/// A Prepare's view and entries
pub fn prepare(message: &SlotFallbackMessage) -> Option<(u64, Lanes<Entry>)> {
    let SlotFallbackMessage::Mvba(MvbaMessage::Prepare(msg)) = message else {
        return None;
    };
    Some((msg.scope.view.get(), msg.vote.0.clone()))
}

pub fn commit_view(message: &SlotFallbackMessage) -> Option<u64> {
    let SlotFallbackMessage::Mvba(MvbaMessage::Commit(msg)) = message else {
        return None;
    };
    Some(msg.scope.view.get())
}

pub fn timeout_view(message: &SlotFallbackMessage) -> Option<u64> {
    let SlotFallbackMessage::Mvba(MvbaMessage::Timeout(msg)) = message else {
        return None;
    };
    Some(msg.view().get())
}

/// The view a standalone `Tc` certifies as left
pub fn tc_view(message: &SlotFallbackMessage) -> Option<u64> {
    let SlotFallbackMessage::Mvba(MvbaMessage::Tc(tc)) = message else {
        return None;
    };
    Some(tc.view.get())
}

/// A justified Pre-Prepare re-signed by `leader` around `value`, as a
/// Byzantine leader would send it; any other message is returned as is
pub fn forge_pre_prepare(
    message: SlotFallbackMessage,
    value: CertifiedMetablock,
    leader: NodeId,
) -> SlotFallbackMessage {
    match message {
        SlotFallbackMessage::Mvba(MvbaMessage::PrePrepare(msg)) if msg.tc.is_some() => {
            let forged = cadence::slot::fallback::monad_mvba::PrePrepareMsg::new_signed(
                msg.slot,
                msg.view,
                value,
                msg.tc,
                &leader.keypair(),
            );
            SlotFallbackMessage::Mvba(MvbaMessage::PrePrepare(forged))
        }
        message => message,
    }
}

fn certificate_of(message: &SlotFallbackMessage) -> Option<SlotCommitProof> {
    match message {
        SlotFallbackMessage::DummyView(DummyViewMessage::DummyDecision { qc, .. }) => {
            Some(SlotCommitProof::Dummy(qc.clone()))
        }
        SlotFallbackMessage::Mvba(MvbaMessage::CommitQc(qc)) => {
            Some(SlotCommitProof::Mvba(qc.clone()))
        }
        _ => None,
    }
}

impl Runtime for SlotFallbackRuntime {
    type Message = SlotFallbackMessage;
    type LocalIn = Input;
    type LocalOut = SlotDecision;

    fn receive(&mut self, now: Timestamp, sender: NodeId, message: SlotFallbackMessage) {
        self.log
            .lock()
            .expect("not poisoned")
            .received
            .push(Received {
                at: time_of(now),
                from: sender,
                message: message.clone(),
            });
        if self.silent() {
            return;
        }
        self.inner.receive(now, sender, message);
        self.pull(now);
    }

    fn poll_msg(&mut self) -> Option<Outbound<SlotFallbackMessage>> {
        self.outbox.pop_front()
    }

    fn handle_input(&mut self, now: Timestamp, input: Input) {
        self.inner.handle_input(now, input);
        self.pull(now);
    }

    fn poll_output(&mut self) -> Option<SlotDecision> {
        self.outputs.pop_front()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.resend_due(now);
        if self.silent() {
            return;
        }
        self.inner.handle_due(now);
        self.pull(now);
    }

    fn next_due(&self) -> Option<Timestamp> {
        if self.silent() {
            return self.next_resend;
        }
        match (self.inner.next_due(), self.next_resend) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}

/// Assembles slot fallback instances over one network, each with a start of
/// its own
pub struct SlotFallbackSwarmBuilder {
    seed: u64,
    network: Network<NodeId, SimMessage<SlotFallbackMessage>>,
    resend: Option<TimestampDelta>,
    silent_after_decide: bool,
    nodes: Vec<(NodeId, SlotFallback)>,
    hooks: HashMap<NodeId, Box<OutboundHook>>,
    inputs: BTreeMap<NodeId, Metablock>,
    starts: Vec<(NodeId, Time, Input)>,
}

impl SlotFallbackSwarmBuilder {
    pub fn new() -> Self {
        Self {
            seed: 0,
            network: Network::default(),
            resend: None,
            silent_after_decide: false,
            nodes: Vec::new(),
            hooks: HashMap::new(),
            inputs: BTreeMap::new(),
            starts: Vec::new(),
        }
    }

    pub fn set_seed(&mut self, seed: u64) -> &mut Self {
        self.seed = seed;
        self
    }

    pub fn set_network(
        &mut self,
        network: Network<NodeId, SimMessage<SlotFallbackMessage>>,
    ) -> &mut Self {
        self.network = network;
        self
    }

    /// Switch on the resend loop for every node, at `interval`
    pub fn set_resend(&mut self, interval: Duration) -> &mut Self {
        let nanos = u64::try_from(interval.as_nanos()).expect("resend interval fits in u64 nanos");
        self.resend = Some(TimestampDelta::from_nanos(nanos));
        self
    }

    /// Once a node decides, drop what it receives and stop its timers; the
    /// resend loop still repeats the certificate
    pub fn set_silent_after_decide(&mut self) -> &mut Self {
        self.silent_after_decide = true;
        self
    }

    /// Route everything `id` sends through `hook` first
    pub fn set_outbound(
        &mut self,
        id: NodeId,
        hook: impl FnMut(Time, SlotFallbackMessage) -> Vec<SlotFallbackMessage> + Send + 'static,
    ) -> &mut Self {
        self.hooks.insert(id, Box::new(hook));
        self
    }

    /// `start` is when this node hands its input to its state machine; until
    /// then it already receives, and retains, what the others send
    pub fn add_node(
        &mut self,
        id: NodeId,
        mvba: SlotFallback,
        block: Metablock,
        cert: Option<EnterFallbackCert>,
        start: Time,
    ) -> &mut Self {
        self.inputs.insert(id, block.clone());
        self.starts.push((id, start, (block, cert)));
        self.nodes.push((id, mvba));
        self
    }

    pub fn build(mut self) -> SlotFallbackSwarm {
        let mut logs = BTreeMap::new();
        let mut nodes = Vec::new();
        for (id, mvba) in self.nodes {
            let log = SharedLog::default();
            logs.insert(id, log.clone());
            let runtime = SlotFallbackRuntime::new(
                MvbaRuntime::new(mvba),
                self.hooks.remove(&id),
                self.resend,
                self.silent_after_decide,
                log.clone(),
            );
            let on_output = move |_: Timestamp, decision: SlotDecision| {
                let mut log = log.lock().expect("not poisoned");
                match &log.first {
                    None => log.first = Some(decision),
                    Some(first) => {
                        if first.block != decision.block {
                            log.conflict_decision = true;
                        }
                    }
                }
            };
            nodes.push((id, SimNode::new(id, runtime, on_output)));
        }

        let mut swarm = build_sim_swarm(self.seed, self.network, nodes);
        for (id, start, input) in self.starts {
            let handle = swarm.handle(&id).expect("node just built");
            swarm
                .sim()
                .schedule(handle, start, StepLabel::source("input"), |node, ctx| {
                    node.handle_input(input, ctx)
                });
        }
        SlotFallbackSwarm {
            swarm,
            inputs: self.inputs,
            logs,
        }
    }
}

impl Default for SlotFallbackSwarmBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A built swarm: run control, plus each node's decision and message trace
pub struct SlotFallbackSwarm {
    swarm: Swarm<SlotFallbackNode>,
    inputs: BTreeMap<NodeId, Metablock>,
    logs: BTreeMap<NodeId, SharedLog>,
}

impl SlotFallbackSwarm {
    pub fn swarm_mut(&mut self) -> &mut Swarm<SlotFallbackNode> {
        &mut self.swarm
    }

    pub fn now(&self) -> Time {
        self.swarm.simulation().now()
    }

    /// Every node's input, including nodes since removed
    pub fn inputs(&self) -> &BTreeMap<NodeId, Metablock> {
        &self.inputs
    }

    /// What each node in the swarm has decided so far
    pub fn decisions(&self) -> BTreeMap<NodeId, SlotDecision> {
        self.swarm
            .node_ids()
            .into_iter()
            .filter_map(|id| self.decision_of(&id).map(|decision| (id, decision)))
            .collect()
    }

    pub fn decision_of(&self, node: &NodeId) -> Option<SlotDecision> {
        self.logs
            .get(node)?
            .lock()
            .expect("not poisoned")
            .first
            .clone()
    }

    /// Whether `node` ever reported a second, different decision
    pub fn conflicted(&self, node: &NodeId) -> bool {
        self.logs
            .get(node)
            .is_some_and(|log| log.lock().expect("not poisoned").conflict_decision)
    }

    /// Everything `node` handed to the network, in order
    pub fn sent(&self, node: &NodeId) -> Vec<Sent> {
        self.logs
            .get(node)
            .map(|log| log.lock().expect("not poisoned").sent.clone())
            .unwrap_or_default()
    }

    /// Everything `node` took off the network, in order
    pub fn received(&self, node: &NodeId) -> Vec<Received> {
        self.logs
            .get(node)
            .map(|log| log.lock().expect("not poisoned").received.clone())
            .unwrap_or_default()
    }

    /// Whether every node in the swarm has decided
    pub fn all_decided(&self) -> bool {
        self.swarm
            .node_ids()
            .iter()
            .all(|id| self.decision_of(id).is_some())
    }

    /// Run until every node has decided or `deadline` passes; whether all did
    pub fn run_until_all_decided(&mut self, deadline: Time) -> bool {
        self.run_until_or(deadline, Self::all_decided);
        self.all_decided()
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
