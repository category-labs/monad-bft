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

//! A sync `PqNode` as a monad-sim process with the real pq DA, and the
//! checks the scenarios share.

use std::{collections::BTreeMap, net::SocketAddr};

use monad_mcp_chorus::spec::KeyPair as _;
use monad_mcp_node::config::{
    CadenceConfig, DAConfig, LeaderElectionConfig, LedgerConfig, NetworkConfig, NodeConfig,
    ProposalConfig, RepeaterSection, ValidatorConfig,
};
use monad_pq_chorus::{Evidence, PqPath};
use monad_pq_node::{
    PqNodeOutput,
    chorus::{
        Outbound,
        types::{NodeId, ProposalId, ProposalMap, Slot, Timestamp, TimestampDelta},
    },
    component::Component,
    config::{PqNodeConfig, QuantumSection},
    network::{Inbound, Packet},
    wire::{FRAGMENT_MARKER, MAX_PAYLOAD_LEN},
};
use monad_sim::{CancelToken, Ctx, Handle, StepLabel, Time};
use monad_sim_swarm::{Net, SimClient, Swarm};

pub const DELTA: TimestampDelta = TimestampDelta::from_millis(50);
pub const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
pub const LEAD: TimestampDelta = TimestampDelta::from_millis(100);
pub const GENESIS_DEADLINE: Timestamp = Timestamp::from_millis(200);

pub fn time_of(at: Timestamp) -> Time {
    Time(i128::try_from(at.as_nanos()).expect("timestamp fits the sim clock"))
}

pub fn timestamp_of(time: Time) -> Timestamp {
    Timestamp::from_nanos(u128::try_from(time.0).expect("the sim clock is past genesis"))
}

pub fn deadline_of_slot(slot: u64) -> Timestamp {
    GENESIS_DEADLINE
        .checked_add_deltas(SLOT_INTERVAL, slot)
        .expect("in range")
}

// the finalized shape of a slot: which proposal indices committed what
pub type Pattern = ProposalMap<Option<ProposalId>>;

#[derive(Clone, Debug)]
pub struct Finalized {
    pub pattern: Pattern,
    pub path: PqPath,
}

// a node as a monad-sim process, boxed so one swarm can mix node kinds
pub struct SimNode {
    id: NodeId,
    node: Box<dyn Component<Input = Inbound, Output = PqNodeOutput>>,
    finalized: BTreeMap<Slot, Finalized>,
    evidence: Vec<(Slot, Evidence)>,
    fragments_sent: u64,
    // the one live timer step, at the node's last polled next_due
    due_alarm: Option<(Time, CancelToken)>,
    me: Option<Handle<Self>>,
    net: Option<Handle<Net<NodeId, Packet>>>,
}

impl SimNode {
    pub fn new(
        id: NodeId,
        node: impl Component<Input = Inbound, Output = PqNodeOutput> + 'static,
    ) -> Self {
        Self {
            id,
            node: Box::new(node),
            finalized: BTreeMap::new(),
            evidence: Vec::new(),
            fragments_sent: 0,
            due_alarm: None,
            me: None,
            net: None,
        }
    }

    pub fn init(&mut self, ctx: &mut Ctx) {
        let _span = self.span().entered();
        self.process(ctx);
    }

    fn handle_due(&mut self, ctx: &mut Ctx) {
        let _span = self.span().entered();
        self.due_alarm = None;
        self.node.handle_due(timestamp_of(ctx.now()));
        self.process(ctx);
    }

    // turn every pending node output into a sim step, then re-arm the timer
    fn process(&mut self, ctx: &mut Ctx) {
        while let Some(output) = self.node.poll() {
            match output {
                PqNodeOutput::Send(outbound) => self.send(outbound, ctx),
                PqNodeOutput::Finalized(finalized) => {
                    let record = Finalized {
                        pattern: finalized.pids(),
                        path: finalized.path,
                    };
                    let previous = self.finalized.insert(finalized.slot, record);
                    assert!(previous.is_none(), "{:?} finalized twice", finalized.slot);
                }
                PqNodeOutput::Evidence(slot, evidence) => self.evidence.push((slot, evidence)),
            }
        }
        self.arm_due_alarm(ctx);
    }

    fn send(&mut self, outbound: Outbound<Packet>, ctx: &mut Ctx) {
        let from = self.id;
        let net = self.net.expect("node not wired");
        if let Outbound::Broadcast(Packet::Cadence(payload))
        | Outbound::Unicast(_, Packet::Cadence(payload)) = &outbound
        {
            // the sim does not enforce the udp frame, so check it here
            assert!(payload.len() <= MAX_PAYLOAD_LEN, "{} bytes", payload.len());
            if payload.first() == Some(&FRAGMENT_MARKER) {
                self.fragments_sent += 1;
            }
        }
        match outbound {
            Outbound::Broadcast(packet) => {
                ctx.schedule(
                    net,
                    ctx.now(),
                    StepLabel::source("broadcast"),
                    move |net, ctx| net.broadcast(ctx, from, packet),
                );
            }
            Outbound::Unicast(to, packet) => {
                ctx.schedule(
                    net,
                    ctx.now(),
                    StepLabel::source("unicast"),
                    move |net, ctx| net.send(ctx, from, to, packet),
                );
            }
        }
    }

    // one step at next_due; a due already passed fires now
    fn arm_due_alarm(&mut self, ctx: &mut Ctx) {
        let me = self.me.expect("node not wired");
        let at = self.node.next_due().map(|due| time_of(due).max(ctx.now()));
        if self.due_alarm.as_ref().map(|(at, _)| *at) == at {
            return;
        }
        if let Some((_, token)) = self.due_alarm.take() {
            token.cancel();
        }
        if let Some(at) = at {
            let token = ctx.schedule(me, at, StepLabel::source("due"), |node, ctx| {
                node.handle_due(ctx)
            });
            self.due_alarm = Some((at, token));
        }
    }

    pub fn finalized(&self) -> &BTreeMap<Slot, Finalized> {
        &self.finalized
    }

    pub fn evidence(&self) -> &[(Slot, Evidence)] {
        &self.evidence
    }

    pub fn fragments_sent(&self) -> u64 {
        self.fragments_sent
    }

    // every log line of a step names its node
    fn span(&self) -> tracing::Span {
        tracing::info_span!("node", id = u64::from(self.id))
    }
}

impl SimClient for SimNode {
    type Addr = NodeId;
    type Message = Packet;

    fn wire(&mut self, me: Handle<Self>, net: Handle<Net<NodeId, Packet>>) {
        self.me = Some(me);
        self.net = Some(net);
    }

    fn receive(&mut self, from: NodeId, packet: Packet, ctx: &mut Ctx) {
        let _span = self.span().entered();
        // the node loops its own messages back itself
        if from == self.id {
            return;
        }
        let inbound = Inbound { from, packet };
        self.node.handle(timestamp_of(ctx.now()), inbound);
        self.process(ctx);
    }
}

// the local demo values (§8.3) over `validators` stub validators
pub fn node_config(id: u64, validators: u64) -> PqNodeConfig {
    let node_id = NodeId::dummy(id);
    let address: SocketAddr = "127.0.0.1:0".parse().expect("valid");
    let validators = (0..validators)
        .map(|id| ValidatorConfig {
            node_id: NodeId::dummy(id),
            stake: 1,
            chorus_pubkey: NodeId::dummy(id).keypair().pubkey(),
            address,
        })
        .collect();
    let base = NodeConfig {
        node_id,
        proposal_key_pair: monad_mcp_node::da::ProposalKeyPair::dummy(node_id),
        cadence_key_pair: node_id.keypair(),
        validators,
        genesis_deadline: GENESIS_DEADLINE,
        network: NetworkConfig { port: 0 },
        cadence: CadenceConfig {
            delta: DELTA,
            slot_interval: SLOT_INTERVAL,
            ..CadenceConfig::default()
        },
        da: DAConfig::default(),
        proposal: ProposalConfig {
            propose_before_deadline: LEAD,
            withhold_before_deadline: TimestampDelta::from_millis(90),
            max_payload_bytes: Some(2_000),
            ..ProposalConfig::default()
        },
        leader_election: LeaderElectionConfig::default(),
        repeater: Some(RepeaterSection {
            interval: DELTA.checked_mul(4).unwrap(),
            ..RepeaterSection::default()
        }),
        mempool: Default::default(),
        ledger: LedgerConfig {
            dir: "unused-ledger".into(),
        },
    };
    PqNodeConfig {
        base,
        quantum: QuantumSection::default(),
        proposal_key: id,
        has_rpc: false,
    }
}

pub fn build_swarm(
    seed: u64,
    network: monad_sim_swarm::Network<NodeId, Packet>,
    nodes: Vec<(NodeId, SimNode)>,
) -> Swarm<SimNode> {
    let mut swarm = Swarm::build(seed, network, nodes);
    for id in swarm.node_ids() {
        let handle = swarm.handle(&id).expect("node just built");
        let sim = swarm.sim();
        sim.schedule(handle, Time(0), "init", |node, ctx| node.init(ctx));
    }
    swarm
}

pub fn run_through(swarm: &mut Swarm<SimNode>, slots: u64) {
    let margin = SLOT_INTERVAL.checked_mul(10).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(slots) + margin));
}

// every two nodes that finalized a slot finalized the same metablock
pub fn assert_agreement(swarm: &Swarm<SimNode>) {
    let mut patterns: BTreeMap<Slot, (NodeId, Pattern)> = BTreeMap::new();
    for id in swarm.node_ids() {
        let finalized = swarm.with_node(&id, |node| node.finalized().clone());
        for (slot, record) in finalized {
            let (first, seen) = patterns
                .entry(slot)
                .or_insert_with(|| (id, record.pattern.clone()));
            assert_eq!(
                *seen, record.pattern,
                "{id:?} and {first:?} disagree on {slot:?}"
            );
        }
    }
}

// agreement, and every node in `ids` finalized every slot below `horizon`
pub fn assert_consistent_progress(swarm: &Swarm<SimNode>, ids: &[NodeId], horizon: Slot) {
    assert_agreement(swarm);
    for id in ids {
        let finalized = swarm.with_node(id, |node| node.finalized().clone());
        for slot in 0..horizon.0 {
            assert!(
                finalized.contains_key(&Slot(slot)),
                "node {id:?} skipped {:?}; last {:?}",
                Slot(slot),
                finalized.keys().next_back()
            );
        }
    }
}

// agreement, and every node in `ids` reached `horizon`; skipped slots
// (a cap jump) are allowed
pub fn assert_progress(swarm: &Swarm<SimNode>, ids: &[NodeId], horizon: Slot) {
    assert_agreement(swarm);
    for id in ids {
        let last = swarm.with_node(id, |node| node.finalized().keys().next_back().copied());
        assert!(
            last.is_some_and(|last| last >= horizon),
            "node {id:?} stalled at {last:?}, wanted {horizon:?}"
        );
    }
}
