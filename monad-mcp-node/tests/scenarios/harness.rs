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

//! A sync `Node` as a monad-sim process, and the checks the scenarios
//! share.

use std::{collections::BTreeMap, net::SocketAddr};

use monad_mcp_chorus::spec::KeyPair as _;
use monad_mcp_node::{
    Component, NodeOutput,
    chorus::{
        Outbound,
        types::{MerkleRoot, NodeId, ProposalMap, Slot, Timestamp, TimestampDelta},
    },
    config::{
        CadenceConfig, DAConfig, NetworkConfig, NodeConfig, ProposalConfig, RepeaterSection,
        ValidatorConfig,
    },
    da::ProposalKeyPair,
    network::{Inbound, Packet},
};
use monad_sim::{CancelToken, Ctx, Handle, StepLabel, Time};
use monad_sim_swarm::{Net, SimClient, Swarm};

pub const DELTA: TimestampDelta = TimestampDelta::from_millis(200);
pub const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
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

// the finalized shape of a slot: which proposal indices committed
pub type Pattern = ProposalMap<Option<MerkleRoot>>;

// a node as a monad-sim process, boxed so one swarm can mix node kinds
pub struct SimNode {
    id: NodeId,
    node: Box<dyn Component<Input = Inbound, Output = NodeOutput>>,
    finalized: BTreeMap<Slot, Pattern>,
    // the one live timer step, at the node's last polled next_due
    due_alarm: Option<(Time, CancelToken)>,
    me: Option<Handle<Self>>,
    net: Option<Handle<Net<NodeId, Packet>>>,
}

impl SimNode {
    pub fn new(
        id: NodeId,
        node: impl Component<Input = Inbound, Output = NodeOutput> + 'static,
    ) -> Self {
        Self {
            id,
            node: Box::new(node),
            finalized: BTreeMap::new(),
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
                NodeOutput::Send(outbound) => self.send(outbound, ctx),
                NodeOutput::Finalized(finalized) => {
                    let pattern = finalized.finalization.roots();
                    self.finalized.insert(finalized.slot, pattern);
                }
            }
        }
        self.arm_due_alarm(ctx);
    }

    fn send(&self, outbound: Outbound<Packet>, ctx: &mut Ctx) {
        let from = self.id;
        let net = self.net.expect("node not wired");
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

    pub fn finalized(&self) -> &BTreeMap<Slot, Pattern> {
        &self.finalized
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

pub fn node_config(id: u64, validators: u64) -> NodeConfig {
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
    NodeConfig {
        node_id,
        proposal_key_pair: ProposalKeyPair::dummy(node_id),
        cadence_key_pair: node_id.keypair(),
        validators,
        genesis_deadline: GENESIS_DEADLINE,
        network: NetworkConfig { port: 0 },
        cadence: CadenceConfig {
            slot_interval: SLOT_INTERVAL,
            ..CadenceConfig::default()
        },
        da: DAConfig::default(),
        proposal: ProposalConfig::default(),
        repeater: Some(RepeaterSection {
            interval: DELTA.checked_mul(2).unwrap(),
            ..RepeaterSection::default()
        }),
    }
}

// every slot below the horizon finalized everywhere, with agreed-upon pattern
pub fn assert_progress(swarm: &Swarm<SimNode>, horizon: Slot) {
    let mut patterns: BTreeMap<Slot, Pattern> = BTreeMap::new();
    for id in swarm.node_ids() {
        let finalized = swarm.with_node(&id, |node| node.finalized().clone());
        let last = finalized.keys().next_back().copied();
        assert!(
            last.is_some_and(|last| last >= horizon),
            "node {id:?} stalled at {last:?}, wanted {horizon:?}"
        );

        for slot in 0..horizon.0 {
            let slot = Slot(slot);
            if let Some(pattern) = finalized.get(&slot) {
                let seen = patterns.entry(slot).or_insert_with(|| pattern.clone());
                assert_eq!(seen, pattern, "node {id:?} disagrees on {slot:?}");
            } else {
                // skipped slots are allowed.
            }
        }
    }
}

// every slot below the horizon finalized everywhere, with agreed-upon pattern
pub fn assert_consistent_progress(swarm: &Swarm<SimNode>, horizon: Slot) {
    let mut patterns: BTreeMap<Slot, Pattern> = BTreeMap::new();
    for id in swarm.node_ids() {
        let finalized = swarm.with_node(&id, |node| node.finalized().clone());
        let last = finalized.keys().next_back().copied();
        assert!(
            last.is_some_and(|last| last >= horizon),
            "node {id:?} stalled at {last:?}, wanted {horizon:?}"
        );

        for slot in 0..horizon.0 {
            let slot = Slot(slot);
            let pattern = finalized
                .get(&slot)
                .unwrap_or_else(|| panic!("node {id:?} skipped {slot:?}"));
            let seen = patterns.entry(slot).or_insert_with(|| pattern.clone());
            assert_eq!(seen, pattern, "node {id:?} disagrees on {slot:?}");
        }
    }
}
