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

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::SocketAddr,
    time::Duration,
};

use monad_mcp_cadence::spec::{Deserializable as _, ProposalHeader as _};
use monad_mcp_common::{Lane, Lanes, Slot, Timestamp, TimestampDelta};
use monad_mcp_node::{
    Component, NodeOutput,
    cadence::{
        CadenceMessage, Outbound,
        slot::chorus::{ChorusMessage, FinalizationPath},
        types::ProposalId,
    },
    common::NodeId,
    component::CadenceWireMsg,
    config::{
        CadenceConfig, DAConfig, LedgerConfig, NetworkConfig, NodeConfig, ProposalConfig,
        RepeaterSection, ValidatorConfig,
    },
    da::{ProposalKeyPair, read_envelope},
    network::{NodeInbound, NodeMessage, OutboundClass},
};
use monad_sim::{CancelToken, ClockDiff, Ctx, Handle, StepLabel, Time};
use monad_sim_swarm::{
    Link, Net, Network, Parallel, Placer, Provenance, RegionTable, SimClient, Swarm,
};
use rand::{SeedableRng as _, distributions::Uniform};
use rand_chacha::ChaCha8Rng;
use rapidhash::v3::{DEFAULT_RAPID_SECRETS, RapidStreamHasherV3, rapidhash_v3};

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

// the finalized shape of a slot: which lanes committed
pub type Pattern = Lanes<Option<ProposalId>>;

// a message in the simulated network: the node's message, plus whether it
// is part of its sender's first hop. The flag is simulation metadata for the
// receiver's records; the node never sees it, and it costs no wire bytes
#[derive(Clone)]
pub struct SimMessage {
    pub message: NodeMessage,
    pub first_hop: bool,
}

// What a recording node saw (opt-in, `SimNode::recording`), for checking
// the adaptive lead against the model per validator. Times are sim times.
#[derive(Default)]
pub struct Records {
    // per (slot, lane), the first-hop chunks this node received from the
    // lane's proposer: the proposer, and the arrival times
    pub first_hop_in: BTreeMap<(Slot, Lane), (NodeId, Vec<Time>)>,
    // per own slot, each peer's first batch vote: arrival time, and whether
    // its entry for the own lane is positive
    pub votes_in: BTreeMap<Slot, BTreeMap<NodeId, (Time, bool)>>,
}

// a node as a monad-sim process, boxed so one swarm can mix node kinds
pub struct SimNode {
    id: NodeId,
    node: Box<dyn Component<Input = NodeInbound, Output = NodeOutput> + Send>,
    finalized: BTreeMap<Slot, Pattern>,
    // how each slot finalized here
    paths: BTreeMap<Slot, FinalizationPath>,
    // when this node's first first-hop message of each own slot left: the
    // seal time, as encoding is instant here
    first_hops: BTreeMap<Slot, Time>,
    // the lane of each own slot
    own_lanes: BTreeMap<Slot, Lane>,
    // receiver-side records, when recording
    records: Option<Records>,
    // hashes every message delivered to this node, with its time, in
    // delivery order
    delivery_hasher: RapidStreamHasherV3<'static>,
    // the node's clock reads sim time + clock_offset
    clock_offset: ClockDiff,
    // the one live timer step, at the node's last polled next_due
    due_alarm: Option<(Time, CancelToken)>,
    me: Option<Handle<Self>>,
    net: Option<Handle<Net<NodeId, SimMessage>>>,
    // strict first-hop priority on the uplink, when enabled
    uplink: Option<Uplink>,
}

// A model of the node's own uplink, ahead of the network's serial egress:
// first-hop shares go to the network at once, everything else waits here
// until the uplink is idle, so a first-hop share never queues behind more
// than the one message already on the wire (docs/0005 §5.6). It accounts
// bytes exactly as the egress does, so a released message starts at once.
struct Uplink {
    bits_per_sec: u64,
    // the receivers of a broadcast, each a separate transmission
    peers: u64,
    busy_until: Time,
    waiting: VecDeque<Outbound<NodeMessage>>,
    // the armed release step, if any
    release_at: Option<Time>,
}

impl Uplink {
    // the egress time of `outbound`, as the network's egress computes it:
    // per transmission, rounded up to the nanosecond
    fn transmit_time(&self, outbound: &Outbound<NodeMessage>) -> Duration {
        let (message, copies) = match outbound {
            Outbound::Broadcast(message) => (message, self.peers),
            Outbound::Unicast(_, message) => (message, 1),
        };
        let bits = wire_size(message) as u128 * 8;
        let nanos = (bits * 1_000_000_000).div_ceil(u128::from(self.bits_per_sec));
        let nanos = u64::try_from(nanos).expect("transmit time") * copies;
        Duration::from_nanos(nanos)
    }

    fn occupy(&mut self, now: Time, outbound: &Outbound<NodeMessage>) {
        let start = self.busy_until.max(now);
        self.busy_until = start + self.transmit_time(outbound);
    }
}

impl SimNode {
    pub fn new(
        id: NodeId,
        node: impl Component<Input = NodeInbound, Output = NodeOutput> + Send + 'static,
    ) -> Self {
        Self {
            id,
            node: Box::new(node),
            finalized: BTreeMap::new(),
            paths: BTreeMap::new(),
            first_hops: BTreeMap::new(),
            own_lanes: BTreeMap::new(),
            records: None,
            delivery_hasher: RapidStreamHasherV3::new(&DEFAULT_RAPID_SECRETS),
            clock_offset: ClockDiff(0),
            due_alarm: None,
            me: None,
            net: None,
            uplink: None,
        }
    }

    // the node's clock runs `offset` ahead of sim time (behind if negative)
    pub fn with_clock_offset(mut self, offset: ClockDiff) -> Self {
        self.clock_offset = offset;
        self
    }

    // a clock step mid-run: the pending due time is re-read on the new clock
    pub fn set_clock_offset(&mut self, offset: ClockDiff, ctx: &mut Ctx) {
        self.clock_offset = offset;
        self.arm_due_alarm(ctx);
    }

    // strict priority for the node's own first-hop shares on an uplink of
    // `bits_per_sec`, among `validators` nodes; must match the network's
    // egress rate. Without it, every message goes to the network in order
    pub fn first_hop_priority(mut self, bits_per_sec: u64, validators: u64) -> Self {
        self.uplink = Some(Uplink {
            bits_per_sec,
            peers: validators.saturating_sub(1),
            busy_until: Time(0),
            waiting: VecDeque::new(),
            release_at: None,
        });
        self
    }

    // keep receiver-side records (see `Records`)
    pub fn recording(mut self) -> Self {
        self.records = Some(Records::default());
        self
    }

    pub fn records(&self) -> Option<&Records> {
        self.records.as_ref()
    }

    pub fn clock_offset(&self) -> ClockDiff {
        self.clock_offset
    }

    pub fn init(&mut self, ctx: &mut Ctx) {
        let _span = self.span().entered();
        self.process(ctx);
    }

    fn handle_due(&mut self, ctx: &mut Ctx) {
        let _span = self.span().entered();
        self.due_alarm = None;
        let now = self.local_now(ctx);
        self.node.handle_due(now);
        self.process(ctx);
    }

    // turn every pending node output into a sim step, then re-arm the timer
    fn process(&mut self, ctx: &mut Ctx) {
        while let Some(output) = self.node.poll() {
            match output {
                NodeOutput::Network(outbound, class) => self.transmit(outbound, class, ctx),
                NodeOutput::Ledger(finalized) => {
                    let pattern = finalized.finalization.pids();
                    self.finalized.insert(finalized.slot, pattern);
                    let path = finalized.finalization.path();
                    self.paths.insert(finalized.slot, path);
                }
            }
        }
        self.arm_due_alarm(ctx);
    }

    // the node's whole outbound path: every message it sends goes through
    // here to its network stack, which models the uplink. With first-hop
    // priority, the uplink queue sits here, ahead of the stack.
    fn transmit(&mut self, outbound: Outbound<NodeMessage>, class: OutboundClass, ctx: &mut Ctx) {
        if class == OutboundClass::FirstHop {
            self.record_first_hop(&outbound, ctx.now());
        }
        let first_hop = class == OutboundClass::FirstHop;
        let Some(uplink) = &mut self.uplink else {
            self.send(outbound, first_hop, ctx);
            return;
        };
        match class {
            OutboundClass::FirstHop => {
                uplink.occupy(ctx.now(), &outbound);
                self.send(outbound, true, ctx);
            }
            OutboundClass::Standard => {
                uplink.waiting.push_back(outbound);
                self.release(ctx);
            }
        }
    }

    // hand waiting messages to the network while the uplink is idle; arm a
    // step for when it next is
    fn release(&mut self, ctx: &mut Ctx) {
        let now = ctx.now();
        let uplink = self.uplink.as_mut().expect("priority enabled");
        let mut released = Vec::new();
        while uplink.busy_until <= now
            && let Some(outbound) = uplink.waiting.pop_front()
        {
            uplink.occupy(now, &outbound);
            released.push(outbound);
        }
        let next = (!uplink.waiting.is_empty()).then_some(uplink.busy_until);
        let arm = next.filter(|at| uplink.release_at.is_none_or(|armed| armed > *at));
        if let Some(at) = arm {
            uplink.release_at = Some(at);
            let me = self.me.expect("node not wired");
            ctx.schedule(me, at, StepLabel::source("uplink"), |node, ctx| {
                node.uplink.as_mut().expect("priority enabled").release_at = None;
                node.release(ctx);
            });
        }
        for outbound in released {
            self.send(outbound, false, ctx);
        }
    }

    fn send(&self, outbound: Outbound<NodeMessage>, first_hop: bool, ctx: &mut Ctx) {
        let net = self.net.expect("node not wired");
        let wrap = |message| SimMessage { message, first_hop };
        match outbound {
            Outbound::Broadcast(message) => {
                let message = wrap(message);
                ctx.schedule(
                    net,
                    ctx.now(),
                    StepLabel::source("broadcast"),
                    move |net, ctx| net.broadcast(ctx, message),
                );
            }
            Outbound::Unicast(to, message) => {
                let message = wrap(message);
                ctx.schedule(
                    net,
                    ctx.now(),
                    StepLabel::source("unicast"),
                    move |net, ctx| net.send(ctx, to, message),
                );
            }
        }
    }

    // one step at next_due; a due already passed fires now
    fn arm_due_alarm(&mut self, ctx: &mut Ctx) {
        let me = self.me.expect("node not wired");
        let at = self
            .node
            .next_due()
            .map(|due| self.sim_time_of(due).max(ctx.now()));
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

    // the node's clock: every time the node sees comes from local_now, and its
    // absolute due times go back through sim_time_of. The clock reads sim
    // time plus clock_offset; before the epoch it reads the epoch.
    fn local_now(&self, ctx: &Ctx) -> Timestamp {
        let local = ctx.now() + self.clock_offset;
        timestamp_of(local.max(Time(0)))
    }

    // the sim time at which the node's clock reads `at`
    fn sim_time_of(&self, at: Timestamp) -> Time {
        time_of(at) - self.clock_offset
    }

    pub fn finalized(&self) -> &BTreeMap<Slot, Pattern> {
        &self.finalized
    }

    pub fn paths(&self) -> &BTreeMap<Slot, FinalizationPath> {
        &self.paths
    }

    // per own slot, when its first hop started to leave
    pub fn first_hops(&self) -> &BTreeMap<Slot, Time> {
        &self.first_hops
    }

    // per own slot, its lane
    pub fn own_lanes(&self) -> &BTreeMap<Slot, Lane> {
        &self.own_lanes
    }

    // a proposal's first hop leaves in one burst: only the first message of
    // a burst is decoded for its slot
    fn record_first_hop(&mut self, outbound: &Outbound<NodeMessage>, now: Time) {
        if self.first_hops.values().next_back() == Some(&now) {
            return;
        }
        let Outbound::Unicast(_, NodeMessage::Chunk(bytes)) = outbound else {
            return;
        };
        let Ok(envelope) = read_envelope(bytes.clone()) else {
            return;
        };
        let slot = envelope.header().slot();
        self.first_hops.entry(slot).or_insert(now);
        self.own_lanes
            .entry(slot)
            .or_insert(envelope.header().lane());
    }

    // a recording node's view of a delivery: first-hop chunks of others'
    // proposals, and, while it proposes, the batch votes on its own slots
    fn record_inbound(&mut self, at: Time, from: NodeId, message: &SimMessage) {
        let Some(records) = &mut self.records else {
            return;
        };
        match &message.message {
            NodeMessage::Chunk(bytes) if message.first_hop => {
                let Ok(envelope) = read_envelope(bytes.clone()) else {
                    return;
                };
                let header = envelope.header();
                let key = (header.slot(), header.lane());
                let (_, times) = records
                    .first_hop_in
                    .entry(key)
                    .or_insert_with(|| (from, Vec::new()));
                times.push(at);
            }
            NodeMessage::Cadence(bytes) => {
                // votes come in up to a few slots after the deadline
                let recent = self
                    .own_lanes
                    .keys()
                    .next_back()
                    .is_some_and(|last| time_of(deadline_of_slot(last.0 + 8)) >= at);
                if !recent {
                    return;
                }
                let Ok(CadenceMessage::Slot(ChorusMessage::BatchVote(vote))) =
                    CadenceWireMsg::deserialize(bytes)
                else {
                    return;
                };
                let Some(lane) = self.own_lanes.get(&vote.slot()) else {
                    return;
                };
                let positive = vote.positive_lanes().get(usize::from(*lane)) == Some(&true);
                let votes = records.votes_in.entry(vote.slot()).or_default();
                votes.entry(from).or_insert((at, positive));
            }
            _ => {}
        }
    }

    fn record_delivery(&mut self, at: Time, from: NodeId, message: &NodeMessage) {
        let (kind, bytes) = match message {
            NodeMessage::Cadence(bytes) => (0u8, bytes),
            NodeMessage::Chunk(bytes) => (1, bytes),
            NodeMessage::ChunkRequest(bytes) => (2, bytes),
            NodeMessage::Tx(bytes) => (3, bytes),
        };
        let hasher = &mut self.delivery_hasher;
        hasher.write(&at.0.to_le_bytes());
        hasher.write(&u64::from(from).to_le_bytes());
        hasher.write(&[kind]);
        hasher.write(&(bytes.len() as u64).to_le_bytes());
        hasher.write(bytes);
    }

    // every log line of a step names its node
    fn span(&self) -> tracing::Span {
        tracing::info_span!("node", id = u64::from(self.id))
    }
}

impl SimClient for SimNode {
    type Addr = NodeId;
    type Message = SimMessage;

    fn wire(&mut self, me: Handle<Self>, net: Handle<Net<NodeId, SimMessage>>) {
        self.me = Some(me);
        self.net = Some(net);
    }

    fn receive(&mut self, from: NodeId, message: SimMessage, ctx: &mut Ctx) {
        let _span = self.span().entered();
        self.record_delivery(ctx.now(), from, &message.message);
        // the node loops its own messages back itself
        if from == self.id {
            return;
        }
        self.record_inbound(ctx.now(), from, &message);
        let inbound = NodeInbound {
            from,
            message: message.message,
        };
        let now = self.local_now(ctx);
        self.node.handle(now, inbound);
        self.process(ctx);
    }
}

// a node's seed: a function of the simulation seed and the node id
pub fn node_seed(seed: u64, id: u64) -> u64 {
    rapidhash_v3(&[seed.to_le_bytes(), id.to_le_bytes()].concat())
}

// validator `id` of `validators`, seeded from the simulation seed
pub fn node_config(seed: u64, id: u64, validators: u64) -> NodeConfig {
    use monad_mcp_common::spec::KeyPair as _;

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
        leader_election: Default::default(),
        repeater: Some(RepeaterSection {
            interval: DELTA.checked_mul(2).unwrap(),
            ..RepeaterSection::default()
        }),
        mempool: Default::default(),
        ledger: LedgerConfig {
            dir: "unused-ledger".into(),
        },
        seed: Some(node_seed(seed, id)),
    }
}

// MCP_SIM_THREADS=n runs scenarios built with start_swarm in parallel on n
// threads, one domain per node; unset, they run sequentially. Results are the
// same either way.
pub fn parallel_from_env() -> Option<Parallel<NodeId>> {
    let threads = std::env::var("MCP_SIM_THREADS").ok()?;
    let threads = threads.parse().expect("MCP_SIM_THREADS is a thread count");
    Some(Parallel::default().threads(threads))
}

// build a swarm, sequential or parallel, and start every node at time 0
pub fn start_swarm(
    seed: u64,
    network: Network<NodeId, SimMessage>,
    nodes: Vec<(NodeId, SimNode)>,
    parallel: Option<Parallel<NodeId>>,
) -> Swarm<SimNode> {
    let mut swarm = match parallel {
        None => Swarm::build(seed, network, nodes),
        Some(parallel) => Swarm::build_parallel(seed, network, nodes, parallel),
    };
    for id in swarm.node_ids() {
        let handle = swarm.handle(&id).expect("node just built");
        swarm
            .sim()
            .schedule(handle, Time(0), "init", |node, ctx| node.init(ctx));
    }
    swarm
}

// a digest of the run: per node, the finalized history and every delivered
// message with its time. Hashes explicit bytes with rapidhash v3, so the
// digest is the same on every platform and release.
pub fn run_digest(swarm: &Swarm<SimNode>) -> u64 {
    let mut hasher = RapidStreamHasherV3::new(&DEFAULT_RAPID_SECRETS);
    for id in swarm.node_ids() {
        swarm.with_node(&id, |node| {
            hasher.write(&u64::from(id).to_le_bytes());
            for (slot, pattern) in &node.finalized {
                hasher.write(&slot.0.to_le_bytes());
                for lane in pattern.lanes() {
                    match pattern.get(usize::from(lane)).expect("in range") {
                        Some(pid) => {
                            hasher.write(&[1]);
                            hasher.write(pid.as_bytes());
                        }
                        None => hasher.write(&[0]),
                    }
                }
            }
            hasher.write(&node.delivery_hasher.finish().to_le_bytes());
        });
    }
    hasher.finish()
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

// p50 RTT in ms between the testnet hosts, row = from
// (monad-mcp-node/deploy/config/latency.toml, measured 2026-10-03):
// ams, deu, ewr, lax, ltu, sgp, swe, vin. The diagonal is 0 there; a table
// needs a positive one, so same-host pairs get 2 ms RTT as in
// RegionTable::public_internet.
const TESTNET_RTT_MS: [[f64; 8]; 8] = [
    [0.0, 6.71, 69.7, 162.0, 34.4, 163.0, 19.6, 88.4],
    [6.7, 0.0, 75.2, 150.0, 27.8, 153.0, 18.8, 88.1],
    [69.7, 75.2, 0.0, 61.6, 110.0, 207.0, 89.3, 6.97],
    [162.0, 150.0, 61.7, 0.0, 172.0, 176.0, 162.0, 65.6],
    [34.4, 27.8, 110.0, 172.0, 0.0, 195.0, 13.0, 114.0],
    [163.0, 153.0, 207.0, 176.0, 195.0, 0.0, 182.0, 238.0],
    [19.6, 18.8, 89.2, 162.0, 13.0, 182.0, 0.0, 116.0],
    [88.4, 88.1, 6.97, 65.5, 114.0, 238.0, 116.0, 0.0],
];

// the 8-host testnet as a region table, one region per host
pub fn testnet_table() -> RegionTable {
    let rtt = TESTNET_RTT_MS
        .iter()
        .map(|row| {
            row.iter()
                .map(|&ms| Duration::from_secs_f64(if ms == 0.0 { 2.0 } else { ms } / 1000.0))
                .collect()
        })
        .collect();
    RegionTable::from_rtt(
        Provenance::new(
            "mcp testnet",
            "monad-mcp-node/deploy/config/latency.toml, p50 RTT; diagonal 2 ms",
            "2026-10-03",
        ),
        ["ams", "deu", "ewr", "lax", "ltu", "sgp", "swe", "vin"],
        rtt,
    )
}

pub const GBIT: u64 = 1_000_000_000;

// what one message costs on the wire: the frame (sender id, kind, payload)
// plus the IPv4 and UDP headers
pub fn wire_size(message: &NodeMessage) -> usize {
    let payload = match message {
        NodeMessage::Cadence(bytes)
        | NodeMessage::Chunk(bytes)
        | NodeMessage::ChunkRequest(bytes)
        | NodeMessage::Tx(bytes) => bytes.len(),
    };
    9 + payload + 20 + 8
}

// validators placed over a region table: a node-level latency matrix with a
// last mile per node, up to 1 ms jitter, and a 1 Gbit/s uplink per node
pub struct Geo {
    pub network: Network<NodeId, SimMessage>,
    // each node's region, numbered densely from 0 in table order
    pub region_of: BTreeMap<NodeId, u32>,
    // per node in id order: its region's name, and its one-way base
    // latency to every node (row = from), last miles included; 0 to itself
    pub labels: Vec<String>,
    pub one_way: Vec<Vec<Duration>>,
}

// the per-message jitter `geo` adds to the base latency, uniform in [0, max]
pub const GEO_JITTER_MAX: Duration = Duration::from_millis(1);

// a delay added to every message on a link, as a function of the link (its
// send time included): a time-varying network
pub type LinkDelay = Box<dyn Fn(&Link<NodeId>) -> Duration + Send + Sync>;

pub fn geo(table: &RegionTable, validators: u64, seed: u64) -> Geo {
    geo_delayed(
        table,
        validators,
        seed,
        None::<fn(&BTreeMap<NodeId, u32>) -> LinkDelay>,
    )
}

// `geo` with an extra, non-negative delay per message, built from each
// node's region. The base matrix stays each link's lower bound
pub fn geo_delayed(
    table: &RegionTable,
    validators: u64,
    seed: u64,
    delay: Option<impl FnOnce(&BTreeMap<NodeId, u32>) -> LinkDelay>,
) -> Geo {
    let placement = Placer::new(table)
        .last_mile(Duration::from_micros(50)..=Duration::from_micros(250))
        .place(
            (0..validators).map(NodeId::dummy),
            &mut ChaCha8Rng::seed_from_u64(seed),
        );
    let regions: BTreeSet<usize> = placement.nodes().iter().map(|n| n.region).collect();
    let dense: BTreeMap<usize, u32> = regions
        .into_iter()
        .enumerate()
        .map(|(index, region)| (region, index as u32))
        .collect();
    let region_of = placement
        .nodes()
        .iter()
        .map(|n| (n.addr, dense[&n.region]))
        .collect();
    let names = table.regions();
    let labels = placement
        .nodes()
        .iter()
        .map(|n| names[n.region].clone())
        .collect();
    let matrix = placement.matrix();
    let addrs: Vec<NodeId> = placement.nodes().iter().map(|n| n.addr).collect();
    let one_way = addrs
        .iter()
        .map(|from| {
            addrs
                .iter()
                .map(|to| {
                    if from == to {
                        Duration::ZERO
                    } else {
                        matrix.latency(from, to)
                    }
                })
                .collect()
        })
        .collect();
    let jitter = Uniform::new_inclusive(Duration::ZERO, GEO_JITTER_MAX);
    let size = |message: &SimMessage| wire_size(&message.message);
    let network = match delay {
        None => Network::with_matrix(matrix, jitter),
        Some(delay) => {
            use rand::distributions::Distribution as _;

            let delay = delay(&region_of);
            let matrix = std::sync::Arc::new(matrix);
            let bound = matrix.clone();
            Network::with_link_latency(move |link, rng| {
                matrix.latency(&link.from, &link.to) + jitter.sample(rng) + delay(link)
            })
            .link_min_latency(move |from, to| bound.latency(from, to))
        }
    }
    .egress(GBIT, size);
    Geo {
        network,
        region_of,
        labels,
        one_way,
    }
}

// one domain per region of `geo`
pub fn by_region(geo: &Geo) -> Parallel<NodeId> {
    let region_of = geo.region_of.clone();
    Parallel::default().domains(move |node| region_of[node])
}
