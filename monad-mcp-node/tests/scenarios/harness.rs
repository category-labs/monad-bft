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
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    time::Duration,
};

use monad_mcp_cadence::spec::ProposalHeader as _;
use monad_mcp_common::{Lane, Lanes, Slot, Timestamp, TimestampDelta};
use monad_mcp_node::{
    Component, NodeOutput,
    cadence::{Outbound, slot::chorus::FinalizationPath, types::ProposalId},
    common::NodeId,
    config::{
        CadenceConfig, DAConfig, LedgerConfig, NetworkConfig, NodeConfig, ProposalConfig,
        RepeaterSection, ValidatorConfig,
    },
    da::{self, ProposalKeyPair, ProposalScheme},
    network::{NodeInbound, NodeMessage, OutboundClass},
};
use monad_sim::{CancelToken, ClockDiff, Ctx, Handle, StepLabel, Time};
use monad_sim_swarm::{
    LatencyMatrix, Net, Network, Parallel, Placer, Provenance, RegionTable, SimClient, Swarm,
};
use rand::{Rng as _, SeedableRng as _, distributions::Uniform};
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

// the chunk bytes delivered to a node: what crossed the sim's wire, and
// what the sim charged for it
#[derive(Clone, Copy, Default, Debug)]
pub struct ChunkBytes {
    pub wire: u64,
    pub charged: u64,
}

// a message in the simulated network: the node's message, and whether it
// is part of its sender's first hop. The flag is simulation metadata for the
// network's priority classes and the receivers' records: the node never sees
// it, and it costs no bytes
#[derive(Clone)]
pub struct SimMessage {
    pub message: NodeMessage,
    pub first_hop: bool,
}

// What a recording node saw (opt-in, `SimNode::recording`), to compare a
// run with a model per validator. Times are sim times.
#[derive(Default)]
pub struct Records {
    // per (slot, lane), the first-hop chunks this node received from the
    // lane's proposer: the proposer, and the arrival times
    pub first_hop_in: BTreeMap<(Slot, Lane), (NodeId, Vec<Time>)>,
}

// a node as a monad-sim process, boxed so one swarm can mix node kinds
pub struct SimNode {
    id: NodeId,
    node: Box<dyn Component<Input = NodeInbound, Output = NodeOutput> + Send>,
    finalized: BTreeMap<Slot, Pattern>,
    // the lanes a slot committed whose proposal resolved invalid, for the
    // slots that have any
    invalid: BTreeMap<Slot, Vec<usize>>,
    // how each slot finalized here
    paths: BTreeMap<Slot, FinalizationPath>,
    // per own slot, when its first first-hop message left (the seal time,
    // as encoding is instant here), and its lane
    first_hops: BTreeMap<Slot, (Time, Lane)>,
    // receiver-side records, when recording
    records: Option<Records>,
    // hashes every message delivered to this node, with its time, in
    // delivery order
    delivery_hasher: RapidStreamHasherV3<'static>,
    chunk_bytes: ChunkBytes,
    // the node's clock reads sim time + clock_offset
    clock_offset: ClockDiff,
    // the one live timer step, at the node's last polled next_due
    due_alarm: Option<(Time, CancelToken)>,
    me: Option<Handle<Self>>,
    net: Option<Handle<Net<NodeId, SimMessage>>>,
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
            invalid: BTreeMap::new(),
            paths: BTreeMap::new(),
            first_hops: BTreeMap::new(),
            records: None,
            delivery_hasher: RapidStreamHasherV3::new(&DEFAULT_RAPID_SECRETS),
            chunk_bytes: ChunkBytes::default(),
            clock_offset: ClockDiff(0),
            due_alarm: None,
            me: None,
            net: None,
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

    // keep receiver-side records (see `Records`)
    pub fn recording(mut self) -> Self {
        self.records = Some(Records::default());
        self
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
                    // committed, but without a message
                    let invalid: Vec<usize> = pattern
                        .lanes()
                        .map(usize::from)
                        .filter(|&j| {
                            let committed = pattern.get(j).expect("in range").is_some();
                            committed && finalized.proposals.get(j).expect("in range").is_none()
                        })
                        .collect();
                    if !invalid.is_empty() {
                        self.invalid.insert(finalized.slot, invalid);
                    }
                    self.finalized.insert(finalized.slot, pattern);
                    let path = finalized.finalization.path();
                    self.paths.insert(finalized.slot, path);
                }
            }
        }
        self.arm_due_alarm(ctx);
    }

    // the node's whole outbound path: every message it sends goes through
    // here to its network stack, which models the uplink, with the outbound
    // class as the message's first-hop flag (see `first_hop_priority`)
    fn transmit(&mut self, outbound: Outbound<NodeMessage>, class: OutboundClass, ctx: &mut Ctx) {
        let net = self.net.expect("node not wired");
        let first_hop = class == OutboundClass::FirstHop;
        if first_hop {
            self.record_first_hop(&outbound, ctx.now());
        }
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

    // the committed lanes whose proposal resolved invalid, by slot
    pub fn invalid(&self) -> &BTreeMap<Slot, Vec<usize>> {
        &self.invalid
    }

    pub fn finalized(&self) -> &BTreeMap<Slot, Pattern> {
        &self.finalized
    }

    pub fn chunk_bytes(&self) -> ChunkBytes {
        self.chunk_bytes
    }

    // how each slot finalized here
    pub fn paths(&self) -> &BTreeMap<Slot, FinalizationPath> {
        &self.paths
    }

    // per own slot, when its first hop started to leave, and its lane
    pub fn first_hops(&self) -> &BTreeMap<Slot, (Time, Lane)> {
        &self.first_hops
    }

    pub fn records(&self) -> Option<&Records> {
        self.records.as_ref()
    }

    // a proposal's first hop leaves in one burst: only the first message of
    // a burst is decoded for its slot
    fn record_first_hop(&mut self, outbound: &Outbound<NodeMessage>, now: Time) {
        if self.first_hops.values().next_back().map(|(at, _)| *at) == Some(now) {
            return;
        }
        let Outbound::Unicast(_, NodeMessage::Chunk(bytes)) = outbound else {
            return;
        };
        let Ok(envelope) = da::read_envelope(bytes.clone()) else {
            return;
        };
        let header = envelope.header();
        let first = (now, header.lane());
        self.first_hops.entry(header.slot()).or_insert(first);
    }

    // a recording node's view of a delivery: first-hop chunks of others'
    // proposals
    fn record_inbound(&mut self, at: Time, from: NodeId, message: &SimMessage) {
        let Some(records) = &mut self.records else {
            return;
        };
        match &message.message {
            NodeMessage::Chunk(bytes) if message.first_hop => {
                let Ok(envelope) = da::read_envelope(bytes.clone()) else {
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
            _ => {}
        }
    }

    fn record_delivery(&mut self, at: Time, from: NodeId, message: &NodeMessage) {
        if let NodeMessage::Chunk(bytes) = message {
            self.chunk_bytes.wire += bytes.len() as u64;
            self.chunk_bytes.charged += da::nominal_len(bytes) as u64;
        }
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

const GBIT: u64 = 1_000_000_000;

// what one message costs on the wire: the frame (sender id, kind, payload)
// plus the IPv4 and UDP headers. a hollow chunk costs its d33 chunk.
pub fn wire_size(message: &NodeMessage) -> usize {
    let payload = match message {
        NodeMessage::Chunk(bytes) => da::nominal_len(bytes),
        NodeMessage::Cadence(bytes) | NodeMessage::ChunkRequest(bytes) | NodeMessage::Tx(bytes) => {
            bytes.len()
        }
    };
    9 + payload + 20 + 8
}

// what a simulated message costs on the wire: its node message's cost
pub fn sim_wire_size(message: &SimMessage) -> usize {
    wire_size(&message.message)
}

// Strict priority for each node's own first-hop shares on its uplink
// (docs/0005 §5.6), as the network's priority classes: first-hop messages
// go on the wire at once, everything else waits until the uplink is idle, so
// a first-hop share waits behind at most the one message already on the
// wire. `network` must have an egress
pub fn first_hop_priority(network: Network<NodeId, SimMessage>) -> Network<NodeId, SimMessage> {
    network.priority(|message: &SimMessage| u8::from(!message.first_hop), 1)
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
    // the base latencies `network` samples from, for building a variant of
    // it (e.g. with a time-varying delay on top)
    pub matrix: LatencyMatrix<NodeId>,
}

// the per-message jitter `geo` adds to the base latency, uniform in [0, max]
pub const GEO_JITTER_MAX: Duration = Duration::from_millis(1);

pub fn geo_jitter() -> Uniform<Duration> {
    Uniform::new_inclusive(Duration::ZERO, GEO_JITTER_MAX)
}

pub fn geo(table: &RegionTable, validators: u64, seed: u64) -> Geo {
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
            let latency = |to: &NodeId| {
                if from == to {
                    Duration::ZERO
                } else {
                    matrix.latency(from, to)
                }
            };
            addrs.iter().map(latency).collect()
        })
        .collect();
    let network =
        Network::with_matrix(placement.matrix(), geo_jitter()).egress(GBIT, sim_wire_size);
    Geo {
        network,
        region_of,
        labels,
        one_way,
        matrix,
    }
}

// The network of `geo`, with equal stakes and the given clock offsets, as
// JSON, so a model can run on the same network: node-level one-way base
// latencies (last miles included), the jitter bound, the uplink, the clock
// offsets and the protocol timing. A scenario adds its own fields.
pub fn topology_json(geo: &Geo, clock_offsets: &[ClockDiff]) -> serde_json::Value {
    let n = geo.labels.len();
    let ms = |nanos: f64| (nanos / 1e3).round() / 1e3;
    let one_way: Vec<Vec<f64>> = geo
        .one_way
        .iter()
        .map(|row| row.iter().map(|d| ms(d.as_nanos() as f64)).collect())
        .collect();
    let offsets: Vec<f64> = clock_offsets.iter().map(|c| ms(c.0 as f64)).collect();
    serde_json::json!({
        "n": n,
        "labels": geo.labels,
        "stake": vec![1.0 / n as f64; n],
        "one_way_ms": one_way,
        "jitter_max_ms": ms(GEO_JITTER_MAX.as_nanos() as f64),
        "clock_offset_ms": offsets,
        "egress_bps": GBIT as f64,
        "delta_ms": ms(DELTA.as_nanos() as f64),
        "slot_ms": ms(SLOT_INTERVAL.as_nanos() as f64),
        "notes": "one_way_ms: base latency per node pair (row = from), last miles \
                  included; every message adds uniform jitter in [0, jitter_max_ms]. \
                  A node's clock reads sim time + clock_offset_ms. Equal stakes.",
    })
}

// per node, a clock offset drawn from normal(0, sigma_ms), seeded, in
// whole microseconds (so an export in ms is exact); all 0 for sigma 0
pub fn normal_clock_offsets(seed: u64, validators: u64, sigma_ms: f64) -> Vec<ClockDiff> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xC10C_0FF5);
    (0..validators)
        .map(|_| {
            if sigma_ms == 0.0 {
                return ClockDiff(0);
            }
            // Box–Muller
            let u1: f64 = 1.0 - rng.r#gen::<f64>();
            let u2: f64 = rng.r#gen();
            let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
            let micros = (z * sigma_ms * 1000.0).round() as i128;
            ClockDiff(micros * 1000)
        })
        .collect()
}

// the chunk data of a scenario's proposals from the environment variable
// `var`: `d33` (default, real chunks) or `d33_hollow` (the same messages
// and sizes without the data; see docs/simulation-data.md)
pub fn scheme_from_env(var: &str) -> ProposalScheme {
    match std::env::var(var).as_deref() {
        Err(_) | Ok("d33") => ProposalScheme::D33,
        Ok("d33_hollow") => ProposalScheme::D33Hollow,
        Ok(other) => panic!("{var} is d33 or d33_hollow, not {other}"),
    }
}

// one domain per region of `geo`
pub fn by_region(geo: &Geo) -> Parallel<NodeId> {
    let region_of = geo.region_of.clone();
    Parallel::default().domains(move |node| region_of[node])
}
