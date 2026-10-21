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

//! Slot swarm harness (§14.3): QuantumChorus nodes in the unchanged
//! `CadenceRuntime` with `MonadConductor<NopAcs>`, over `monad-sim-swarm`,
//! with DA events scripted per (node, slot, lane).

// each test binary uses a different subset
#![allow(dead_code)]

pub mod faulty;
pub mod network;
pub mod repeater;
pub mod scripted_da;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    num::NonZeroU64,
    ops::Range,
    sync::{Arc, Mutex},
};

use faulty::{Faulty, FaultyConfig, Rewrite};
use monad_mcp_chorus::{
    spec::{ProposalHeader as _, vote::KeyPair as _},
    stub::{
        CadenceRuntime, CadenceWireMsg, FinalizationObserver, SlotManager,
        conductor::{ConductorConfig, MonadConductor, acs::nop::NopAcs},
        env::{D25, EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader, ProposalSignature},
        proposers::header_auth,
        types::{
            FixedProposerSchedule, NodeId, ProposalId, ProposerSchedule, SignedProposalHeader,
            Slot, SlotDeadline, Stake, Timestamp, TimestampDelta, ValidatorData,
        },
    },
};
use monad_mcp_chorus_sim::{CadenceSwarm, CadenceSwarmBuilder, SimMessage};
use monad_pq_chorus::{
    Evidence, PqEvent, PqHalt, PqPath, QuantumChorus, QuantumConfig, QuantumContext,
    da::ChorusDAEvent,
    sig::StubPq,
    types::{Entry, Metablock},
};
use monad_sim_swarm::{Link, Network};
use network::Model;
use repeater::Repeater;
use scripted_da::{Registry, ScriptedDa};

pub type Conductor = MonadConductor<NopAcs<SlotDeadline>>;
pub type Wire = CadenceWireMsg<QuantumChorus, Conductor>;
pub type Swarm = CadenceSwarm<Wire, ChorusDAEvent>;
pub type DropFn = Arc<dyn Fn(&Link<NodeId>, &SimMessage<Wire>) -> bool>;

pub const GENESIS_DEADLINE: SlotDeadline = SlotDeadline::from_millis(100);

/// What DA reports to one node about one proposal of a lane, in ms
/// relative to the slot deadline.
#[derive(Clone, Debug)]
pub struct LanePlan {
    pub header: SignedProposalHeader,
    /// the node's own chunks arrive (HeaderSeen + ProposerObligationFulfilled)
    pub fulfilled_at: Option<i64>,
    /// decoding completes (with HeaderSeen), from forwarded chunks
    pub decoded_at: Option<i64>,
    pub valid: bool,
}

pub type Plan = Arc<dyn Fn(NodeId, Slot, usize) -> Vec<LanePlan> + Send + Sync>;

#[derive(Clone)]
pub struct Params {
    pub n: u64,
    pub k: usize,
    pub slot_interval: u64,
    pub latency: u64,
    /// the proposers' seal lead before the deadline (L)
    pub lead: u64,
    pub slots_per_window: u64,
    pub sync_boundary: u64,
    pub lag_threshold: u64,
    /// slots the DA script covers
    pub slots: u64,
    pub pull_delay: u64,
    /// the post-halt Repeater: interval in ms, retention in slots
    pub repeat_interval: u64,
    pub certificate_retention: u64,
    pub config: QuantumConfig,
}

impl Params {
    pub fn new(n: u64, k: usize) -> Self {
        Self {
            n,
            k,
            slot_interval: 100,
            latency: 10,
            lead: 50,
            slots_per_window: 10,
            sync_boundary: 5,
            lag_threshold: 10,
            slots: 40,
            pull_delay: 20,
            repeat_interval: 100,
            certificate_retention: 20,
            config: QuantumConfig::with_delta(TimestampDelta::from_millis(50)),
        }
    }

    pub fn deadline(&self, slot: u64) -> Timestamp {
        GENESIS_DEADLINE
            .checked_add_deltas(TimestampDelta::from_millis(self.slot_interval), slot)
            .unwrap()
    }

    /// The canonical plan: every lane's chunks arrive one hop after the
    /// seal, and decode one hop later (forwarding at the early vote).
    pub fn timely(&self) -> Plan {
        let seal = -(self.lead as i64);
        let h = self.latency as i64;
        Arc::new(move |_node, slot, j| {
            vec![LanePlan {
                header: lane_header(slot, j, 0),
                fulfilled_at: Some(seal + h),
                decoded_at: Some(seal + 2 * h),
                valid: true,
            }]
        })
    }
}

/// The header proposer `j` signs for `slot`; `variant` tells equivocations
/// apart.
pub fn lane_header(slot: Slot, j: usize, variant: u8) -> SignedProposalHeader {
    let mut root = [0u8; 20];
    root[..8].copy_from_slice(&slot.get().to_be_bytes());
    root[8] = j as u8;
    root[9] = variant;
    SignedProposalHeader {
        header: ProposalHeader {
            root: MerkleRoot(MerkleHash(root)),
            scheme: EncodingScheme::D25(D25 {
                slot,
                msg_len: 1,
                unix_ts: 0,
                depth: 3,
            }),
        },
        sig: ProposalSignature {
            signer: NodeId::dummy(j as u64),
            checksum: 0,
        },
    }
}

pub fn lane_pid(slot: Slot, j: usize, variant: u8) -> ProposalId {
    lane_header(slot, j, variant).pid()
}

pub fn validator_data(n: u64) -> Arc<ValidatorData> {
    let ids: Vec<NodeId> = (0..n).map(NodeId::dummy).collect();
    let valset = ids.iter().map(|id| (*id, Stake::from(1))).collect();
    let mapping = ids.iter().map(|id| (*id, id.keypair().pubkey())).collect();
    Arc::new(ValidatorData::new(valset, mapping))
}

/// Proposer j is validator j.
pub fn fixed_schedule(k: usize) -> Arc<dyn ProposerSchedule + Send + Sync> {
    Arc::new(FixedProposerSchedule::new(
        (0..k as u64).map(NodeId::dummy).collect(),
    ))
}

struct Schedule(Arc<dyn ProposerSchedule + Send + Sync>);

impl ProposerSchedule for Schedule {
    fn num_indices(&self) -> usize {
        self.0.num_indices()
    }

    fn proposers_at(
        &self,
        slot: Slot,
    ) -> Result<
        monad_mcp_chorus::stub::types::ProposerSet,
        monad_mcp_chorus::stub::types::ScheduleError,
    > {
        self.0.proposers_at(slot)
    }
}

pub fn context(
    me: NodeId,
    n: u64,
    proposers: Arc<dyn ProposerSchedule + Send + Sync>,
) -> QuantumContext {
    let scheme = StubPq::default();
    QuantumContext::new(
        me,
        Arc::new(scheme.signer(me)),
        Arc::new(scheme),
        validator_data(n),
        Arc::new(header_auth(Arc::new(Schedule(proposers.clone())))),
        proposers,
    )
    .expect("equal stake")
}

/// Everything a node reported to its observer.
#[derive(Default, Debug)]
pub struct NodeLog {
    pub finals: BTreeMap<Slot, Vec<(Timestamp, Metablock, PqPath)>>,
    /// the Coda view of each Coda-path finalization
    pub coda_views: BTreeMap<Slot, u64>,
    pub speculative: BTreeMap<Slot, Vec<Metablock>>,
    pub evidence: Vec<(Slot, Evidence)>,
    pub halts: BTreeMap<Slot, Vec<(Timestamp, PqHalt)>>,
}

#[derive(Clone, Default)]
pub struct LogHandle(pub Arc<Mutex<NodeLog>>);

impl FinalizationObserver<PqEvent, PqHalt> for LogHandle {
    fn handle_optimistic_commit(&mut self, now: Timestamp, slot: Slot, event: &PqEvent) {
        let mut log = self.0.lock().unwrap();
        match event {
            PqEvent::Speculative { metablock } => log
                .speculative
                .entry(slot)
                .or_default()
                .push(metablock.clone()),
            PqEvent::Final {
                metablock,
                path,
                at_view,
                ..
            } => {
                if let Some(view) = at_view {
                    log.coda_views.insert(slot, *view);
                }
                log.finals
                    .entry(slot)
                    .or_default()
                    .push((now, metablock.clone(), *path))
            }
            PqEvent::Evidence(evidence) => log.evidence.push((slot, evidence.clone())),
        }
    }

    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, halt: &PqHalt) {
        let mut log = self.0.lock().unwrap();
        log.halts.entry(slot).or_default().push((now, halt.clone()));
    }
}

pub struct Scenario {
    pub params: Params,
    pub plan: Plan,
    pub schedule: Arc<dyn ProposerSchedule + Send + Sync>,
    /// validators that never run
    pub silent: BTreeSet<u64>,
    pub faulty: BTreeMap<u64, Rewrite>,
    pub loss: f64,
    pub partitions: Vec<(Range<u64>, Vec<u64>)>,
    pub drop_if: Option<DropFn>,
    /// per-link one-way latency overrides in ms
    pub link_latency: HashMap<(u64, u64), u64>,
    pub seed: u64,
}

pub struct Outcome {
    pub swarm: Swarm,
    pub logs: BTreeMap<u64, LogHandle>,
    pub params: Params,
}

impl Scenario {
    pub fn new(params: Params) -> Self {
        Self {
            plan: params.timely(),
            schedule: fixed_schedule(params.k),
            params,
            silent: BTreeSet::new(),
            faulty: BTreeMap::new(),
            loss: 0.0,
            partitions: Vec::new(),
            drop_if: None,
            link_latency: HashMap::new(),
            seed: 1,
        }
    }

    fn conductor(&self) -> Conductor {
        let p = &self.params;
        let config = ConductorConfig::new(
            NonZeroU64::new(p.slots_per_window).unwrap(),
            NonZeroU64::new(p.sync_boundary).unwrap(),
            TimestampDelta::from_millis(p.slot_interval),
            GENESIS_DEADLINE,
            NonZeroU64::new(p.lag_threshold).unwrap(),
        )
        .unwrap();
        Conductor::genesis(config, ()).unwrap()
    }

    pub fn run_until_ms(self, until: u64) -> Outcome {
        let p = self.params.clone();
        let registry = Arc::new(Registry::build(&p, &self.plan, self.schedule.as_ref()));
        let mut builder = CadenceSwarmBuilder::<Wire, ChorusDAEvent>::new();
        builder.set_seed(self.seed);
        builder.track_logs(false);
        builder.set_network(Network::custom(Model {
            latency: p.latency,
            link_latency: self.link_latency.clone(),
            loss: self.loss,
            partitions: self.partitions.clone(),
            drop_if: self.drop_if.clone(),
        }));

        let mut logs = BTreeMap::new();
        for i in (0..p.n).filter(|i| !self.silent.contains(i)) {
            let id = NodeId::dummy(i);
            let log = LogHandle::default();
            logs.insert(i, log.clone());
            let context = context(id, p.n, self.schedule.clone());
            let da = scripted_da::script(id, &p, &self.plan, self.schedule.as_ref());
            let (repeater, feed) = Repeater::new(
                TimestampDelta::from_millis(p.repeat_interval),
                p.certificate_retention,
            );
            match self.faulty.get(&i) {
                None => {
                    let manager = SlotManager::<QuantumChorus>::new(p.config.clone(), context);
                    let mut runtime = CadenceRuntime::new(manager, self.conductor());
                    runtime.on_finalization((log, feed));
                    let runtime = ScriptedDa::new(runtime, repeater, da, registry.clone(), &p);
                    builder.add_generic_node(id, runtime);
                }
                Some(rewrite) => {
                    let config = FaultyConfig {
                        inner: p.config.clone(),
                        rewrite: rewrite.clone(),
                    };
                    let manager = SlotManager::<Faulty>::new(config, context);
                    let mut runtime = CadenceRuntime::new(manager, self.conductor());
                    runtime.on_finalization((log, feed));
                    let runtime = ScriptedDa::new(runtime, repeater, da, registry.clone(), &p);
                    builder.add_generic_node(id, runtime);
                }
            }
        }
        let mut swarm = builder.build();
        swarm.run_until(Timestamp::from_millis(until));
        Outcome {
            swarm,
            logs,
            params: p,
        }
    }
}

impl Outcome {
    pub fn log(&self, node: u64) -> std::sync::MutexGuard<'_, NodeLog> {
        self.logs[&node].0.lock().unwrap()
    }

    /// The single Final of `slot` at `node`.
    pub fn final_of(&self, node: u64, slot: u64) -> Option<(Timestamp, Metablock, PqPath)> {
        let log = self.log(node);
        let finals = log.finals.get(&Slot(slot))?;
        assert_eq!(finals.len(), 1, "node {node} finalized slot {slot} twice");
        Some(finals[0].clone())
    }

    pub fn finalized_slots(&self, node: u64) -> Vec<u64> {
        self.log(node)
            .finals
            .keys()
            .map(|slot| slot.get())
            .collect()
    }

    /// agreement, Final and halt at most once, halt matching the Final
    pub fn assert_safety(&self) {
        let mut decided: BTreeMap<Slot, (u64, Metablock)> = BTreeMap::new();
        for (node, log) in &self.logs {
            let log = log.0.lock().unwrap();
            for (slot, finals) in &log.finals {
                assert_eq!(finals.len(), 1, "node {node} finalized slot {slot:?} twice");
                let metablock = &finals[0].1;
                match decided.get(slot) {
                    Some((other, x)) => assert_eq!(
                        x, metablock,
                        "nodes {other} and {node} disagree on slot {slot:?}"
                    ),
                    None => {
                        decided.insert(*slot, (*node, metablock.clone()));
                    }
                }
            }
            for (slot, halts) in &log.halts {
                assert_eq!(halts.len(), 1, "node {node} halted slot {slot:?} twice");
                let finals = log.finals.get(slot).expect("halt implies finalization");
                assert_eq!(halts[0].1.metablock, finals[0].1);
                assert!(halts[0].0 >= finals[0].0);
            }
        }
    }

    /// every lane of `slot` is a root: the proposal inclusion of a timely
    /// slot whose proposers are all correct
    pub fn assert_all_included(&self, slot: u64) {
        for node in self.logs.keys() {
            let (_, metablock, _) = self.final_of(*node, slot).expect("finalized");
            assert!(
                metablock
                    .as_ref()
                    .into_iter()
                    .all(|entry| *entry != Entry::Bot),
                "node {node} excluded a proposer in slot {slot}"
            );
        }
    }

    pub fn ms(&self, at: Timestamp) -> i64 {
        (at.as_nanos() / 1_000_000) as i64
    }

    /// `at` relative to the deadline of `slot`, in ms
    pub fn rel(&self, slot: u64, at: Timestamp) -> i64 {
        self.ms(at) - self.ms(self.params.deadline(slot))
    }
}
