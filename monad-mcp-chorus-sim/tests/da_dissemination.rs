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

//! Which path a slot takes is decided by how far its proposal got: the
//! disseminator serves k of the n validators, and with n = 3f+1,
//!   k >= 2f+1     every validator votes positive, the fast path commits;
//!   k <= f        no root ever decodes, the fast path commits it absent;
//!   f < k < 2f+1  neither verdict has a supermajority, so the fast path
//!                 cannot commit and the fallback path decides the slot --
//!                 positively, because the k holders relay in time for
//!                 everyone to decode before the fallback vote -- or
//!                 negatively, when those relays are lost and no one decodes.
//!
//! What a single `SimDA` sends and reports is unit-tested in the crate itself.

use std::{collections::BTreeSet, num::NonZeroU64, ops::Range, sync::Arc};

use chorus::{
    CadenceNodeMsg, NodeMessage,
    conductor::{ConductorConfig, MonadConductor, acs::nop::NopAcs},
    slot::{
        chorus::{Chorus, ChorusConfig, ChorusContext, SlotFinalization},
        fallback::Entry,
    },
    types::{NodeId, Slot, SlotDeadline, Stake, Timestamp, TimestampDelta, ValidatorData},
};
use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
use monad_mcp_chorus_sim::{
    CadenceSwarmBuilder, DaDisseminator, DaWire, DisseminationPlan, Network, SimDA, SlotLog,
    Upstream, disseminator_id, root_of,
};

const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(10).unwrap();
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(8).unwrap();
const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
const GENESIS_DEADLINE: SlotDeadline = SlotDeadline::from_millis(100);
const LATENCY: TimestampDelta = TimestampDelta::from_millis(50);
const DELTA: TimestampDelta = TimestampDelta::from_millis(100);

const NUM_PROPOSALS: usize = 1;
const PROPOSER: usize = 0;

type Conductor = MonadConductor<NopAcs<SlotDeadline>>;
type NodeMsg = CadenceNodeMsg<Chorus, Conductor, SimDA>;
type Logs = Vec<SlotLog<SlotFinalization>>;

fn conductor() -> Conductor {
    let config = ConductorConfig::new(
        SLOTS_PER_WINDOW,
        SYNC_BOUNDARY_SLOTS,
        SLOT_INTERVAL,
        GENESIS_DEADLINE,
    )
    .unwrap();
    MonadConductor::genesis(config, ()).unwrap()
}

fn deadline_of(slot: Slot) -> Timestamp {
    GENESIS_DEADLINE
        .checked_add_deltas(SLOT_INTERVAL, slot.get())
        .unwrap()
}

fn gen_validator_data(n: u64) -> ValidatorData {
    let validators = (0..n).map(NodeId::dummy).collect::<Vec<_>>();
    let valset = validators.iter().map(|id| (*id, Stake::from(1))).collect();
    let mapping = validators
        .iter()
        .map(|id| (*id, id.keypair().pubkey()))
        .collect();

    ValidatorData::new(valset, mapping)
}

/// n validators plus the disseminator authoring every proposal under `plans`
fn run(
    n: u64,
    slots: Range<u64>,
    plans: impl Fn(Slot, usize) -> Vec<DisseminationPlan> + 'static,
) -> Logs {
    run_on(reliable(), n, slots, plans)
}

/// `run` over a network of the caller's choosing, so faults can be layered on
fn run_on(
    network: Network<NodeId, NodeMsg>,
    n: u64,
    slots: Range<u64>,
    plans: impl Fn(Slot, usize) -> Vec<DisseminationPlan> + 'static,
) -> Logs {
    let mut builder = CadenceSwarmBuilder::<NodeMsg>::new();
    builder.set_network(network);

    let val_data = Arc::new(gen_validator_data(n));
    let config = ChorusConfig {
        delta: DELTA,
        num_proposals: NUM_PROPOSALS,
    };

    let logs: Logs = (0..n)
        .map(|i| {
            let node_id = NodeId::dummy(i);
            let context = ChorusContext {
                node_id,
                key: Arc::new(node_id.keypair()),
                validator_data: val_data.clone(),
            };
            builder.add_node_with_da::<Chorus, _, _>(
                node_id,
                conductor(),
                config.clone(),
                context,
                SimDA::new(node_id, val_data.clone()),
            )
        })
        .collect();

    let last = Slot(slots.end - 1);
    let disseminator = DaDisseminator::new(
        (0..n).map(NodeId::dummy).collect(),
        NUM_PROPOSALS,
        slots,
        DELTA,
        deadline_of,
        plans,
    );
    builder.add_generic_node(disseminator_id(), disseminator);

    let mut swarm = builder.build();
    swarm.run_until(deadline_of(last) + fallback_latency());
    logs
}

fn reliable() -> Network<NodeId, NodeMsg> {
    Network::reliable(LATENCY.as_duration())
}

/// D_s + 2 latencies: batch votes form the fast block, commit votes the QC
fn fast_latency() -> TimestampDelta {
    LATENCY.checked_mul(2).unwrap()
}

/// D_s + 2Δ to enter the fallback path, then the MVBA's first view:
/// pre-prepare, prepare, commit, one crossing each
fn fallback_latency() -> TimestampDelta {
    DELTA.checked_mul(2).unwrap() + LATENCY.checked_mul(3).unwrap()
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum Path {
    Fast,
    Fallback,
}

fn path_of(finalization: &SlotFinalization) -> Path {
    match finalization {
        SlotFinalization::Fallback(_) => Path::Fallback,
        // the fast commit certificate's vote type is crate-private
        _ => Path::Fast,
    }
}

/// Every validator finalized `slot` the same way, on time, with the same
/// verdict for the proposal
fn assert_agreed(logs: &Logs, slot: u64, path: Path, entry: Entry) {
    let slot = Slot(slot);
    let at = deadline_of(slot)
        + match path {
            Path::Fast => fast_latency(),
            Path::Fallback => fallback_latency(),
        };

    let mut agreed: Option<SlotFinalization> = None;
    for (i, log) in logs.iter().enumerate() {
        let node = NodeId::dummy(i as u64);
        let finalized = log.borrow();
        let (finalized_at, _, data) = finalized
            .iter()
            .find(|(_, finalized_slot, _)| *finalized_slot == slot)
            .unwrap_or_else(|| panic!("{node:?} did not finalize {slot:?}"));

        assert_eq!(
            path_of(data),
            path,
            "{node:?} finalized {slot:?} on the wrong path"
        );
        assert_eq!(
            data.entries()[PROPOSER],
            entry,
            "{node:?} committed the wrong verdict for {slot:?}"
        );
        assert_eq!(
            *finalized_at, at,
            "{node:?} finalized {slot:?} off schedule"
        );

        match &agreed {
            None => agreed = Some(data.clone()),
            Some(agreed) => assert_eq!(data, agreed, "{node:?} finalized {slot:?} differently"),
        }
    }
}

fn positive(slot: u64) -> Entry {
    Entry::Positive {
        root: root_of(Slot(slot), PROPOSER),
    }
}

/// Slot s is reached by exactly s validators, so one run walks the whole
/// range from "nobody has it" to "everybody has it"
#[test]
fn sweep_k_over_four_validators() {
    // n = 4, f = 1
    let logs = run(4, 0..5, |slot, _j| {
        vec![DisseminationPlan::Reach(slot.get() as usize)]
    });

    assert_agreed(&logs, 0, Path::Fast, Entry::Negative);
    assert_agreed(&logs, 1, Path::Fast, Entry::Negative);
    assert_agreed(&logs, 2, Path::Fallback, positive(2));
    assert_agreed(&logs, 3, Path::Fast, positive(3));
    assert_agreed(&logs, 4, Path::Fast, positive(4));
}

/// The same walk with f = 2, so the split band f < k < 2f+1 is two slots wide
#[test]
fn sweep_k_over_seven_validators() {
    // n = 7, f = 2
    let logs = run(7, 0..8, |slot, _j| {
        vec![DisseminationPlan::Reach(slot.get() as usize)]
    });

    for slot in 0..=2 {
        assert_agreed(&logs, slot, Path::Fast, Entry::Negative);
    }
    for slot in 3..=4 {
        assert_agreed(&logs, slot, Path::Fallback, positive(slot));
    }
    for slot in 5..=7 {
        assert_agreed(&logs, slot, Path::Fast, positive(slot));
    }
}

/// A proposal that reaches everyone, but only after the deadline it had to
/// beat, is committed absent by the fast path
#[test]
fn late_delivery_is_voted_absent() {
    let logs = run(4, 0..3, |_slot, _j| {
        vec![DisseminationPlan::Delay {
            recipients: (0..4).map(NodeId::dummy).collect(),
            offset: DELTA,
        }]
    });

    for slot in 0..=2 {
        assert_agreed(&logs, slot, Path::Fast, Entry::Negative);
    }
}

/// The same delayed delivery, offset little enough to still land before the
/// deadline (at `D_s - Delta + 25ms + latency`): it is voted present, so what
/// makes the late one absent is the offset, not the plan dropping chunks
#[test]
fn delivery_before_the_deadline_is_voted_present() {
    let logs = run(4, 0..3, |_slot, _j| {
        vec![DisseminationPlan::Delay {
            recipients: (0..4).map(NodeId::dummy).collect(),
            offset: TimestampDelta::from_millis(25),
        }]
    });

    for slot in 0..=2 {
        assert_agreed(&logs, slot, Path::Fast, positive(slot));
    }
}

/// The fast path cannot commit a slot its validators disagree about, and only
/// the DA layer can make them disagree: two of the four hold the proposal and
/// vote it positive, two hold nothing and vote it absent. Neither verdict is a
/// supermajority, so no fast QC forms and every validator enters the MVBA
#[test]
fn a_slot_the_fast_path_cannot_commit_finalizes_through_the_fallback() {
    let logs = run(4, 0..3, |_slot, _j| vec![DisseminationPlan::Reach(2)]);

    for slot in 0..=2 {
        assert_agreed(&logs, slot, Path::Fallback, positive(slot));
    }
}

/// f+1 validators are served in time and the rest too late to vote on it. The
/// on-time holders relay at D_s, so everyone decodes well before the fallback
/// vote and the fallback path commits it positive
#[test]
fn late_delivery_splits_the_vote_into_a_positive_fallback() {
    let on_time: BTreeSet<NodeId> = (0..2).map(NodeId::dummy).collect();
    let late: BTreeSet<NodeId> = (2..4).map(NodeId::dummy).collect();

    let logs = run(4, 0..3, move |_slot, _j| {
        vec![
            DisseminationPlan::ReachSet(on_time.clone()),
            DisseminationPlan::Delay {
                recipients: late.clone(),
                offset: DELTA,
            },
        ]
    });

    for slot in 0..=2 {
        assert_agreed(&logs, slot, Path::Fallback, positive(slot));
    }
}

/// Two of the four are served, so the fast path splits, and every relay is
/// lost, so no one ever holds more than its own share. The positive weak QC
/// exists but no root decodes, so all four cast a negative fallback entry and
/// the fallback path commits the slot absent
#[test]
fn lost_relays_turn_a_split_vote_into_a_negative_fallback() {
    let network = reliable().drop_if(|_link, msg: &NodeMsg| {
        matches!(
            msg,
            NodeMessage::DA(DaWire::Chunk {
                upstream: Upstream::Owner(_),
                ..
            })
        )
    });

    let logs = run_on(network, 4, 0..3, |_slot, _j| {
        vec![DisseminationPlan::Reach(2)]
    });

    for slot in 0..=2 {
        assert_agreed(&logs, slot, Path::Fallback, Entry::Negative);
    }
}
