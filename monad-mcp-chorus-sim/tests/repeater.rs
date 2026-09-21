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

//! The repeater over a transport that drops packets: nothing else re-sends
//! a slot's votes or its certificate to a node that missed them.

use std::{collections::BTreeSet, num::NonZeroU64, sync::Arc, time::Duration};

use chorus::{
    CadenceDriverMsg, RepeaterConfig,
    conductor::{ConductorConfig, MonadConductor, acs::nop::NopAcs},
    proposers::header_auth,
    proposing::{PlannerConfig, ProposalPlanner},
    slot::chorus::{Chorus, ChorusConfig, ChorusContext, ChorusDAEvent, SlotFinalization},
    types::{
        HeaderAuth, NodeId, ProposerConfig, RotatingProposerSchedule, RoundRobinLeaderSchedule,
        Slot, SlotDeadline, Stake, Timestamp, TimestampDelta, ValidatorData,
    },
};
use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
use monad_mcp_chorus_sim::{CadenceSwarm, CadenceSwarmBuilder, MockDa};
use monad_sim::Time;
use monad_sim_swarm::Network;

const NODES: u64 = 4;
const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(10).unwrap();
const LAG_THRESHOLD: NonZeroU64 = SLOTS_PER_WINDOW;
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(8).unwrap();
const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
const GENESIS_DEADLINE: SlotDeadline = SlotDeadline::from_millis(100);
const LATENCY: TimestampDelta = TimestampDelta::from_millis(50);
const DELTA: TimestampDelta = TimestampDelta::from_millis(100);

type Conductor = MonadConductor<NopAcs<SlotDeadline>>;
type Msg = CadenceDriverMsg<Chorus, Conductor>;
type Schedule = RotatingProposerSchedule<RoundRobinLeaderSchedule>;

fn conductor() -> Conductor {
    let config = ConductorConfig::new(
        SLOTS_PER_WINDOW,
        SYNC_BOUNDARY_SLOTS,
        SLOT_INTERVAL,
        GENESIS_DEADLINE,
        LAG_THRESHOLD,
    )
    .unwrap();
    Conductor::genesis(config, ()).unwrap()
}

fn validator_data() -> ValidatorData {
    let validators = (0..NODES).map(NodeId::dummy).collect::<Vec<_>>();
    let valset = validators.iter().map(|id| (*id, Stake::from(1))).collect();
    let mapping = validators
        .iter()
        .map(|id| (*id, id.keypair().pubkey()))
        .collect();

    ValidatorData::new(valset, mapping)
}

// K = 1: one proposer per slot, rotating every slot
fn schedule(val_data: &Arc<ValidatorData>) -> Arc<Schedule> {
    let config = ProposerConfig {
        concurrent_proposers: 1,
        observation_cutoff: 0,
        rotation_slack: 1,
        slots_per_epoch: 1_000,
    };
    let algorithm = RoundRobinLeaderSchedule::new(&config);
    Arc::new(RotatingProposerSchedule::new(config, algorithm, val_data.clone()).unwrap())
}

/// The slots of `window` that some node finalized and another did not.
fn laggards<E: 'static>(
    swarm: &CadenceSwarm<Msg, E>,
    window: impl IntoIterator<Item = u64>,
) -> Vec<(NodeId, Slot)> {
    let finalized: Vec<(NodeId, BTreeSet<Slot>)> = (0..NODES)
        .map(NodeId::dummy)
        .map(|node| {
            (
                node,
                swarm.log().get_finalized_slots(node).into_iter().collect(),
            )
        })
        .collect();

    let mut behind = Vec::new();
    for slot in window.into_iter().map(Slot) {
        if !finalized.iter().any(|(_, slots)| slots.contains(&slot)) {
            continue;
        }
        for (node, slots) in &finalized {
            if !slots.contains(&slot) {
                behind.push((*node, slot));
            }
        }
    }
    behind
}

// A node cut off across one slot's exchange
mod partition {
    use super::*;

    // the cut spans the exchanges of slots 1 and 2: slot k's deadline is
    // at 100 + 100k ms and its certificate forms 100ms later
    const CUT_OFF: [Slot; 2] = [Slot(1), Slot(2)];
    const PARTITION: Duration = Duration::from_millis(250);
    const HEALS: Duration = Duration::from_millis(500);

    fn laggard() -> NodeId {
        NodeId::dummy(NODES - 1)
    }

    const WINDOW: std::ops::Range<u64> = 0..9;
    const RUN_UNTIL: Timestamp = Timestamp::from_millis(1_200);

    // ticks land after the heal, and the retention spans the whole window,
    // so the cap does not drop the certificate before the laggard hears it
    const REPEATER: RepeaterConfig = RepeaterConfig {
        interval: TimestampDelta::from_millis(150),
        certificate_retention: 10,
    };

    // A proposal-less swarm: the only traffic is the slot and conductor
    // messages, so nothing but the repeater can recover the cut-off slot.
    fn run(repeater: Option<RepeaterConfig>) -> CadenceSwarm<Msg, ChorusDAEvent> {
        let network = Network::reliable(LATENCY.as_duration())
            .partition(Time(0) + PARTITION..Time(0) + HEALS, [[laggard()]]);

        let mut builder = CadenceSwarmBuilder::new();
        builder.set_network(network);
        if let Some(repeater) = repeater {
            builder.set_repeater(repeater);
        }

        let val_data = Arc::new(validator_data());
        let proposers = schedule(&val_data);
        for i in 0..NODES {
            let id = NodeId::dummy(i);
            let context = ChorusContext {
                node_id: id,
                key: Arc::new(id.keypair()),
                validator_data: val_data.clone(),
                header_auth: Arc::new(HeaderAuth::new(|_, _| None)),
                proposers: proposers.clone(),
            };
            builder.add_node::<Chorus, _>(id, conductor(), ChorusConfig { delta: DELTA }, context);
        }

        let mut swarm = builder.build();
        swarm.run_until(RUN_UNTIL);
        swarm
    }

    fn missing(swarm: &CadenceSwarm<Msg, ChorusDAEvent>, node: NodeId) -> Vec<Slot> {
        let finalized: BTreeSet<Slot> = swarm.log().get_finalized_slots(node).into_iter().collect();
        WINDOW
            .map(Slot)
            .filter(|slot| !finalized.contains(slot))
            .collect()
    }

    /// The premise: nothing re-sends what the cut-off node missed, so it
    /// never finalizes those slots.
    #[test]
    fn without_the_repeater_the_cut_off_node_never_recovers_its_slots() {
        let swarm = run(None);

        for node in (0..NODES - 1).map(NodeId::dummy) {
            assert_eq!(missing(&swarm, node), [], "{node:?} fell behind");
        }
        assert_eq!(missing(&swarm, laggard()), CUT_OFF);
    }

    /// ... and the repeater's certificate re-broadcast recovers them.
    #[test]
    fn the_repeater_recovers_the_slots_the_cut_off_node_missed() {
        let swarm = run(Some(REPEATER));

        for node in (0..NODES).map(NodeId::dummy) {
            assert_eq!(missing(&swarm, node), [], "{node:?} fell behind");
        }
    }
}

// The same property under uniform packet loss, over a seed sweep
mod lossy {
    use super::*;

    const LOSS: f64 = 0.04;
    const SEEDS: std::ops::Range<u64> = 0..64;
    const LEAD: TimestampDelta = TimestampDelta::from_millis(60);
    const WINDOW: std::ops::Range<u64> = 0..40;
    // four windows of slots, and a tail of several repeater intervals
    const RUN_UNTIL: Timestamp = Timestamp::from_millis(6_000);

    // retention spans at least two ticks (300 ms at one slot per 100 ms),
    // so a certificate is repeated more than the guaranteed once
    const REPEATER: RepeaterConfig = RepeaterConfig {
        interval: TimestampDelta::from_millis(300),
        certificate_retention: 7,
    };

    fn run(seed: u64, repeater: Option<RepeaterConfig>) -> CadenceSwarm<Msg, ChorusDAEvent> {
        let mut builder = CadenceSwarmBuilder::new();
        builder.set_seed(seed);
        builder.set_network(Network::reliable(LATENCY.as_duration()).loss(LOSS));
        if let Some(repeater) = repeater {
            builder.set_repeater(repeater);
        }

        let val_data = Arc::new(validator_data());
        let proposers = schedule(&val_data);
        for i in 0..NODES {
            let id = NodeId::dummy(i);
            let header_auth = Arc::new(header_auth(proposers.clone()));
            let da = Arc::new(MockDa::new(id, header_auth.clone()));
            let context = ChorusContext {
                node_id: id,
                key: Arc::new(id.keypair()),
                validator_data: val_data.clone(),
                header_auth,
                proposers: proposers.clone(),
            };
            let planner = ProposalPlanner::new(
                id,
                proposers.clone(),
                PlannerConfig {
                    lead: LEAD,
                    observation_cutoff: proposers.config().observation_cutoff,
                },
            );

            builder.add_proposer_node::<Chorus, _, _>(
                id,
                conductor(),
                ChorusConfig { delta: DELTA },
                context,
                planner,
                da,
                |_now: Timestamp, _slot: Slot, _data: &SlotFinalization| {},
            );
        }

        let mut swarm = builder.build();
        swarm.run_until(RUN_UNTIL);
        swarm
    }

    /// The premise: at this loss rate some seed really does leave a node
    /// behind a slot its peers finalized, so the recovery test below is not
    /// vacuous.
    #[test]
    fn without_the_repeater_a_lossy_run_leaves_a_node_behind() {
        let behind: Vec<(u64, NodeId, Slot)> = SEEDS
            .flat_map(|seed| {
                let swarm = run(seed, None);
                laggards(&swarm, WINDOW)
                    .into_iter()
                    .map(move |(node, slot)| (seed, node, slot))
                    .collect::<Vec<_>>()
            })
            .collect();

        assert!(
            !behind.is_empty(),
            "no seed left a node behind at {LOSS} loss"
        );
    }

    /// ... and with the repeater no node is left behind a slot its peers
    /// finalized.
    #[test]
    fn the_repeater_leaves_no_node_behind_on_a_lossy_network() {
        for seed in SEEDS {
            let swarm = run(seed, Some(REPEATER));
            let behind = laggards(&swarm, WINDOW);
            assert!(behind.is_empty(), "seed {seed}: {behind:?} never caught up");
        }
    }
}
