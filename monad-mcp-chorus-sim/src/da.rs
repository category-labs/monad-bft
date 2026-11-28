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

//! A simulated data-availability layer: chunks carry no payload, so a chunk is
//! just "one validator's share of root r", and a proposal decodes once shares
//! from f+1 distinct validators are in hand. Dissemination is driven by a
//! [`DaDisseminator`] node that authors every proposal and follows a
//! [`DisseminationPlan`] per (slot, proposer), which is what lets a test say
//! "this proposal reached k of n validators". Validators relay their own share
//! when they vote positive, and re-serve every validator its share when they
//! cast a positive fallback entry.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::Arc,
};

use chorus::{
    DAOutput, DASink, DataAvailability, NodeEvent, NodeMessage, Runtime, SlotLifecycle, WakeId,
    slot::chorus::{Chorus, ChorusDACommand, ChorusDAEvent, ProposalDAEvent},
    types::{
        MerkleRoot, NodeId, OpaqueChunkHeader, ProposalIndex, ProposalMeta, ProposalSignature,
        Slot, Timestamp, TimestampDelta, Validated, ValidatorData,
    },
};
use monad_mcp_chorus::{
    spec::{Stake as _, validator::ValidatorData as _},
    stub as chorus,
};

/// One validator's share of a proposal. Who it came from is what matters here,
/// not what it carries: from the author or a recaster it is the recipient's own
/// share, from an owner it is the owner's
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DaWire {
    Chunk {
        slot: Slot,
        j: ProposalIndex,
        meta: ProposalMeta,
        upstream: Upstream,
    },
}

/// Who handed us the chunk: the proposal's author, the validator whose own
/// share it is relaying, or a fallback-yes voter that recovered the proposal and
/// re-encoded our share
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Upstream {
    Author,
    Owner(NodeId),
    Recast(NodeId),
}

/// The root the author uses for proposer `j` in `slot`
pub fn root_of(slot: Slot, j: ProposalIndex) -> MerkleRoot {
    MerkleRoot(slot.get() * 1000 + j as u64)
}

/// The header the author signs for proposer `j` in `slot`
pub fn meta_of(slot: Slot, j: ProposalIndex) -> ProposalMeta {
    ProposalMeta {
        root: root_of(slot, j),
        sig: ProposalSignature,
        opaque_header: OpaqueChunkHeader,
    }
}

/// The author of every proposal: a swarm node outside the validator set
pub fn disseminator_id() -> NodeId {
    NodeId::dummy(u64::MAX)
}

/// One validator's view of the chunks in flight
pub struct SimDA {
    me: NodeId,
    validator_data: Arc<ValidatorData>,
    slots: BTreeMap<Slot, HashMap<(ProposalIndex, MerkleRoot), ChunkState>>,
    outputs: VecDeque<DAOutput<ChorusDAEvent, DaWire>>,
}

struct ChunkState {
    meta: ProposalMeta,
    /// our own share arrived from the author
    from_author: bool,
    /// our own share arrived re-encoded from a fallback-yes voter
    recast: bool,
    /// validators that relayed their own share to us
    owners: BTreeSet<NodeId>,
    decoded: bool,
}

impl SimDA {
    pub fn new(me: NodeId, validator_data: Arc<ValidatorData>) -> Self {
        Self {
            me,
            validator_data,
            slots: BTreeMap::new(),
            outputs: VecDeque::new(),
        }
    }

    fn emit(&mut self, slot: Slot, j: ProposalIndex, event: ProposalDAEvent) {
        self.outputs
            .push_back(DAOutput::Event(slot, ChorusDAEvent { j, event }));
    }

    fn state(&self, slot: Slot, j: ProposalIndex, root: MerkleRoot) -> Option<&ChunkState> {
        self.slots.get(&slot)?.get(&(j, root))
    }

    /// Relay our own share of a root we were served, as the paper has positive
    /// voters do
    fn relay(&mut self, slot: Slot, j: ProposalIndex, root: MerkleRoot) {
        let held = self
            .state(slot, j, root)
            .filter(|state| state.from_author)
            .map(|state| state.meta.clone());

        if let Some(meta) = held {
            self.outputs.push_back(DAOutput::Broadcast(DaWire::Chunk {
                slot,
                j,
                meta,
                upstream: Upstream::Owner(self.me),
            }));
        }
    }

    /// Recover the proposal from the shares we hold, re-encode it and serve
    /// every other validator its own share, as the paper has fallback-yes
    /// voters do
    fn recast(&mut self, slot: Slot, j: ProposalIndex, root: MerkleRoot) {
        // consensus casts a positive fallback entry only for a root it saw
        // decode, and decoding is what lets us re-encode every share
        let Some(meta) = self
            .state(slot, j, root)
            .filter(|state| state.decoded)
            .map(|state| state.meta.clone())
        else {
            return;
        };

        let validator_data = self.validator_data.clone();
        for to in validator_data.nodes().copied().filter(|v| *v != self.me) {
            self.outputs.push_back(DAOutput::Unicast {
                to,
                message: DaWire::Chunk {
                    slot,
                    j,
                    meta: meta.clone(),
                    upstream: Upstream::Recast(self.me),
                },
            });
        }
    }
}

/// The shares we hold are our own (when the author or a recaster served us)
/// plus every relayed one; f+1 of them recover the proposal
fn is_decodable(validator_data: &ValidatorData, me: &NodeId, state: &ChunkState) -> bool {
    let holds_own = state.from_author || state.recast;
    let holders = state.owners.iter().chain(holds_own.then_some(me));
    let stake = validator_data.sum_stake(holders);

    stake > validator_data.total_stake().honest_threshold()
}

impl DASink<ChorusDACommand> for SimDA {
    fn handle_lifecycle(&mut self, slot: Slot, lifecycle: SlotLifecycle) {
        match lifecycle {
            SlotLifecycle::Opened => {}
            SlotLifecycle::Completed => {
                self.slots.remove(&slot);
            }
        }
    }

    fn handle_command(&mut self, slot: Slot, command: ChorusDACommand) {
        match command {
            ChorusDACommand::SlotVoted { positive } => {
                for (j, root) in positive {
                    self.relay(slot, j, root);
                }
            }
            ChorusDACommand::FallbackEntryCast { j, root } => self.recast(slot, j, root),
            // the header is already recorded by consensus itself; a real DA
            // layer would pin the root here
            ChorusDACommand::ObserveProposal { .. } => {}
        }
    }
}

impl DataAvailability<Chorus> for SimDA {
    type WireMsg = DaWire;

    fn handle_message(&mut self, _now: Timestamp, message: Validated<DaWire>) {
        let (chunk, _from) = message.destructure();
        let DaWire::Chunk {
            slot,
            j,
            meta,
            upstream,
        } = chunk;

        // our own relay comes back to us over the broadcast, carrying a share
        // we already hold
        if upstream == Upstream::Owner(self.me) {
            return;
        }

        let root = meta.root;
        let mut events = Vec::new();

        let proposals = self.slots.entry(slot).or_default();
        if !proposals.contains_key(&(j, root)) {
            events.push(ProposalDAEvent::HeaderSeen(meta.clone()));
        }

        let state = proposals.entry((j, root)).or_insert_with(|| ChunkState {
            meta,
            from_author: false,
            recast: false,
            owners: BTreeSet::new(),
            decoded: false,
        });

        match upstream {
            Upstream::Author if !state.from_author => {
                state.from_author = true;
                events.push(ProposalDAEvent::ProposerObligationFulfilled(root));
            }
            // our own share, but not the author's doing: nothing to report
            // beyond what it lets us decode
            Upstream::Recast(_) => state.recast = true,
            Upstream::Owner(owner) if state.owners.insert(owner) => {
                events.push(ProposalDAEvent::OwnerObligationFulfilled { owner, root });
            }
            _ => {}
        }

        if !state.decoded && is_decodable(&self.validator_data, &self.me, state) {
            state.decoded = true;
            events.push(ProposalDAEvent::Decoded(root));
        }

        for event in events {
            self.emit(slot, j, event);
        }
    }

    fn wake(&mut self, _now: Timestamp, _wake: WakeId) {}

    fn poll(&mut self) -> Option<DAOutput<ChorusDAEvent, DaWire>> {
        self.outputs.pop_front()
    }
}

/// What the author does with proposer `j`'s chunks in a slot
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DisseminationPlan {
    /// serve the first `k` validators in NodeId order
    Reach(usize),
    ReachSet(BTreeSet<NodeId>),
    /// serve these validators `offset` late, i.e. at `D_s - Delta + offset`
    Delay {
        recipients: BTreeSet<NodeId>,
        offset: TimestampDelta,
    },
    Silent,
}

/// The sole author of every proposal: a swarm node outside the validator set
/// that serves each validator its own chunk at `D_s - Delta`, or does not.
/// It ignores everything it receives, the validators' own relays included.
pub struct DaDisseminator<CM> {
    validators: Vec<NodeId>,
    num_proposals: usize,
    slots: std::ops::Range<u64>,
    delta: TimestampDelta,
    deadline_of: Box<dyn Fn(Slot) -> Timestamp>,
    plans_of: Box<dyn Fn(Slot, ProposalIndex) -> Vec<DisseminationPlan>>,
    pending: HashMap<WakeId, (Slot, ProposalIndex, Vec<NodeId>)>,
    next_wake: WakeId,
    outbox: VecDeque<NodeEvent<NodeMessage<CM, DaWire>>>,
}

impl<CM> DaDisseminator<CM> {
    /// `plans_of` may return several plans for one (slot, j) so a proposal can
    /// reach some validators on time and others late
    pub fn new(
        validators: Vec<NodeId>,
        num_proposals: usize,
        slots: std::ops::Range<u64>,
        delta: TimestampDelta,
        deadline_of: impl Fn(Slot) -> Timestamp + 'static,
        plans_of: impl Fn(Slot, ProposalIndex) -> Vec<DisseminationPlan> + 'static,
    ) -> Self {
        let mut validators = validators;
        validators.sort();

        Self {
            validators,
            num_proposals,
            slots,
            delta,
            deadline_of: Box::new(deadline_of),
            plans_of: Box::new(plans_of),
            pending: HashMap::new(),
            next_wake: WakeId::FIRST,
            outbox: VecDeque::new(),
        }
    }

    fn recipients(&self, plan: &DisseminationPlan) -> (Vec<NodeId>, TimestampDelta) {
        match plan {
            DisseminationPlan::Reach(k) => (
                self.validators.iter().copied().take(*k).collect(),
                TimestampDelta::ZERO,
            ),
            DisseminationPlan::ReachSet(recipients) => {
                (recipients.iter().copied().collect(), TimestampDelta::ZERO)
            }
            DisseminationPlan::Delay { recipients, offset } => {
                (recipients.iter().copied().collect(), *offset)
            }
            DisseminationPlan::Silent => (Vec::new(), TimestampDelta::ZERO),
        }
    }
}

impl<CM> Runtime<NodeMessage<CM, DaWire>> for DaDisseminator<CM> {
    fn init(&mut self) {
        for s in self.slots.clone() {
            let slot = Slot(s);
            for j in 0..self.num_proposals {
                for plan in (self.plans_of)(slot, j) {
                    let (recipients, offset) = self.recipients(&plan);
                    if recipients.is_empty() {
                        continue;
                    }

                    let deadline = (self.deadline_of)(slot).as_nanos();
                    let at = Timestamp::from_nanos(
                        deadline.saturating_sub(u128::from(self.delta.as_nanos())),
                    ) + offset;

                    let id = self.next_wake.post_increment();
                    self.pending.insert(id, (slot, j, recipients));
                    self.outbox.push_back(NodeEvent::Wake(at, id));
                }
            }
        }
    }

    fn wake(&mut self, _now: Timestamp, wake: WakeId) {
        let Some((slot, j, recipients)) = self.pending.remove(&wake) else {
            return;
        };

        for to in recipients {
            let chunk = DaWire::Chunk {
                slot,
                j,
                meta: meta_of(slot, j),
                upstream: Upstream::Author,
            };
            self.outbox.push_back(NodeEvent::Unicast {
                to,
                message: NodeMessage::DA(chunk),
            });
        }
    }

    fn receive(&mut self, _now: Timestamp, _message: Validated<NodeMessage<CM, DaWire>>) {}

    fn poll(&mut self) -> Option<NodeEvent<NodeMessage<CM, DaWire>>> {
        self.outbox.pop_front()
    }
}

/// Direct-drive tests: one `SimDA`, no network, no consensus. They check what
/// a swarm cannot see -- the exact events reported and shares served
#[cfg(test)]
mod tests {
    use monad_mcp_chorus::spec::KeyPair as _;

    use super::{chorus::types::Stake, *};

    const PROPOSER: ProposalIndex = 0;

    /// The slot every test here drives
    const SLOT: Slot = Slot(1);

    /// n = 4, f = 1: a root decodes once two of the four shares are in hand
    fn node(i: u64) -> SimDA {
        let validators = (0..4).map(NodeId::dummy).collect::<Vec<_>>();
        let valset = validators.iter().map(|id| (*id, Stake::from(1))).collect();
        let mapping = validators
            .iter()
            .map(|id| (*id, id.keypair().pubkey()))
            .collect();
        SimDA::new(
            NodeId::dummy(i),
            Arc::new(ValidatorData::new(valset, mapping)),
        )
    }

    /// A share of `PROPOSER`'s proposal in `SLOT`, handed to us by `upstream`
    fn chunk(upstream: Upstream) -> DaWire {
        DaWire::Chunk {
            slot: SLOT,
            j: PROPOSER,
            meta: meta_of(SLOT, PROPOSER),
            upstream,
        }
    }

    fn ingest(da: &mut SimDA, upstream: Upstream, from: NodeId) {
        da.handle_message(
            Timestamp::GENESIS,
            Validated::new_unchecked(chunk(upstream), from),
        );
    }

    fn cast(da: &mut SimDA) {
        da.handle_command(
            SLOT,
            ChorusDACommand::FallbackEntryCast {
                j: PROPOSER,
                root: root_of(SLOT, PROPOSER),
            },
        );
    }

    /// Everything the DA layer has queued, split by kind. `DAOutput` has no
    /// `PartialEq`, so we take it apart here
    fn drain(da: &mut SimDA) -> (Vec<ProposalDAEvent>, Vec<(NodeId, DaWire)>, Vec<DaWire>) {
        let (mut events, mut unicasts, mut broadcasts) = (Vec::new(), Vec::new(), Vec::new());
        while let Some(output) = da.poll() {
            match output {
                DAOutput::Event(_, ChorusDAEvent { event, .. }) => events.push(event),
                DAOutput::Unicast { to, message } => unicasts.push((to, message)),
                DAOutput::Broadcast(message) => broadcasts.push(message),
                DAOutput::Schedule(..) => panic!("the sim DA schedules no wakes"),
            }
        }
        (events, unicasts, broadcasts)
    }

    fn decoded() -> ProposalDAEvent {
        ProposalDAEvent::Decoded(root_of(SLOT, PROPOSER))
    }

    /// A caster the author never served still owes every validator its share: the
    /// shares it decoded from are enough to re-encode them all
    #[test]
    fn a_fallback_yes_voter_serves_every_other_validator_its_share() {
        let mut da = node(2);
        ingest(&mut da, Upstream::Owner(NodeId::dummy(0)), NodeId::dummy(0));
        ingest(&mut da, Upstream::Owner(NodeId::dummy(1)), NodeId::dummy(1));

        let (events, ..) = drain(&mut da);
        assert_eq!(events.last(), Some(&decoded()));

        cast(&mut da);
        let (events, unicasts, broadcasts) = drain(&mut da);
        assert!(events.is_empty(), "a recast reports nothing to consensus");
        assert!(broadcasts.is_empty(), "a recast is unicast per assignee");
        assert_eq!(
            unicasts,
            (0..4)
                .filter(|i| *i != 2)
                .map(|i| (NodeId::dummy(i), chunk(Upstream::Recast(NodeId::dummy(2)))))
                .collect::<Vec<_>>()
        );
    }

    /// Consensus never casts a positive entry for a root it did not decode, and
    /// without the shares to re-encode there is nothing to serve
    #[test]
    fn a_root_we_could_not_decode_is_not_recast() {
        let mut da = node(0);
        ingest(&mut da, Upstream::Author, disseminator_id());

        let (events, ..) = drain(&mut da);
        assert_eq!(
            events,
            vec![
                ProposalDAEvent::HeaderSeen(meta_of(SLOT, PROPOSER)),
                ProposalDAEvent::ProposerObligationFulfilled(root_of(SLOT, PROPOSER)),
            ]
        );

        cast(&mut da);
        let (events, unicasts, broadcasts) = drain(&mut da);
        assert!(events.is_empty());
        assert!(unicasts.is_empty());
        assert!(broadcasts.is_empty());
    }

    /// A recaster hands us our own share, which is one holder toward decoding:
    /// with one owner's relay already in hand, it is what tips the root over
    #[test]
    fn a_recast_share_counts_as_our_own_toward_decoding() {
        let root = root_of(SLOT, PROPOSER);
        let mut da = node(3);

        ingest(&mut da, Upstream::Owner(NodeId::dummy(0)), NodeId::dummy(0));
        let (events, ..) = drain(&mut da);
        assert_eq!(
            events,
            vec![
                ProposalDAEvent::HeaderSeen(meta_of(SLOT, PROPOSER)),
                ProposalDAEvent::OwnerObligationFulfilled {
                    owner: NodeId::dummy(0),
                    root
                },
            ]
        );

        ingest(
            &mut da,
            Upstream::Recast(NodeId::dummy(1)),
            NodeId::dummy(1),
        );
        let (events, ..) = drain(&mut da);
        assert_eq!(events, vec![decoded()]);

        // a slow author still fulfils its obligation, and decoding happens once
        ingest(&mut da, Upstream::Author, disseminator_id());
        let (events, ..) = drain(&mut da);
        assert_eq!(
            events,
            vec![ProposalDAEvent::ProposerObligationFulfilled(root)]
        );
    }

    /// Two recasters gave us one share -- ours -- not two: a recaster's own share
    /// never reaches us, so it is not a holder we can count
    #[test]
    fn recasts_do_not_stand_in_for_the_casters_shares() {
        let mut da = node(3);

        ingest(
            &mut da,
            Upstream::Recast(NodeId::dummy(0)),
            NodeId::dummy(0),
        );
        let (events, ..) = drain(&mut da);
        assert_eq!(
            events,
            vec![ProposalDAEvent::HeaderSeen(meta_of(SLOT, PROPOSER))]
        );

        ingest(
            &mut da,
            Upstream::Recast(NodeId::dummy(1)),
            NodeId::dummy(1),
        );
        let (events, ..) = drain(&mut da);
        assert!(events.is_empty());
    }
}
