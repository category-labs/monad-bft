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

//! The fallback path as Chorus runs it: the leaderless dummy vote and the
//! leader-based MVBA, started together and deciding by whichever certificate
//! forms first

use std::collections::VecDeque;

use alloy_rlp::{Decodable, Encodable};

use super::{
    super::types::{Lanes, Slot, ValidatorData, ValidatorIndex, VoteMsg},
    EnterFallbackCert, Entry, FallbackCommitQc, MVBAOutput, Metablock, Mvba, ValidateCert,
    ValidateInput, Votable,
    leaderless::{DummyQc, LeaderlessEffect, LeaderlessMessage, LeaderlessRound, NfCert},
    monad_mvba::{
        MakesValidationContext, MonadMvba, MvbaContext, MvbaMessage, TimerEvent, ValidationContext,
    },
};

/// The value the core MVBA runs over: a metablock with the `fbcert` the paper
/// makes a field of it, so the certificate travels wherever the block does. A
/// fast metablock carries no `fbcert`, any other one
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CertifiedMetablock(Certified);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Certified {
    Fast(Metablock),
    NonFast {
        block: Metablock,
        fbcert: EnterFallbackCert,
    },
}

impl CertifiedMetablock {
    /// `None` unless `block` is fast
    pub fn fast(block: Metablock) -> Option<Self> {
        block.is_fast().then_some(Self(Certified::Fast(block)))
    }

    /// `None` if `block` is fast
    pub fn non_fast(block: Metablock, fbcert: EnterFallbackCert) -> Option<Self> {
        (!block.is_fast()).then_some(Self(Certified::NonFast { block, fbcert }))
    }

    pub fn block(&self) -> &Metablock {
        match &self.0 {
            Certified::Fast(block) | Certified::NonFast { block, .. } => block,
        }
    }

    pub fn fbcert(&self) -> Option<&EnterFallbackCert> {
        match &self.0 {
            Certified::Fast(_) => None,
            Certified::NonFast { fbcert, .. } => Some(fbcert),
        }
    }

    // the wire form: the block, then the `fbcert` if there is one
    fn rlp_fields(&self) -> Vec<&dyn Encodable> {
        match &self.0 {
            Certified::Fast(block) => vec![block],
            Certified::NonFast { block, fbcert } => vec![block, fbcert],
        }
    }
}

impl Encodable for CertifiedMetablock {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        alloy_rlp::encode_list::<_, dyn Encodable>(&self.rlp_fields(), out);
    }

    fn length(&self) -> usize {
        alloy_rlp::list_length::<_, dyn Encodable>(&self.rlp_fields())
    }
}

impl Decodable for CertifiedMetablock {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let block = Metablock::decode(&mut payload)?;
        let value = match payload.is_empty() {
            true => Self::fast(block),
            false => Self::non_fast(block, EnterFallbackCert::decode(&mut payload)?),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        value.ok_or(alloy_rlp::Error::Custom(
            "an fbcert must accompany exactly the non-fast metablocks",
        ))
    }
}

impl ValidateInput for CertifiedMetablock {
    type Context = ValidationContext;

    /// `Metablock.is_valid`; a non-fast metablock's `fbcert` must be valid too
    fn validate(&self, context: &Self::Context) -> bool {
        self.block().validate(context)
            && self.fbcert().is_none_or(|fbcert| fbcert.validate(context))
    }

    fn cert_optional(&self) -> bool {
        matches!(self.0, Certified::Fast(_))
    }
}

impl Votable for CertifiedMetablock {
    type Entries = Lanes<Entry>;

    fn entries(&self) -> Self::Entries {
        self.block().entries()
    }
}

impl MakesValidationContext<CertifiedMetablock> for MvbaContext {
    fn make_validation_context(&self) -> ValidationContext {
        MakesValidationContext::<Metablock>::make_validation_context(self)
    }
}

/// What proves the slot's fallback decision
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum SlotCommitProof {
    Dummy(DummyQc),
    Mvba(FallbackCommitQc<Lanes<Entry>>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum SlotFallbackMessage {
    Leaderless(LeaderlessMessage),
    Mvba(MvbaMessage<CertifiedMetablock, NfCert>),
}

impl SlotFallbackMessage {
    pub fn slot(&self) -> Slot {
        match self {
            Self::Leaderless(message) => message.slot(),
            Self::Mvba(message) => message.slot(),
        }
    }
}

impl LeaderlessMessage {
    pub fn slot(&self) -> Slot {
        match self {
            Self::FastInput(block) => block.slot(),
            Self::NotFull(declaration) => declaration.scope,
            Self::DummyVote(vote) => vote.scope,
            Self::DummyDecision { qc, .. } => qc.scope,
        }
    }

    // the wire form: a tag, then the variant's fields
    fn rlp_fields(&self) -> Vec<&dyn Encodable> {
        match self {
            Self::FastInput(block) => vec![&1u8, block],
            Self::NotFull(declaration) => vec![&2u8, declaration],
            Self::DummyVote(vote) => vec![&3u8, vote],
            Self::DummyDecision { qc, block } => vec![&4u8, qc, block],
        }
    }
}

impl Encodable for LeaderlessMessage {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        alloy_rlp::encode_list::<_, dyn Encodable>(&self.rlp_fields(), out);
    }

    fn length(&self) -> usize {
        alloy_rlp::list_length::<_, dyn Encodable>(&self.rlp_fields())
    }
}

impl Decodable for LeaderlessMessage {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let result = match u8::decode(&mut payload)? {
            1 => Self::FastInput(Metablock::decode(&mut payload)?),
            2 => Self::NotFull(VoteMsg::decode(&mut payload)?),
            3 => Self::DummyVote(VoteMsg::decode(&mut payload)?),
            4 => Self::DummyDecision {
                qc: DummyQc::decode(&mut payload)?,
                block: Metablock::decode(&mut payload)?,
            },
            _ => return Err(alloy_rlp::Error::Custom("unknown LeaderlessMessage tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

impl SlotFallbackMessage {
    fn rlp_fields(&self) -> [&dyn Encodable; 2] {
        match self {
            Self::Leaderless(message) => [&1u8, message],
            Self::Mvba(message) => [&2u8, message],
        }
    }
}

impl Encodable for SlotFallbackMessage {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        alloy_rlp::encode_list::<_, dyn Encodable>(&self.rlp_fields(), out);
    }

    fn length(&self) -> usize {
        alloy_rlp::list_length::<_, dyn Encodable>(&self.rlp_fields())
    }
}

impl Decodable for SlotFallbackMessage {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let result = match u8::decode(&mut payload)? {
            1 => Self::Leaderless(LeaderlessMessage::decode(&mut payload)?),
            2 => Self::Mvba(MvbaMessage::decode(&mut payload)?),
            _ => return Err(alloy_rlp::Error::Custom("unknown SlotFallbackMessage tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

type Output = MVBAOutput<SlotFallbackMessage, TimerEvent<CertifiedMetablock>>;

pub struct SlotFallback {
    round: LeaderlessRound,
    core: MonadMvba<CertifiedMetablock, NfCert>,

    /// This validator's registered input, set once by [`Mvba::propose`]
    input: Option<CertifiedMetablock>,
    /// What the round found before the input was registered, replayed then
    held: Vec<LeaderlessEffect>,
    /// The first decision recorded, by whichever certificate formed first
    decision: Option<(Metablock, SlotCommitProof)>,

    outputs: VecDeque<Output>,
    abandoned: bool,
}

impl SlotFallback {
    /// Drains the round, feeding what it found to the core, then the core
    fn pump(&mut self) {
        while let Some(effect) = self.round.poll() {
            if self.input.is_some() {
                self.apply(effect);
            } else {
                self.held.push(effect);
            }
        }

        while let Some(output) = self.core.poll() {
            self.outputs.push_back(match output {
                MVBAOutput::Broadcast(message) => {
                    MVBAOutput::Broadcast(SlotFallbackMessage::Mvba(message))
                }
                MVBAOutput::Unicast { to, message } => MVBAOutput::Unicast {
                    to,
                    message: SlotFallbackMessage::Mvba(message),
                },
                MVBAOutput::ScheduleTimer {
                    duration,
                    timer_event,
                } => MVBAOutput::ScheduleTimer {
                    duration,
                    timer_event,
                },
            });
        }

        if self.decision.is_none()
            && let Some(value) = self.core.decision()
            && let Some(qc) = self.core.decision_proof()
        {
            self.decision = Some((value.block().clone(), SlotCommitProof::Mvba(qc.clone())));
        }
    }

    /// The core is offered proposals and abandoned in whatever view it is in
    fn apply(&mut self, effect: LeaderlessEffect) {
        match effect {
            LeaderlessEffect::Broadcast(message) => {
                self.outputs
                    .push_back(MVBAOutput::Broadcast(SlotFallbackMessage::Leaderless(
                        message,
                    )));
            }
            LeaderlessEffect::FastBlock(block) => {
                if let Some(value) = CertifiedMetablock::fast(block) {
                    self.core.offer_proposal(value, None);
                }
            }
            LeaderlessEffect::NfCert(nf_cert) => {
                if let Some(input) = &self.input
                    && !input.block().is_fast()
                {
                    self.core.offer_proposal(input.clone(), Some(nf_cert));
                }
            }
            LeaderlessEffect::Decided { qc, block } => {
                if self.decision.is_some() {
                    return;
                }
                // persist-before-send: the certificate that decides the slot
                self.decision = Some((block.clone(), SlotCommitProof::Dummy(qc.clone())));
                self.outputs
                    .push_back(MVBAOutput::Broadcast(SlotFallbackMessage::Leaderless(
                        LeaderlessMessage::DummyDecision { qc, block },
                    )));
                self.core.abandon();
                self.round.abandon();
            }
        }
    }
}

impl Mvba<Metablock> for SlotFallback {
    type Message = SlotFallbackMessage;
    type Context = MvbaContext;
    // the core's timers only: the round has none
    type TimerEvent = TimerEvent<CertifiedMetablock>;
    type FallbackCert = EnterFallbackCert;
    type DecisionProof = SlotCommitProof;

    fn new(ctx: MvbaContext) -> Self {
        Self {
            round: LeaderlessRound::new(&ctx),
            core: MonadMvba::new(ctx),
            input: None,
            held: Vec::new(),
            decision: None,
            outputs: VecDeque::new(),
            abandoned: false,
        }
    }

    fn propose(&mut self, data: Metablock, cert: Option<EnterFallbackCert>) {
        if self.abandoned || self.input.is_some() {
            return;
        }

        // `fbcert` is ⊥ for a fast metablock, whatever certificate admitted the path
        let input = match cert {
            Some(fbcert) if !data.is_fast() => CertifiedMetablock::non_fast(data.clone(), fbcert),
            _ => CertifiedMetablock::fast(data.clone()),
        };
        // a non-fast metablock without its fbcert is no input: the instance stays unstarted
        let Some(input) = input else {
            tracing::warn!(slot = ?data.slot(), "non-fast fallback input without an fbcert ignored");
            return;
        };
        self.input = Some(input.clone());

        self.core.propose(input, None);
        self.round.enter(data);
        for effect in std::mem::take(&mut self.held) {
            self.apply(effect);
        }
        self.pump();
    }

    fn validator_data(&self) -> &ValidatorData {
        self.core.validator_data()
    }

    fn handle_message(&mut self, sender: ValidatorIndex, message: SlotFallbackMessage) {
        match message {
            SlotFallbackMessage::Leaderless(message) => self.round.handle_message(sender, message),
            SlotFallbackMessage::Mvba(message) => self.core.handle_message(sender, message),
        }
        self.pump();
    }

    fn handle_timer(&mut self, timer_event: TimerEvent<CertifiedMetablock>) {
        self.core.handle_timer(timer_event);
        self.pump();
    }

    fn abandon(&mut self) {
        self.abandoned = true;
        self.core.abandon();
        self.round.abandon();
        self.held.clear();
        self.outputs.clear();
    }

    fn decision(&self) -> Option<&Metablock> {
        self.decision.as_ref().map(|(block, _)| block)
    }

    fn decision_proof(&self) -> Option<&SlotCommitProof> {
        self.decision.as_ref().map(|(_, proof)| proof)
    }

    fn poll(&mut self) -> Option<Output> {
        self.outputs.pop_front()
    }
}

#[cfg(test)]
mod tests {
    //! One instance fed forged messages, on the core's four-validator cluster
    //! (f = 1). A non-fast metablock there has the entries of the fast one on
    //! the same seed

    use std::{collections::BTreeSet, sync::Arc};

    use monad_mcp_common::SlotDeadline;

    use super::{
        super::{
            super::{
                super::{
                    conductor::{CadenceConductor, acs::median::MedianAcs},
                    message::CadenceWireMsg,
                    test_utils::{assert_roundtrip, assert_serialization_roundtrip, dummy_index},
                },
                chorus::{Chorus, ChorusMessage},
                types::{HeaderAuth, NodeId},
            },
            EnterFallbackVote,
            leaderless::{DummyVote, NotFull},
            monad_mvba::{PrePrepareMsg, test_helpers as h},
        },
        *,
    };

    type Core = MvbaMessage<CertifiedMetablock, NfCert>;
    type Proposal = PrePrepareMsg<CertifiedMetablock, NfCert>;
    type Wire = CadenceWireMsg<Chorus, CadenceConductor<MedianAcs<SlotDeadline>>>;

    fn context(node: NodeId) -> MvbaContext {
        MvbaContext {
            slot: h::SLOT,
            num_lanes: h::NUM_LANES,
            delta: h::DELTA,
            node_id: node,
            key: Arc::new(node.keypair()),
            validator_data: h::validator_data(),
            header_auth: Arc::new(HeaderAuth::new(|_| false)),
        }
    }

    fn validation_context() -> ValidationContext {
        MakesValidationContext::<CertifiedMetablock>::make_validation_context(&context(
            h::nodes()[0],
        ))
    }

    fn slot_fallback(node: NodeId) -> SlotFallback {
        SlotFallback::new(context(node))
    }

    fn fast(seed: u64) -> Metablock {
        h::metablock(seed, &h::validator_data())
    }

    fn non_fast(seed: u64) -> Metablock {
        h::mixed_evidence_metablock(seed, &h::validator_data())
    }

    fn fbcert() -> EnterFallbackCert {
        h::enter_fallback_cert(&h::validator_data())
    }

    /// An instance that has proposed `input`, a non-fast one beside its
    /// fallback certificate
    fn started(node: NodeId, input: &Metablock) -> SlotFallback {
        let mut instance = slot_fallback(node);
        instance.propose(input.clone(), (!input.is_fast()).then(fbcert));
        instance
    }

    fn fast_input(block: &Metablock) -> SlotFallbackMessage {
        SlotFallbackMessage::Leaderless(LeaderlessMessage::FastInput(block.clone()))
    }

    fn dummy_vote_of(node: NodeId, block: &Metablock) -> LeaderlessMessage {
        let vote = DummyVote(block.entries());
        LeaderlessMessage::DummyVote(VoteMsg::new_signed(h::SLOT, vote, &node.keypair()))
    }

    fn dummy_vote(node: NodeId, block: &Metablock) -> SlotFallbackMessage {
        SlotFallbackMessage::Leaderless(dummy_vote_of(node, block))
    }

    fn not_full_of(node: NodeId) -> LeaderlessMessage {
        LeaderlessMessage::NotFull(VoteMsg::new_signed(h::SLOT, NotFull, &node.keypair()))
    }

    fn not_full(node: NodeId) -> SlotFallbackMessage {
        SlotFallbackMessage::Leaderless(not_full_of(node))
    }

    fn dummy_decision(block: &Metablock) -> SlotFallbackMessage {
        let vote = DummyVote(block.entries());
        SlotFallbackMessage::Leaderless(LeaderlessMessage::DummyDecision {
            qc: h::strong_qc(h::SLOT, vote, &h::quorum(), &h::validator_data()),
            block: block.clone(),
        })
    }

    /// `TC_{slot, view}` on its own, carrying no lock
    fn tc(view: u64) -> SlotFallbackMessage {
        let tc = h::timeout_certificate(h::view(view), None, &h::validator_data());
        SlotFallbackMessage::Mvba(Core::Tc(tc))
    }

    /// A view-1 commit certificate over the entries of `block`
    fn commit_qc(block: &Metablock) -> FallbackCommitQc<Lanes<Entry>> {
        match h::commit_qc_message(h::view(1), block, &h::validator_data()) {
            MvbaMessage::CommitQc(qc) => qc,
            _ => unreachable!("the helper builds a commit certificate"),
        }
    }

    fn deliver(instance: &mut SlotFallback, from: NodeId, message: SlotFallbackMessage) {
        instance.handle_message(dummy_index(from), message);
    }

    fn drain(instance: &mut SlotFallback) -> Vec<Output> {
        std::iter::from_fn(|| instance.poll()).collect()
    }

    fn broadcasts(outputs: &[Output]) -> Vec<&SlotFallbackMessage> {
        outputs
            .iter()
            .filter_map(|output| match output {
                MVBAOutput::Broadcast(message) => Some(message),
                MVBAOutput::Unicast { .. } | MVBAOutput::ScheduleTimer { .. } => None,
            })
            .collect()
    }

    fn leaderless(outputs: &[Output]) -> Vec<&LeaderlessMessage> {
        broadcasts(outputs)
            .into_iter()
            .filter_map(|message| match message {
                SlotFallbackMessage::Leaderless(message) => Some(message),
                SlotFallbackMessage::Mvba(_) => None,
            })
            .collect()
    }

    fn proposals(outputs: &[Output]) -> Vec<&Proposal> {
        broadcasts(outputs)
            .into_iter()
            .filter_map(|message| match message {
                SlotFallbackMessage::Mvba(MvbaMessage::PrePrepare(proposal)) => Some(proposal),
                _ => None,
            })
            .collect()
    }

    fn timers(outputs: &[Output]) -> Vec<TimerEvent<CertifiedMetablock>> {
        outputs
            .iter()
            .filter_map(|output| match output {
                MVBAOutput::ScheduleTimer { timer_event, .. } => Some(timer_event.clone()),
                MVBAOutput::Broadcast(_) | MVBAOutput::Unicast { .. } => None,
            })
            .collect()
    }

    fn is_dummy_decision(message: &LeaderlessMessage) -> bool {
        matches!(message, LeaderlessMessage::DummyDecision { .. })
    }

    /// Delivers what `cluster` sends, a broadcast to its sender too, until
    /// nobody has more to send; returns everything sent
    fn run(cluster: &mut [(NodeId, SlotFallback)]) -> Vec<SlotFallbackMessage> {
        let mut sent = Vec::new();
        loop {
            let mut in_flight = Vec::new();
            for (node, instance) in cluster.iter_mut() {
                for output in drain(instance) {
                    match output {
                        MVBAOutput::Broadcast(message) => in_flight.push((*node, None, message)),
                        MVBAOutput::Unicast { to, message } => {
                            in_flight.push((*node, Some(to), message))
                        }
                        MVBAOutput::ScheduleTimer { .. } => {}
                    }
                }
            }
            if in_flight.is_empty() {
                return sent;
            }
            for (from, to, message) in in_flight {
                for (node, instance) in cluster.iter_mut() {
                    if to.is_none_or(|to| to == *node) {
                        deliver(instance, from, message.clone());
                    }
                }
                sent.push(message);
            }
        }
    }

    /// `[block, fbcert?]`, the wire layout, for any pairing
    fn wire(block: &Metablock, fbcert: Option<&EnterFallbackCert>) -> Vec<u8> {
        let mut fields: Vec<&dyn Encodable> = vec![block];
        fields.extend(fbcert.map(|fbcert| fbcert as &dyn Encodable));
        let mut out = Vec::new();
        alloy_rlp::encode_list::<_, dyn Encodable>(&fields, &mut out);
        out
    }

    fn decodes(bytes: &[u8]) -> bool {
        alloy_rlp::decode_exact::<CertifiedMetablock>(bytes).is_ok()
    }

    #[test]
    fn a_non_fast_metablock_without_its_fallback_certificate_is_invalid() {
        let context = validation_context();
        let block = non_fast(1);

        let certified =
            CertifiedMetablock::non_fast(block.clone(), fbcert()).expect("a non-fast metablock");
        assert!(certified.validate(&context));
        assert!(!certified.cert_optional());
        assert_eq!(wire(&block, Some(&fbcert())), alloy_rlp::encode(&certified));

        assert!(CertifiedMetablock::fast(block.clone()).is_none());
        assert!(!decodes(&wire(&block, None)));

        // a certificate of another slot is not this metablock's
        let validators = h::validator_data();
        let elsewhere = h::strong_qc(Slot(1), EnterFallbackVote, &h::quorum(), &validators);
        let misplaced =
            CertifiedMetablock::non_fast(block, elsewhere).expect("a non-fast metablock");
        assert!(!misplaced.validate(&context));
    }

    #[test]
    fn a_fast_metablock_carrying_a_fallback_certificate_is_invalid() {
        let context = validation_context();
        let block = fast(1);

        let bare = CertifiedMetablock::fast(block.clone()).expect("a fast metablock");
        assert!(bare.validate(&context));
        assert!(bare.cert_optional());
        assert_eq!(wire(&block, None), alloy_rlp::encode(&bare));

        assert!(CertifiedMetablock::non_fast(block.clone(), fbcert()).is_none());
        assert!(!decodes(&wire(&block, Some(&fbcert()))));
    }

    #[test]
    fn entries_ignore_the_fallback_certificate() {
        let block = non_fast(1);
        let certified =
            CertifiedMetablock::non_fast(block.clone(), fbcert()).expect("a non-fast metablock");

        assert_eq!(certified.entries(), block.entries());
        // nor do they tell a fast metablock from a non-fast one
        let fast_value = CertifiedMetablock::fast(fast(1)).expect("a fast metablock");
        assert_eq!(certified.entries(), fast_value.entries());
    }

    #[test]
    fn a_certified_metablock_round_trips() {
        let context = validation_context();
        let fast_value = CertifiedMetablock::fast(fast(1)).expect("a fast metablock");
        let certified =
            CertifiedMetablock::non_fast(non_fast(1), fbcert()).expect("a non-fast metablock");
        for value in [&fast_value, &certified] {
            assert!(assert_roundtrip(value).validate(&context));
        }

        // the pairing is checked at decode, validity at ingress
        assert!(!decodes(&wire(&non_fast(1), None)));
    }

    #[test]
    fn propose_starts_view_1_and_the_dummy_round_together() {
        let follower = h::nodes()[0];
        assert_ne!(
            h::leader_of(h::view(1)),
            follower,
            "test assumes a follower"
        );

        let mut instance = started(follower, &non_fast(1));
        let outputs = drain(&mut instance);
        assert_eq!(
            timers(&outputs),
            vec![TimerEvent::ViewTimeout(h::view(1))],
            "the core starts view 1"
        );
        assert_eq!(leaderless(&outputs), vec![&not_full_of(follower)]);
        assert_eq!(broadcasts(&outputs).len(), 1, "a follower proposes nothing");

        let block = fast(1);
        let mut instance = started(follower, &block);
        let outputs = drain(&mut instance);
        assert_eq!(timers(&outputs), vec![TimerEvent::ViewTimeout(h::view(1))]);
        let sent = leaderless(&outputs);
        assert_eq!(sent.len(), 2, "{sent:?}");
        assert!(sent.contains(&&LeaderlessMessage::FastInput(block.clone())));
        assert!(sent.contains(&&dummy_vote_of(follower, &block)));
        assert_eq!(broadcasts(&outputs).len(), 2);

        // one input per instance
        instance.propose(non_fast(2), Some(fbcert()));
        assert!(drain(&mut instance).is_empty());
    }

    #[test]
    fn a_fast_leader_proposes_at_entry_beside_its_dummy_vote() {
        let leader = h::leader_of(h::view(1));
        let block = fast(1);
        let mut instance = slot_fallback(leader);

        // the certificate that admitted the path is no part of a fast value
        instance.propose(block.clone(), Some(fbcert()));
        let outputs = drain(&mut instance);

        let sent = leaderless(&outputs);
        assert_eq!(sent.len(), 2, "{sent:?}");
        assert!(sent.contains(&&LeaderlessMessage::FastInput(block.clone())));
        assert!(sent.contains(&&dummy_vote_of(leader, &block)));

        let proposed = proposals(&outputs);
        assert_eq!(proposed.len(), 1);
        let proposal = proposed[0];
        assert_eq!(proposal.view, h::view(1));
        let value = CertifiedMetablock::fast(block).expect("a fast metablock");
        assert_eq!(proposal.value, value);
        assert!(proposal.value.validate(&validation_context()));
        assert!(proposal.tc.is_none());
        assert!(proposal.cert.is_none());
    }

    #[test]
    fn a_non_fast_leader_waits_for_the_nf_cert_then_proposes_its_input() {
        let leader = h::leader_of(h::view(1));
        let block = non_fast(1);
        let mut instance = started(leader, &block);
        assert!(
            proposals(&drain(&mut instance)).is_empty(),
            "nothing admits the non-fast input yet"
        );

        // its own declaration comes back off the wire, then two peers'
        deliver(&mut instance, leader, not_full(leader));
        deliver(&mut instance, h::nodes()[0], not_full(h::nodes()[0]));
        assert!(proposals(&drain(&mut instance)).is_empty());

        deliver(&mut instance, h::nodes()[2], not_full(h::nodes()[2]));
        let outputs = drain(&mut instance);
        let proposed = proposals(&outputs);
        assert_eq!(proposed.len(), 1);
        let proposal = proposed[0];
        assert_eq!(proposal.view, h::view(1));
        let input = CertifiedMetablock::non_fast(block, fbcert()).expect("a non-fast metablock");
        assert_eq!(proposal.value, input);
        let nf_cert = proposal
            .cert
            .as_ref()
            .expect("an unlocked non-fast proposal carries the NfCert");
        assert!(nf_cert.validate(&validation_context()));
    }

    #[test]
    fn a_received_fast_block_releases_a_non_fast_leader_with_that_block() {
        let leader = h::leader_of(h::view(1));
        let received = fast(2);
        let mut instance = started(leader, &non_fast(1));
        drain(&mut instance);

        deliver(&mut instance, h::nodes()[0], fast_input(&received));
        let outputs = drain(&mut instance);
        let proposed = proposals(&outputs);
        assert_eq!(proposed.len(), 1);
        let value = CertifiedMetablock::fast(received).expect("a fast metablock");
        assert_eq!(proposed[0].value, value);
        assert!(proposed[0].cert.is_none());

        // the certificate for its own input, arriving later, is not proposed
        for node in h::quorum() {
            deliver(&mut instance, node, not_full(node));
        }
        assert!(proposals(&drain(&mut instance)).is_empty());
    }

    #[test]
    fn an_nf_cert_formed_after_view_1_releases_a_later_unlocked_leader() {
        let leader = h::leader_of(h::view(2));
        let block = non_fast(1);
        let mut instance = started(leader, &block);
        drain(&mut instance);

        // TC_1 forms before the NfCert: the view-2 leader waits, timer running
        deliver(&mut instance, h::nodes()[0], tc(1));
        let outputs = drain(&mut instance);
        assert_eq!(timers(&outputs), vec![TimerEvent::ViewTimeout(h::view(2))]);
        assert!(proposals(&outputs).is_empty());

        for node in &h::quorum()[..2] {
            deliver(&mut instance, *node, not_full(*node));
        }
        assert!(proposals(&drain(&mut instance)).is_empty());

        deliver(&mut instance, h::quorum()[2], not_full(h::quorum()[2]));
        let outputs = drain(&mut instance);
        let proposed = proposals(&outputs);
        assert_eq!(proposed.len(), 1);
        let proposal = proposed[0];
        assert_eq!(proposal.view, h::view(2));
        assert!(proposal.tc.is_some(), "justified by TC_1");
        let input = CertifiedMetablock::non_fast(block, fbcert()).expect("a non-fast metablock");
        assert_eq!(proposal.value, input);
        let nf_cert = proposal
            .cert
            .as_ref()
            .expect("an unlocked non-fast proposal carries the NfCert");
        assert!(nf_cert.validate(&validation_context()));
    }

    #[test]
    fn propose_past_view_1_still_sends_evidence_and_the_dummy_vote() {
        let node = h::nodes()[0];
        for block in [fast(1), non_fast(2)] {
            let mut instance = slot_fallback(node);
            deliver(&mut instance, h::nodes()[1], tc(1));
            assert!(drain(&mut instance).is_empty());

            instance.propose(block.clone(), (!block.is_fast()).then(fbcert));
            let outputs = drain(&mut instance);
            assert!(
                timers(&outputs).contains(&TimerEvent::ViewTimeout(h::view(2))),
                "the buffered certificate takes the core past view 1 at entry"
            );

            let sent = leaderless(&outputs);
            if block.is_fast() {
                assert_eq!(sent.len(), 2, "{sent:?}");
                assert!(sent.contains(&&LeaderlessMessage::FastInput(block.clone())));
                assert!(sent.contains(&&dummy_vote_of(node, &block)));
            } else {
                assert_eq!(sent, vec![&not_full_of(node)]);
            }
        }
    }

    #[test]
    fn a_dummy_qc_decides_with_the_dummy_proof_and_abandons_the_core() {
        let node = h::nodes()[0];
        let leader = h::leader_of(h::view(1));
        assert_ne!(leader, node, "test assumes a follower");
        let block = fast(1);
        let mut instance = started(node, &block);

        deliver(&mut instance, leader, fast_input(&block));
        for voter in &h::quorum()[..2] {
            deliver(&mut instance, *voter, dummy_vote(*voter, &block));
        }
        assert!(instance.decision().is_none());
        drain(&mut instance);

        deliver(
            &mut instance,
            h::quorum()[2],
            dummy_vote(h::quorum()[2], &block),
        );
        let outputs = drain(&mut instance);
        assert_eq!(instance.decision(), Some(&block));
        let Some(SlotCommitProof::Dummy(qc)) = instance.decision_proof().cloned() else {
            panic!("a DummyQC decision is proven by the DummyQC");
        };
        assert_eq!(qc.scope, h::SLOT);
        assert_eq!(qc.verdict, DummyVote(block.entries()));
        assert!(qc.verify(&h::validator_data()));
        let decision = LeaderlessMessage::DummyDecision {
            qc: qc.clone(),
            block: block.clone(),
        };
        assert_eq!(
            broadcasts(&outputs),
            vec![&SlotFallbackMessage::Leaderless(decision)],
            "the certificate and its block are passed on"
        );

        // the core is abandoned: no vote on the leader's proposal, no timeout,
        // and its own certificate changes nothing
        let value = CertifiedMetablock::fast(block.clone()).expect("a fast metablock");
        let proposal =
            Proposal::new_signed(h::SLOT, h::view(1), value, None, None, &leader.keypair());
        deliver(
            &mut instance,
            leader,
            SlotFallbackMessage::Mvba(Core::PrePrepare(proposal)),
        );
        instance.handle_timer(TimerEvent::ViewTimeout(h::view(1)));
        deliver(
            &mut instance,
            leader,
            SlotFallbackMessage::Mvba(Core::CommitQc(commit_qc(&block))),
        );
        assert!(drain(&mut instance).is_empty());
        assert_eq!(instance.decision_proof(), Some(&SlotCommitProof::Dummy(qc)));
    }

    #[test]
    fn a_dummy_qc_forming_after_view_1_decides_at_once() {
        let node = h::nodes()[0];
        let block = fast(1);
        let mut instance = started(node, &block);

        deliver(&mut instance, h::nodes()[1], tc(1));
        let outputs = drain(&mut instance);
        assert!(timers(&outputs).contains(&TimerEvent::ViewTimeout(h::view(2))));

        // votes sent again complete the quorum while the core is in view 2
        deliver(&mut instance, h::nodes()[1], fast_input(&block));
        for voter in h::quorum() {
            deliver(&mut instance, voter, dummy_vote(voter, &block));
        }
        assert_eq!(instance.decision(), Some(&block));
        assert!(matches!(
            instance.decision_proof(),
            Some(SlotCommitProof::Dummy(_))
        ));
        let outputs = drain(&mut instance);
        assert!(leaderless(&outputs).into_iter().any(is_dummy_decision));

        instance.handle_timer(TimerEvent::ViewTimeout(h::view(2)));
        assert!(drain(&mut instance).is_empty(), "the core is abandoned");
    }

    #[test]
    fn a_commit_qc_decides_with_the_mvba_proof() {
        let node = h::nodes()[0];
        let block = non_fast(1);
        let mut instance = started(node, &block);
        drain(&mut instance);

        let qc = commit_qc(&block);
        let certificate = SlotFallbackMessage::Mvba(Core::CommitQc(qc.clone()));
        deliver(&mut instance, h::nodes()[1], certificate.clone());
        let outputs = drain(&mut instance);
        assert_eq!(instance.decision(), Some(&block));
        assert_eq!(
            instance.decision_proof(),
            Some(&SlotCommitProof::Mvba(qc.clone()))
        );
        assert!(
            broadcasts(&outputs).contains(&&certificate),
            "the certificate is passed on so others can decide"
        );

        // the first recorded decision wins: a dummy certificate on the same
        // entries, arriving later, replaces neither the block nor the proof
        deliver(&mut instance, h::nodes()[1], dummy_decision(&fast(1)));
        assert!(drain(&mut instance).is_empty());
        assert_eq!(instance.decision(), Some(&block));
        assert_eq!(instance.decision_proof(), Some(&SlotCommitProof::Mvba(qc)));
    }

    #[test]
    fn evidence_seen_before_propose_is_replayed_at_propose() {
        let leader = h::leader_of(h::view(1));
        let own = non_fast(1);

        // a quorum of declarations: a non-fast leader proposes at entry
        let mut instance = slot_fallback(leader);
        for node in h::nodes().into_iter().filter(|node| *node != leader) {
            deliver(&mut instance, node, not_full(node));
        }
        assert!(drain(&mut instance).is_empty());
        instance.propose(own.clone(), Some(fbcert()));
        let outputs = drain(&mut instance);
        let proposed = proposals(&outputs);
        assert_eq!(proposed.len(), 1);
        assert_eq!(proposed[0].value.block(), &own);
        assert!(proposed[0].cert.is_some());

        // a fast block: a non-fast leader proposes it at entry instead
        let received = fast(2);
        let mut instance = slot_fallback(leader);
        deliver(&mut instance, h::nodes()[0], fast_input(&received));
        assert!(drain(&mut instance).is_empty());
        instance.propose(own, Some(fbcert()));
        let outputs = drain(&mut instance);
        let proposed = proposals(&outputs);
        assert_eq!(proposed.len(), 1);
        assert_eq!(proposed[0].value.block(), &received);
        assert!(proposed[0].cert.is_none());

        // a quorum of dummy votes and their block: the decision waits for the
        // input, then is made at entry
        let block = fast(1);
        let mut instance = slot_fallback(h::nodes()[0]);
        deliver(&mut instance, leader, fast_input(&block));
        for voter in h::quorum() {
            deliver(&mut instance, voter, dummy_vote(voter, &block));
        }
        assert!(drain(&mut instance).is_empty());
        assert!(
            instance.decision().is_none(),
            "holding the certificate does not start the instance"
        );
        instance.propose(block.clone(), None);
        assert_eq!(instance.decision(), Some(&block));
        assert!(matches!(
            instance.decision_proof(),
            Some(SlotCommitProof::Dummy(_))
        ));
        let outputs = drain(&mut instance);
        assert!(leaderless(&outputs).into_iter().any(is_dummy_decision));
    }

    fn kind(message: &SlotFallbackMessage) -> &'static str {
        match message {
            SlotFallbackMessage::Leaderless(message) => match message {
                LeaderlessMessage::FastInput(_) => "fast input",
                LeaderlessMessage::NotFull(_) => "not full",
                LeaderlessMessage::DummyVote(_) => "dummy vote",
                LeaderlessMessage::DummyDecision { .. } => "dummy decision",
            },
            SlotFallbackMessage::Mvba(message) => match message {
                MvbaMessage::PrePrepare(_) => "pre-prepare",
                MvbaMessage::Prepare(_) => "prepare",
                MvbaMessage::Commit(_) => "commit",
                MvbaMessage::Timeout(_) => "timeout",
                MvbaMessage::CommitQc(_) => "commit qc",
                MvbaMessage::BlockRequest(_) => "block request",
                MvbaMessage::BlockResponse(_) => "block response",
                MvbaMessage::Tc(_) => "tc",
            },
        }
    }

    #[test]
    fn every_slot_fallback_message_round_trips() {
        // three validators with one non-fast input, beside a silent view-1
        // leader: declarations, a timed-out view, then a view-2 decision
        let silent = h::leader_of(h::view(1));
        let block = non_fast(1);
        let mut cluster: Vec<_> = h::nodes()
            .into_iter()
            .map(|node| (node, slot_fallback(node)))
            .collect();
        for (node, instance) in &mut cluster {
            if *node != silent {
                instance.propose(block.clone(), Some(fbcert()));
            }
        }
        let mut messages = run(&mut cluster);
        for (node, instance) in &mut cluster {
            if *node != silent {
                instance.handle_timer(TimerEvent::ViewTimeout(h::view(1)));
            }
        }
        messages.extend(run(&mut cluster));
        for (node, instance) in &cluster {
            let decided = matches!(instance.decision_proof(), Some(SlotCommitProof::Mvba(_)));
            assert_eq!(decided, *node != silent, "{node:?}");
        }

        // a validator handed the commit certificate alone asks for its block
        let certificate = messages
            .iter()
            .find(|message| kind(message) == "commit qc")
            .cloned()
            .expect("the deciders pass their certificate on");
        let mut lagging = started(silent, &non_fast(2));
        drain(&mut lagging);
        deliver(&mut lagging, h::nodes()[0], certificate);
        let request = broadcasts(&drain(&mut lagging))
            .into_iter()
            .find(|message| kind(message) == "block request")
            .cloned()
            .expect("the certified block is missing");
        let (_, holder) = &mut cluster[0];
        deliver(holder, silent, request.clone());
        let response = drain(holder)
            .into_iter()
            .find_map(|output| match output {
                MVBAOutput::Unicast { message, .. } => Some(message),
                _ => None,
            })
            .expect("a decided validator serves the block");
        messages.extend([request, response]);

        // a fast view-1 leader: its block, its vote, its proposal, the decision
        let fast_block = fast(3);
        let mut voter = started(silent, &fast_block);
        let entry = drain(&mut voter);
        messages.extend(broadcasts(&entry).into_iter().cloned());
        deliver(&mut voter, silent, fast_input(&fast_block));
        for node in h::quorum() {
            deliver(&mut voter, node, dummy_vote(node, &fast_block));
        }
        let decided = drain(&mut voter);
        messages.extend(broadcasts(&decided).into_iter().cloned());

        let kinds: BTreeSet<_> = messages.iter().map(kind).collect();
        assert_eq!(kinds.len(), 12, "not every variant was sent: {kinds:?}");
        let proposed: Vec<_> = messages
            .iter()
            .filter_map(|message| match message {
                SlotFallbackMessage::Mvba(MvbaMessage::PrePrepare(proposal)) => Some(proposal),
                _ => None,
            })
            .collect();
        assert!(proposed.iter().any(|proposal| {
            proposal.tc.is_some() && proposal.cert.is_some() && proposal.value.fbcert().is_some()
        }));
        assert!(proposed.iter().any(|proposal| {
            proposal.tc.is_none() && proposal.cert.is_none() && proposal.value.fbcert().is_none()
        }));

        messages.dedup();
        for message in messages {
            assert_eq!(message.slot(), h::SLOT);
            assert_roundtrip(&message);
            // the outer Chorus and Cadence framing wraps the fallback arm
            assert_serialization_roundtrip(&Wire::Slot(ChorusMessage::Fallback(message)));
        }

        assert!(alloy_rlp::decode_exact::<SlotFallbackMessage>(&[0xc1, 3]).is_err());
        assert!(alloy_rlp::decode_exact::<LeaderlessMessage>(&[0xc1, 5]).is_err());
    }
}
