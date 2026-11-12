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

//! The fallback path as Chorus runs it: the dummy view and the
//! leader-based MVBA, started together and deciding by whichever certificate
//! forms first

use std::collections::VecDeque;

use alloy_rlp::{Decodable, Encodable};

use super::{
    super::types::{Lanes, Slot, ValidatorData, ValidatorIndex},
    EnterFallbackCert, Entry, FallbackCommitQc, MVBAOutput, Metablock, Mvba, ValidateCert,
    ValidateInput, Votable,
    dummy_view::{DummyQc, DummyView, DummyViewEffect, DummyViewMessage, NfCert},
    monad_mvba::{
        MakesValidationContext, MonadMvba, MvbaContext, MvbaMessage, TimerEvent, ValidationContext,
    },
};

/// The value the core MVBA runs over, so its certificates travel wherever the
/// block does. A fast metablock stands alone; any other one carries the
/// `fbcert` the paper makes a field of it and the `NFCert` that admits it
///
/// The paper asks for the `NFCert` only on an unlocked proposal; here it
/// travels with the value in every view
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CertifiedMetablock(Certified);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Certified {
    Fast(Metablock),
    NonFast {
        block: Metablock,
        fbcert: EnterFallbackCert,
        nfcert: NfCert,
    },
}

impl CertifiedMetablock {
    /// `None` unless `block` is fast
    pub fn fast(block: Metablock) -> Option<Self> {
        block.is_fast().then_some(Self(Certified::Fast(block)))
    }

    /// `None` if `block` is fast
    pub fn non_fast(block: Metablock, fbcert: EnterFallbackCert, nfcert: NfCert) -> Option<Self> {
        (!block.is_fast()).then_some(Self(Certified::NonFast {
            block,
            fbcert,
            nfcert,
        }))
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

    pub fn nfcert(&self) -> Option<&NfCert> {
        match &self.0 {
            Certified::Fast(_) => None,
            Certified::NonFast { nfcert, .. } => Some(nfcert),
        }
    }

    // the wire form: the block, then both certificates if it is not fast
    fn rlp_fields(&self) -> Vec<&dyn Encodable> {
        match &self.0 {
            Certified::Fast(block) => vec![block],
            Certified::NonFast {
                block,
                fbcert,
                nfcert,
            } => vec![block, fbcert, nfcert],
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
            false => {
                let fbcert = EnterFallbackCert::decode(&mut payload)?;
                let nfcert = NfCert::decode(&mut payload)?;
                Self::non_fast(block, fbcert, nfcert)
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        value.ok_or(alloy_rlp::Error::Custom(
            "certificates must accompany exactly the non-fast metablocks",
        ))
    }
}

impl ValidateInput for CertifiedMetablock {
    type Context = ValidationContext;

    /// `Metablock.is_valid`; a non-fast metablock's certificates must be valid too
    fn validate(&self, context: &Self::Context) -> bool {
        match &self.0 {
            Certified::Fast(block) => block.validate(context),
            Certified::NonFast {
                block,
                fbcert,
                nfcert,
            } => block.validate(context) && fbcert.validate(context) && nfcert.validate(context),
        }
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
    DummyView(DummyViewMessage),
    Mvba(MvbaMessage<CertifiedMetablock>),
}

impl SlotFallbackMessage {
    pub fn slot(&self) -> Slot {
        match self {
            Self::DummyView(message) => message.slot(),
            Self::Mvba(message) => message.slot(),
        }
    }
}

impl SlotFallbackMessage {
    fn rlp_fields(&self) -> [&dyn Encodable; 2] {
        match self {
            Self::DummyView(message) => [&1u8, message],
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
            1 => Self::DummyView(DummyViewMessage::decode(&mut payload)?),
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

/// What `propose` registered. A non-fast input is no value until the NFCert exists
enum FallbackInput {
    Fast(Metablock),
    NonFast {
        block: Metablock,
        fbcert: EnterFallbackCert,
    },
}

pub struct SlotFallback {
    dummy_view: DummyView,
    core: MonadMvba<CertifiedMetablock>,

    /// This validator's registered input, set once by [`Mvba::propose`]
    input: Option<FallbackInput>,
    /// What the dummy view found before the input was registered, replayed then
    held: Vec<DummyViewEffect>,
    /// The first decision recorded, by whichever certificate formed first
    decision: Option<(Metablock, SlotCommitProof)>,

    outputs: VecDeque<Output>,
    abandoned: bool,
}

impl SlotFallback {
    /// Drains the dummy view, feeding what it found to the core, then the core
    fn drain(&mut self) {
        self.drain_dummy_view();
        self.drain_core();
    }

    fn drain_dummy_view(&mut self) {
        while let Some(effect) = self.dummy_view.poll() {
            if self.input.is_some() {
                self.apply(effect);
            } else {
                self.held.push(effect);
            }
        }
    }

    fn drain_core(&mut self) {
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

        if self.decision.is_some() {
            return;
        }
        if let Some(value) = self.core.decision()
            && let Some(qc) = self.core.decision_proof()
        {
            self.decision = Some((value.block().clone(), SlotCommitProof::Mvba(qc.clone())));
        } else if let Some(FallbackInput::NonFast { block, .. }) = &self.input
            && let Some(qc) = self.core.commit_qc()
            && block.entries() == qc.verdict.0
        {
            // the input is no value without its NFCert, so the core cannot decide it
            // itself; the certificate over its entries settles the slot all the same
            let qc = qc.clone();
            self.decision = Some((block.clone(), SlotCommitProof::Mvba(qc.clone())));
            self.outputs
                .push_back(MVBAOutput::Broadcast(SlotFallbackMessage::Mvba(
                    MvbaMessage::CommitQc(qc),
                )));
            self.core.abandon();
        }
    }

    /// The core is offered proposals, a fast block over the non-fast input, and
    /// abandoned in whatever view it is in
    fn apply(&mut self, effect: DummyViewEffect) {
        match effect {
            DummyViewEffect::Broadcast(message) => {
                self.outputs
                    .push_back(MVBAOutput::Broadcast(SlotFallbackMessage::DummyView(
                        message,
                    )));
            }
            DummyViewEffect::FastBlock(block) => {
                // a fast input is already the offer, with the same entries as any fast block
                if let Some(FallbackInput::NonFast { .. }) = &self.input
                    && let Some(value) = CertifiedMetablock::fast(block)
                {
                    self.core.offer_proposal(value);
                }
            }
            DummyViewEffect::NfCert(nf_cert) => {
                // a fast block held is, or is about to be, the offer
                if self.dummy_view.fast_block().is_none()
                    && let Some(FallbackInput::NonFast { block, fbcert }) = &self.input
                    && let Some(value) =
                        CertifiedMetablock::non_fast(block.clone(), fbcert.clone(), nf_cert)
                {
                    self.core.offer_proposal(value);
                }
            }
            DummyViewEffect::Decided { qc, block } => {
                if self.decision.is_some() {
                    return;
                }
                // persist-before-send: the certificate that decides the slot
                self.decision = Some((block.clone(), SlotCommitProof::Dummy(qc.clone())));
                self.outputs
                    .push_back(MVBAOutput::Broadcast(SlotFallbackMessage::DummyView(
                        DummyViewMessage::DummyDecision { qc, block },
                    )));
                self.core.abandon();
                self.dummy_view.abandon();
            }
        }
    }
}

impl Mvba<Metablock> for SlotFallback {
    type Message = SlotFallbackMessage;
    type Context = MvbaContext;
    // the core's timers only: the dummy view has none
    type TimerEvent = TimerEvent<CertifiedMetablock>;
    type Input = (Metablock, Option<EnterFallbackCert>);
    type DecisionProof = SlotCommitProof;

    fn new(ctx: MvbaContext) -> Self {
        Self {
            dummy_view: DummyView::new(&ctx),
            core: MonadMvba::new(ctx),
            input: None,
            held: Vec::new(),
            decision: None,
            outputs: VecDeque::new(),
            abandoned: false,
        }
    }

    fn propose(&mut self, (data, cert): (Metablock, Option<EnterFallbackCert>)) {
        if self.abandoned || self.input.is_some() {
            return;
        }

        // `fbcert` is ⊥ for a fast metablock, whatever certificate admitted the path
        let input = match (data.is_fast(), cert) {
            (true, _) => FallbackInput::Fast(data.clone()),
            (false, Some(fbcert)) => FallbackInput::NonFast {
                block: data.clone(),
                fbcert,
            },
            // a non-fast metablock without its fbcert is no input: the instance stays unstarted
            (false, None) => {
                tracing::warn!(slot = ?data.slot(), "non-fast fallback input without an fbcert ignored");
                return;
            }
        };
        // offered before starting, so the core's first pass already holds it
        if let FallbackInput::Fast(block) = &input
            && let Some(value) = CertifiedMetablock::fast(block.clone())
        {
            self.core.offer_proposal(value);
        }
        self.core.start();
        self.input = Some(input);

        self.dummy_view.enter(data);
        for effect in std::mem::take(&mut self.held) {
            self.apply(effect);
        }
        self.drain();
    }

    fn validator_data(&self) -> &ValidatorData {
        self.core.validator_data()
    }

    fn handle_message(&mut self, sender: ValidatorIndex, message: SlotFallbackMessage) {
        match message {
            SlotFallbackMessage::DummyView(message) => {
                self.dummy_view.handle_message(sender, message)
            }
            SlotFallbackMessage::Mvba(message) => self.core.handle_message(sender, message),
        }
        self.drain();
    }

    fn handle_timer(&mut self, timer_event: TimerEvent<CertifiedMetablock>) {
        self.core.handle_timer(timer_event);
        self.drain();
    }

    fn abandon(&mut self) {
        self.abandoned = true;
        self.core.abandon();
        self.dummy_view.abandon();
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
                types::{HeaderAuth, NodeId, StrongQc, VoteMsg, VotePool},
            },
            EnterFallbackVote,
            dummy_view::{DummyVote, NotFull},
            monad_mvba::{PrePrepareMsg, test_helpers as h},
        },
        *,
    };

    type Core = MvbaMessage<CertifiedMetablock>;
    type Proposal = PrePrepareMsg<CertifiedMetablock>;
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

    fn nfcert() -> NfCert {
        h::strong_qc(h::SLOT, NotFull, &h::quorum(), &h::validator_data())
    }

    /// An `NfCert` over the declarations of `signers`, however few
    fn nfcert_of(signers: &[NodeId]) -> NfCert {
        let mut pool = VotePool::new(h::SLOT);
        for node in signers {
            let declaration = VoteMsg::new_signed(h::SLOT, NotFull, &node.keypair());
            pool.add_vote(dummy_index(node), declaration);
        }
        let validator_data = h::validator_data();
        let tally = pool.tally(&validator_data);
        let (_, sigcol) = tally.groups().next().expect("the signers declared");
        StrongQc {
            scope: h::SLOT,
            verdict: NotFull,
            sigcol: sigcol.clone(),
        }
    }

    /// The non-fast metablock on `seed`, with valid certificates
    fn certified(seed: u64) -> CertifiedMetablock {
        CertifiedMetablock::non_fast(non_fast(seed), fbcert(), nfcert())
            .expect("a non-fast metablock")
    }

    /// An instance that has proposed `input`, a non-fast one beside its
    /// fallback certificate
    fn started(node: NodeId, input: &Metablock) -> SlotFallback {
        let mut instance = slot_fallback(node);
        instance.propose((input.clone(), (!input.is_fast()).then(fbcert)));
        instance
    }

    fn fast_input(block: &Metablock) -> SlotFallbackMessage {
        SlotFallbackMessage::DummyView(DummyViewMessage::FastInput(block.clone()))
    }

    fn dummy_vote_of(node: NodeId, block: &Metablock) -> DummyViewMessage {
        let vote = DummyVote(block.entries());
        DummyViewMessage::DummyVote(VoteMsg::new_signed(h::SLOT, vote, &node.keypair()))
    }

    fn dummy_vote(node: NodeId, block: &Metablock) -> SlotFallbackMessage {
        SlotFallbackMessage::DummyView(dummy_vote_of(node, block))
    }

    fn not_full_of(node: NodeId) -> DummyViewMessage {
        DummyViewMessage::NotFull(VoteMsg::new_signed(h::SLOT, NotFull, &node.keypair()))
    }

    fn not_full(node: NodeId) -> SlotFallbackMessage {
        SlotFallbackMessage::DummyView(not_full_of(node))
    }

    fn dummy_decision(block: &Metablock) -> SlotFallbackMessage {
        let vote = DummyVote(block.entries());
        SlotFallbackMessage::DummyView(DummyViewMessage::DummyDecision {
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

    fn dummy_view_messages(outputs: &[Output]) -> Vec<&DummyViewMessage> {
        broadcasts(outputs)
            .into_iter()
            .filter_map(|message| match message {
                SlotFallbackMessage::DummyView(message) => Some(message),
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

    /// The entries of the prepare vote in `outputs`, if there is one
    fn prepared(outputs: &[Output]) -> Option<Lanes<Entry>> {
        broadcasts(outputs)
            .into_iter()
            .find_map(|message| match message {
                SlotFallbackMessage::Mvba(MvbaMessage::Prepare(vote)) => Some(vote.vote.0.clone()),
                _ => None,
            })
    }

    /// `message` as a peer receives it, `None` if it does not decode
    fn delivered(message: SlotFallbackMessage) -> Option<SlotFallbackMessage> {
        alloy_rlp::decode_exact::<SlotFallbackMessage>(alloy_rlp::encode(&message)).ok()
    }

    /// The leader's Pre-Prepare of `value` in `view`, justified by `tc`
    fn pre_prepare(
        view: u64,
        value: CertifiedMetablock,
        tc: Option<h::TimeoutCertificate>,
    ) -> SlotFallbackMessage {
        let leader = h::leader_of(h::view(view));
        let proposal = Proposal::new_signed(h::SLOT, h::view(view), value, tc, &leader.keypair());
        SlotFallbackMessage::Mvba(Core::PrePrepare(proposal))
    }

    fn is_dummy_decision(message: &DummyViewMessage) -> bool {
        matches!(message, DummyViewMessage::DummyDecision { .. })
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

    /// `[block, certs..]`, the wire layout, for any pairing
    fn wire(block: &Metablock, certs: &[&dyn Encodable]) -> Vec<u8> {
        let mut fields: Vec<&dyn Encodable> = vec![block];
        fields.extend_from_slice(certs);
        let mut out = Vec::new();
        alloy_rlp::encode_list::<_, dyn Encodable>(&fields, &mut out);
        out
    }

    fn decodes(bytes: &[u8]) -> bool {
        alloy_rlp::decode_exact::<CertifiedMetablock>(bytes).is_ok()
    }

    #[test]
    fn a_non_fast_metablock_without_both_certificates_is_invalid() {
        let context = validation_context();
        let block = non_fast(1);

        let certified = certified(1);
        assert!(certified.validate(&context));
        assert_eq!(certified.fbcert(), Some(&fbcert()));
        assert_eq!(certified.nfcert(), Some(&nfcert()));
        assert_eq!(
            wire(&block, &[&fbcert(), &nfcert()]),
            alloy_rlp::encode(&certified)
        );

        assert!(CertifiedMetablock::fast(block.clone()).is_none());
        assert!(!decodes(&wire(&block, &[])));
        assert!(!decodes(&wire(&block, &[&fbcert()])));
        assert!(!decodes(&wire(&block, &[&nfcert()])));
        assert!(!decodes(&wire(&block, &[&fbcert(), &nfcert(), &nfcert()])));

        // a certificate of another slot is not this metablock's, and a
        // declaration certificate below quorum admits nothing
        let validators = h::validator_data();
        let foreign_fbcert = h::strong_qc(Slot(1), EnterFallbackVote, &h::quorum(), &validators);
        let foreign_nfcert = h::strong_qc(Slot(1), NotFull, &h::quorum(), &validators);
        let thin_nfcert = nfcert_of(&h::quorum()[..2]);
        for (fbcert, nfcert) in [
            (foreign_fbcert, nfcert()),
            (fbcert(), foreign_nfcert),
            (fbcert(), thin_nfcert),
        ] {
            let misplaced = CertifiedMetablock::non_fast(block.clone(), fbcert, nfcert)
                .expect("a non-fast metablock");
            assert!(!misplaced.validate(&context), "{misplaced:?}");
            // validity is judged at ingress, not at decode
            assert!(decodes(&alloy_rlp::encode(&misplaced)));
        }
    }

    #[test]
    fn a_fast_metablock_carrying_a_certificate_is_invalid() {
        let context = validation_context();
        let block = fast(1);

        let bare = CertifiedMetablock::fast(block.clone()).expect("a fast metablock");
        assert!(bare.validate(&context));
        assert!(bare.fbcert().is_none() && bare.nfcert().is_none());
        assert_eq!(wire(&block, &[]), alloy_rlp::encode(&bare));

        assert!(CertifiedMetablock::non_fast(block.clone(), fbcert(), nfcert()).is_none());
        assert!(!decodes(&wire(&block, &[&fbcert(), &nfcert()])));
        assert!(!decodes(&wire(&block, &[&fbcert()])));
    }

    #[test]
    fn entries_ignore_the_certificates() {
        let block = non_fast(1);
        let certified = certified(1);

        assert_eq!(certified.entries(), block.entries());
        // nor do they tell a fast metablock from a non-fast one
        let fast_value = CertifiedMetablock::fast(fast(1)).expect("a fast metablock");
        assert_eq!(certified.entries(), fast_value.entries());
    }

    #[test]
    fn a_certified_metablock_round_trips() {
        let context = validation_context();
        let fast_value = CertifiedMetablock::fast(fast(1)).expect("a fast metablock");
        let certified = certified(1);
        for value in [&fast_value, &certified] {
            assert!(assert_roundtrip(value).validate(&context));
        }

        // the pairing is checked at decode, validity at ingress
        assert!(!decodes(&wire(&non_fast(1), &[])));
        assert!(!decodes(&wire(&non_fast(1), &[&fbcert()])));
    }

    #[test]
    fn propose_starts_view_1_and_the_dummy_view_together() {
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
        assert_eq!(dummy_view_messages(&outputs), vec![&not_full_of(follower)]);
        assert_eq!(broadcasts(&outputs).len(), 1, "a follower proposes nothing");

        let block = fast(1);
        let mut instance = started(follower, &block);
        let outputs = drain(&mut instance);
        assert_eq!(timers(&outputs), vec![TimerEvent::ViewTimeout(h::view(1))]);
        let sent = dummy_view_messages(&outputs);
        assert_eq!(sent.len(), 2, "{sent:?}");
        assert!(sent.contains(&&DummyViewMessage::FastInput(block.clone())));
        assert!(sent.contains(&&dummy_vote_of(follower, &block)));
        assert_eq!(broadcasts(&outputs).len(), 2);

        // one input per instance
        instance.propose((non_fast(2), Some(fbcert())));
        assert!(drain(&mut instance).is_empty());
    }

    #[test]
    fn a_non_fast_follower_votes_and_times_out_before_any_nf_cert() {
        let follower = h::nodes()[0];
        let leader = h::leader_of(h::view(1));
        assert_ne!(leader, follower, "test assumes a follower");

        let mut instance = started(follower, &non_fast(1));
        drain(&mut instance);

        // no declaration has reached it: its own input is no value yet, but a
        // leader's valid one is prepared all the same
        let value = certified(2);
        deliver(&mut instance, leader, pre_prepare(1, value.clone(), None));
        let outputs = drain(&mut instance);
        assert_eq!(prepared(&outputs), Some(value.entries()));

        instance.handle_timer(TimerEvent::ViewTimeout(h::view(1)));
        let outputs = drain(&mut instance);
        assert!(
            broadcasts(&outputs).iter().any(|message| matches!(
                message,
                SlotFallbackMessage::Mvba(MvbaMessage::Timeout(timeout)) if timeout.view() == h::view(1)
            )),
            "it times out like any other validator"
        );
    }

    #[test]
    fn a_fast_leader_proposes_at_entry_beside_its_dummy_vote() {
        let leader = h::leader_of(h::view(1));
        let block = fast(1);
        let mut instance = slot_fallback(leader);

        // the certificate that admitted the path is no part of a fast value
        instance.propose((block.clone(), Some(fbcert())));
        let outputs = drain(&mut instance);

        let sent = dummy_view_messages(&outputs);
        assert_eq!(sent.len(), 2, "{sent:?}");
        assert!(sent.contains(&&DummyViewMessage::FastInput(block.clone())));
        assert!(sent.contains(&&dummy_vote_of(leader, &block)));

        let proposed = proposals(&outputs);
        assert_eq!(proposed.len(), 1);
        let proposal = proposed[0];
        assert_eq!(proposal.view, h::view(1));
        let value = CertifiedMetablock::fast(block).expect("a fast metablock");
        assert_eq!(proposal.value, value);
        assert!(proposal.value.validate(&validation_context()));
        assert!(proposal.tc.is_none());
        assert!(proposal.value.fbcert().is_none() && proposal.value.nfcert().is_none());
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
        assert_eq!(proposal.value.block(), &block);
        assert_eq!(proposal.value.fbcert(), Some(&fbcert()));
        let nf_cert = proposal
            .value
            .nfcert()
            .expect("a non-fast value carries the NfCert");
        assert!(nf_cert.validate(&validation_context()));
        assert!(proposal.value.validate(&validation_context()));
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
        assert!(proposed[0].value.nfcert().is_none());

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
        assert_eq!(proposal.value.block(), &block);
        assert_eq!(proposal.value.fbcert(), Some(&fbcert()));
        let nf_cert = proposal
            .value
            .nfcert()
            .expect("a non-fast value carries the NfCert");
        assert!(nf_cert.validate(&validation_context()));
    }

    #[test]
    fn a_view_2_leader_proposes_a_received_fast_block_over_its_own_non_fast_input() {
        // Cases: the NFCert arrives before or after the received fast block, and the
        // leader's own input is set before or after both.
        let leader = h::leader_of(h::view(2));
        assert_ne!(
            h::leader_of(h::view(1)),
            leader,
            "test assumes a view-2 leader"
        );
        let own = non_fast(1);
        let received = fast(2);

        let deliver_nf_cert = |instance: &mut SlotFallback| {
            for node in h::quorum() {
                deliver(instance, node, not_full(node));
            }
        };
        let deliver_fast_block = |instance: &mut SlotFallback| {
            deliver(instance, h::nodes()[0], fast_input(&received));
        };

        for nf_cert_first in [true, false] {
            for before_propose in [false, true] {
                let case =
                    format!("nf_cert_first: {nf_cert_first}, before_propose: {before_propose}");
                let mut instance = slot_fallback(leader);
                if !before_propose {
                    instance.propose((own.clone(), Some(fbcert())));
                }
                if nf_cert_first {
                    deliver_nf_cert(&mut instance);
                    deliver_fast_block(&mut instance);
                } else {
                    deliver_fast_block(&mut instance);
                    deliver_nf_cert(&mut instance);
                }
                if before_propose {
                    instance.propose((own.clone(), Some(fbcert())));
                }
                assert!(proposals(&drain(&mut instance)).is_empty(), "{case}");

                deliver(&mut instance, h::nodes()[0], tc(1));
                let outputs = drain(&mut instance);
                let proposed = proposals(&outputs);
                assert_eq!(proposed.len(), 1, "{case}");
                assert_eq!(proposed[0].view, h::view(2), "{case}");
                let value = CertifiedMetablock::fast(received.clone()).expect("a fast metablock");
                assert_eq!(proposed[0].value, value, "{case}");
                assert!(
                    proposed[0].value.fbcert().is_none() && proposed[0].value.nfcert().is_none(),
                    "{case}"
                );
            }
        }
    }

    #[test]
    fn a_fast_input_is_not_displaced_by_a_received_fast_block() {
        let received = fast(2);
        for view in [1, 2] {
            let leader = h::leader_of(h::view(view));
            assert_ne!(h::nodes()[0], leader, "test assumes another sender");
            let own = fast(1);
            let mut instance = started(leader, &own);
            deliver(&mut instance, h::nodes()[0], fast_input(&received));
            deliver(&mut instance, h::nodes()[0], tc(1));

            let outputs = drain(&mut instance);
            let proposed = proposals(&outputs);
            assert_eq!(proposed.len(), 1, "view {view}");
            assert_eq!(proposed[0].view, h::view(view));
            let value = CertifiedMetablock::fast(own).expect("a fast metablock");
            assert_eq!(proposed[0].value, value, "view {view}");
        }
    }

    #[test]
    fn entering_directly_into_view_2_still_sends_evidence_and_the_dummy_vote() {
        let node = h::nodes()[0];
        for block in [fast(1), non_fast(2)] {
            let mut instance = slot_fallback(node);
            deliver(&mut instance, h::nodes()[1], tc(1));
            assert!(drain(&mut instance).is_empty());

            instance.propose((block.clone(), (!block.is_fast()).then(fbcert)));
            let outputs = drain(&mut instance);
            assert!(
                timers(&outputs).contains(&TimerEvent::ViewTimeout(h::view(2))),
                "the buffered certificate takes the core past view 1 at entry"
            );

            let sent = dummy_view_messages(&outputs);
            if block.is_fast() {
                assert_eq!(sent.len(), 2, "{sent:?}");
                assert!(sent.contains(&&DummyViewMessage::FastInput(block.clone())));
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
        let decision = DummyViewMessage::DummyDecision {
            qc: qc.clone(),
            block: block.clone(),
        };
        assert_eq!(
            broadcasts(&outputs),
            vec![&SlotFallbackMessage::DummyView(decision)],
            "the certificate and its block are passed on"
        );

        // the core is abandoned: no vote on the leader's proposal, no timeout,
        // and its own certificate changes nothing
        let value = CertifiedMetablock::fast(block.clone()).expect("a fast metablock");
        let proposal = Proposal::new_signed(h::SLOT, h::view(1), value, None, &leader.keypair());
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
        assert!(
            dummy_view_messages(&outputs)
                .into_iter()
                .any(is_dummy_decision)
        );

        instance.handle_timer(TimerEvent::ViewTimeout(h::view(2)));
        assert!(drain(&mut instance).is_empty(), "the core is abandoned");
    }

    #[test]
    fn a_commit_qc_decides_with_the_mvba_proof() {
        let node = h::nodes()[0];
        let block = non_fast(1);
        let mut instance = started(node, &block);
        // the non-fast input becomes a value this instance holds once the NFCert forms
        for declarer in h::quorum() {
            deliver(&mut instance, declarer, not_full(declarer));
        }
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
    fn a_commit_qc_over_a_non_fast_input_decides_before_its_nf_cert() {
        let node = h::nodes()[0];
        let block = non_fast(1);
        let qc = commit_qc(&block);
        let certificate = SlotFallbackMessage::Mvba(Core::CommitQc(qc.clone()));

        // no NFCert yet: the input is not a value the core holds
        let mut instance = started(node, &block);
        drain(&mut instance);
        deliver(&mut instance, h::nodes()[1], certificate.clone());
        let outputs = drain(&mut instance);
        assert_eq!(instance.decision(), Some(&block));
        assert_eq!(
            instance.decision_proof(),
            Some(&SlotCommitProof::Mvba(qc.clone()))
        );
        assert!(broadcasts(&outputs).contains(&&certificate));

        instance.handle_timer(TimerEvent::ViewTimeout(h::view(1)));
        assert!(drain(&mut instance).is_empty(), "the core is abandoned");

        // a certificate held before the input is registered decides at propose
        let mut instance = slot_fallback(node);
        deliver(&mut instance, h::nodes()[1], certificate);
        assert!(instance.decision().is_none());
        instance.propose((block.clone(), Some(fbcert())));
        assert_eq!(instance.decision(), Some(&block));

        // other entries are not this validator's to decide without the block
        let mut instance = started(node, &non_fast(2));
        let other = SlotFallbackMessage::Mvba(Core::CommitQc(qc));
        deliver(&mut instance, h::nodes()[1], other);
        assert!(instance.decision().is_none());
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
        instance.propose((own.clone(), Some(fbcert())));
        let outputs = drain(&mut instance);
        let proposed = proposals(&outputs);
        assert_eq!(proposed.len(), 1);
        assert_eq!(proposed[0].value.block(), &own);
        assert!(proposed[0].value.nfcert().is_some());

        // a fast block: a non-fast leader proposes it at entry instead
        let received = fast(2);
        let mut instance = slot_fallback(leader);
        deliver(&mut instance, h::nodes()[0], fast_input(&received));
        assert!(drain(&mut instance).is_empty());
        instance.propose((own, Some(fbcert())));
        let outputs = drain(&mut instance);
        let proposed = proposals(&outputs);
        assert_eq!(proposed.len(), 1);
        assert_eq!(proposed[0].value.block(), &received);
        assert!(proposed[0].value.nfcert().is_none());

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
        instance.propose((block.clone(), None));
        assert_eq!(instance.decision(), Some(&block));
        assert!(matches!(
            instance.decision_proof(),
            Some(SlotCommitProof::Dummy(_))
        ));
        let outputs = drain(&mut instance);
        assert!(
            dummy_view_messages(&outputs)
                .into_iter()
                .any(is_dummy_decision)
        );
    }

    /// The non-fast metablock on `seed` beside the certificates given
    fn non_fast_with(seed: u64, fbcert: EnterFallbackCert, nfcert: NfCert) -> CertifiedMetablock {
        CertifiedMetablock::non_fast(non_fast(seed), fbcert, nfcert).expect("a non-fast metablock")
    }

    /// Values valid in all but one certificate, each paired with what it gets wrong
    fn values_with_a_bad_certificate(seed: u64) -> Vec<(&'static str, CertifiedMetablock)> {
        let validators = h::validator_data();
        let other_slot = Slot(h::SLOT.get() + 1);
        vec![
            (
                "an FBCert of another slot",
                non_fast_with(
                    seed,
                    h::strong_qc(other_slot, EnterFallbackVote, &h::quorum(), &validators),
                    nfcert(),
                ),
            ),
            (
                "an NFCert of another slot",
                non_fast_with(
                    seed,
                    fbcert(),
                    h::strong_qc(other_slot, NotFull, &h::quorum(), &validators),
                ),
            ),
            (
                "an NFCert below quorum",
                non_fast_with(seed, fbcert(), nfcert_of(&h::quorum()[..2])),
            ),
        ]
    }

    #[test]
    fn a_view_1_proposal_is_prepared_only_with_valid_certificates() {
        let follower = h::nodes()[0];
        let leader = h::leader_of(h::view(1));
        assert_ne!(leader, follower, "test assumes a follower");

        // what a follower prepares in view 1, given `value` as a peer receives it
        let prepared_in_view_1 = |value: CertifiedMetablock| {
            let mut instance = started(follower, &fast(1));
            drain(&mut instance);
            if let Some(message) = delivered(pre_prepare(1, value, None)) {
                deliver(&mut instance, leader, message);
            }
            prepared(&drain(&mut instance))
        };

        let bare_fast = CertifiedMetablock::fast(fast(2)).expect("a fast metablock");
        assert_eq!(
            prepared_in_view_1(bare_fast),
            Some(fast(2).entries()),
            "a fast metablock stands alone"
        );
        assert_eq!(
            prepared_in_view_1(certified(2)),
            Some(non_fast(2).entries())
        );
        for (case, value) in values_with_a_bad_certificate(2) {
            assert_eq!(prepared_in_view_1(value), None, "{case}");
        }
    }

    /// D0. A view whose timeout certificate carries no lock leaves its leader as
    /// free as view 1 does, so the same admission rule has to hold there
    #[test]
    fn an_unlocked_later_view_proposal_is_prepared_only_with_valid_certificates() {
        let follower = h::nodes()[0];
        let leader = h::leader_of(h::view(2));
        assert_ne!(leader, follower, "test assumes a follower");
        let unlocked = h::timeout_certificate(h::view(1), None, &h::validator_data());

        // what a follower that reached view 2 prepares, given `value` as a peer receives it
        let prepared_in_view_2 = |value: CertifiedMetablock| {
            let mut instance = started(follower, &fast(1));
            deliver(&mut instance, h::nodes()[1], tc(1));
            let entered = drain(&mut instance);
            assert!(timers(&entered).contains(&TimerEvent::ViewTimeout(h::view(2))));

            if let Some(message) = delivered(pre_prepare(2, value, Some(unlocked.clone()))) {
                deliver(&mut instance, leader, message);
            }
            prepared(&drain(&mut instance))
        };

        for (case, value) in values_with_a_bad_certificate(2) {
            assert_eq!(
                prepared_in_view_2(value),
                None,
                "nothing locked, so the value must stand on its own: {case}"
            );
        }
        let bare_fast = CertifiedMetablock::fast(fast(2)).expect("a fast metablock");
        assert_eq!(
            prepared_in_view_2(bare_fast),
            Some(fast(2).entries()),
            "a fast metablock needs no certificate in any view"
        );
        assert_eq!(
            prepared_in_view_2(certified(2)),
            Some(non_fast(2).entries()),
            "the certificates admit the value here exactly as they do in view 1"
        );
    }

    #[test]
    fn a_locked_non_fast_value_is_re_proposed_with_its_certificates() {
        let follower = h::nodes()[0];
        let leader = h::leader_of(h::view(2));
        assert_ne!(leader, follower, "test assumes a follower");
        let validators = h::validator_data();
        let lock = h::prepare_qc(h::view(1), &non_fast(2).entries(), &validators);
        let locked = h::timeout_certificate(h::view(1), Some(lock), &validators);

        let prepared_in_view_2 = |value: CertifiedMetablock| {
            let mut instance = started(follower, &fast(1));
            drain(&mut instance);
            let entry = SlotFallbackMessage::Mvba(Core::Tc(locked.clone()));
            deliver(&mut instance, h::nodes()[1], entry);
            drain(&mut instance);

            let message = delivered(pre_prepare(2, value, Some(locked.clone())))
                .expect("a proposal that keeps the lock decodes");
            deliver(&mut instance, leader, message);
            prepared(&drain(&mut instance))
        };

        assert_eq!(
            prepared_in_view_2(certified(2)),
            Some(non_fast(2).entries()),
            "the locked value travels with its certificates"
        );
        // they are checked under a lock too
        for (case, value) in values_with_a_bad_certificate(2) {
            assert_eq!(prepared_in_view_2(value), None, "{case}");
        }
    }

    fn kind(message: &SlotFallbackMessage) -> &'static str {
        match message {
            SlotFallbackMessage::DummyView(message) => match message {
                DummyViewMessage::FastInput(_) => "fast input",
                DummyViewMessage::NotFull(_) => "not full",
                DummyViewMessage::DummyVote(_) => "dummy vote",
                DummyViewMessage::DummyDecision { .. } => "dummy decision",
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
                instance.propose((block.clone(), Some(fbcert())));
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
            proposal.tc.is_some()
                && proposal.value.fbcert().is_some()
                && proposal.value.nfcert().is_some()
        }));
        assert!(proposed.iter().any(|proposal| {
            proposal.tc.is_none()
                && proposal.value.fbcert().is_none()
                && proposal.value.nfcert().is_none()
        }));

        messages.dedup();
        for message in messages {
            assert_eq!(message.slot(), h::SLOT);
            assert_roundtrip(&message);
            // the outer Chorus and Cadence framing wraps the fallback arm
            assert_serialization_roundtrip(&Wire::Slot(ChorusMessage::Fallback(message)));
        }

        assert!(alloy_rlp::decode_exact::<SlotFallbackMessage>(&[0xc1, 3]).is_err());
        assert!(alloy_rlp::decode_exact::<DummyViewMessage>(&[0xc1, 5]).is_err());
    }
}
