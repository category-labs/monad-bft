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

//! The dummy vote, run beside view 1 of the MVBA rather than before it: a
//! validator whose input is a fast metablock votes for its entries, any other
//! declares `NotFull`. Nothing here depends on the view the MVBA is in

use std::{collections::VecDeque, sync::Arc};

use alloy_rlp::{
    Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
};

use super::{
    super::types::{IsVote, KeyPair, Lanes, Slot, StrongQc, ValidatorIndex, VoteMsg},
    Entry, Metablock, ValidateCert, ValidateInput,
    monad_mvba::{
        MakesValidationContext, MvbaContext, ValidationContext, collectors::SealingVotePool,
    },
};
use crate::spec::vote::{SigningDomain, assert_signing_prefix};

/// `⟨DummyVote, slot, entries(x)⟩` over the entries of a fast metablock
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct DummyVote(pub Lanes<Entry>);

pub struct DummyVoteDomain;
const _: () = assert_signing_prefix::<DummyVoteDomain>();

impl SigningDomain for DummyVoteDomain {
    const PREFIX: &'static [u8] = b"\x20monad/cadence/mvba-dummy-vote/1\n";
}

impl IsVote for DummyVote {
    type Scope = Slot;
    type SigningDomain = DummyVoteDomain;
}

/// `DummyQC`: 2f+1 dummy votes on the same entries, which decides the slot
pub type DummyQc = StrongQc<DummyVote>;

/// `⟨NotFull, slot⟩`: the signer's registered input is not a fast metablock
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
pub struct NotFull;

pub struct NotFullDomain;
const _: () = assert_signing_prefix::<NotFullDomain>();

impl SigningDomain for NotFullDomain {
    const PREFIX: &'static [u8] = b"\x1Emonad/cadence/mvba-not-full/1\n";
}

impl IsVote for NotFull {
    type Scope = Slot;
    type SigningDomain = NotFullDomain;
}

/// `NFCert`: 2f+1 declarations, which admits an unlocked non-fast proposal
pub type NfCert = StrongQc<NotFull>;

impl ValidateCert for NfCert {
    type Context = ValidationContext;

    fn validate(&self, context: &Self::Context) -> bool {
        self.scope == context.slot && self.verify(&context.validator_data)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DummyViewMessage {
    /// The sender's registered input, a fast metablock
    FastInput(Metablock),
    NotFull(VoteMsg<NotFull>),
    DummyVote(VoteMsg<DummyVote>),
    /// So a validator that missed the votes or the block can still decide
    DummyDecision {
        qc: DummyQc,
        block: Metablock,
    },
}

impl DummyViewMessage {
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

impl Encodable for DummyViewMessage {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        alloy_rlp::encode_list::<_, dyn Encodable>(&self.rlp_fields(), out);
    }

    fn length(&self) -> usize {
        alloy_rlp::list_length::<_, dyn Encodable>(&self.rlp_fields())
    }
}

impl Decodable for DummyViewMessage {
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
            _ => return Err(alloy_rlp::Error::Custom("unknown DummyViewMessage tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

/// What the dummy view hands its owner; consumed by `SlotFallback::apply`
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum DummyViewEffect {
    Broadcast(DummyViewMessage),
    /// The first valid fast metablock seen, which any leader may propose
    FastBlock(Metablock),
    NfCert(NfCert),
    Decided {
        qc: DummyQc,
        block: Metablock,
    },
}

pub(crate) struct DummyView {
    slot: Slot,
    key: Arc<KeyPair>,
    validation_context: ValidationContext,

    abandoned: bool,

    /// The first valid fast metablock received
    fast_block: Option<Metablock>,
    not_full: SealingVotePool<NotFull>,
    dummy_votes: SealingVotePool<DummyVote>,

    effects: VecDeque<DummyViewEffect>,
}

impl DummyView {
    /// Messages are retained from construction, before [`Self::enter`]
    pub(crate) fn new(ctx: &MvbaContext) -> Self {
        Self {
            slot: ctx.slot,
            key: ctx.key.clone(),
            validation_context: MakesValidationContext::<Metablock>::make_validation_context(ctx),
            abandoned: false,
            fast_block: None,
            not_full: SealingVotePool::new(ctx.slot),
            dummy_votes: SealingVotePool::new(ctx.slot),
            effects: VecDeque::new(),
        }
    }

    /// Registers this validator's input and sends its evidence. Called once:
    /// the owner guards on its own registered input
    pub(crate) fn enter(&mut self, input: Metablock) {
        if self.abandoned {
            return;
        }

        // persist-before-send: the registered input, which fixes DummyVote or NotFull
        if input.is_fast() {
            let vote = VoteMsg::new_signed(self.slot, DummyVote(input.entries()), &self.key);
            self.broadcast(DummyViewMessage::FastInput(input));
            self.broadcast(DummyViewMessage::DummyVote(vote));
        } else {
            let declaration = VoteMsg::new_signed(self.slot, NotFull, &self.key);
            self.broadcast(DummyViewMessage::NotFull(declaration));
        }
    }

    pub(crate) fn handle_message(&mut self, sender: ValidatorIndex, msg: DummyViewMessage) {
        if self.abandoned || msg.slot() != self.slot || self.decision().is_some() {
            return;
        }
        let had_nf_cert = self.not_full.sealed().is_some();

        match msg {
            DummyViewMessage::FastInput(block) => self.handle_fast_input(block),
            DummyViewMessage::NotFull(declaration) => self.not_full.add_vote(sender, declaration),
            DummyViewMessage::DummyVote(vote) => self.dummy_votes.add_vote(sender, vote),
            DummyViewMessage::DummyDecision { qc, block } => self.handle_dummy_decision(qc, block),
        }

        let validator_data = &self.validation_context.validator_data;
        if !had_nf_cert && let Some(nf_cert) = self.not_full.try_form_strong_qc(validator_data) {
            self.effects
                .push_back(DummyViewEffect::NfCert(nf_cert.clone()));
        }
        self.dummy_votes.try_form_strong_qc(validator_data);
        if let Some((qc, block)) = self.decision() {
            let (qc, block) = (qc.clone(), block.clone());
            self.effects
                .push_back(DummyViewEffect::Decided { qc, block });
        }
    }

    /// The valid fast metablock held, received or certified
    pub(crate) fn fast_block(&self) -> Option<&Metablock> {
        self.fast_block.as_ref()
    }

    /// The DummyQC and the fast block it certifies, once both are held
    fn decision(&self) -> Option<(&DummyQc, &Metablock)> {
        let qc = self.dummy_votes.sealed()?;
        let block = self.fast_block.as_ref()?;
        (qc.verdict.0 == block.entries()).then_some((qc, block))
    }

    pub(crate) fn abandon(&mut self) {
        self.abandoned = true;
        self.effects.clear();
    }

    pub(crate) fn poll(&mut self) -> Option<DummyViewEffect> {
        self.effects.pop_front()
    }

    fn broadcast(&mut self, msg: DummyViewMessage) {
        self.effects.push_back(DummyViewEffect::Broadcast(msg));
    }

    fn is_fast_block(&self, block: &Metablock) -> bool {
        block.is_fast() && block.validate(&self.validation_context)
    }

    fn handle_fast_input(&mut self, block: Metablock) {
        if self.fast_block.is_some() || !self.is_fast_block(&block) {
            return;
        }
        self.fast_block = Some(block.clone());
        self.effects.push_back(DummyViewEffect::FastBlock(block));
    }

    fn handle_dummy_decision(&mut self, qc: DummyQc, block: Metablock) {
        if qc.verdict.0 != block.entries()
            || !self.is_fast_block(&block)
            || !qc.verify(&self.validation_context.validator_data)
        {
            return;
        }
        self.dummy_votes.seal(qc);
        self.fast_block = Some(block);
    }
}

#[cfg(test)]
mod tests {
    //! A four-validator cluster (f = 1) on the stub environment, built as the
    //! core's `test_helpers` builds it; those fixtures are private to the core

    use std::{collections::HashMap, marker::PhantomData, sync::Arc};

    use alloy_rlp::{Decodable, Encodable};

    use super::{
        super::{
            super::{
                super::test_utils::dummy_index,
                fast::{CertifiedEntry, EnterFallbackVote, Entry, FallbackEntry},
                types::{
                    HeaderAuth, IsVote, Lane, Lanes, NodeId, ProposalId, ProposalScope, Slot,
                    StrongQc, TimestampDelta, ValidatorData, VoteMsg, VotePool,
                },
            },
            FallbackCommitQc, FallbackView, Metablock, MvbaScope, ValidateCert,
            monad_mvba::{MakesValidationContext, MvbaContext},
        },
        DummyQc, DummyView, DummyViewEffect, DummyViewMessage, DummyVote, NotFull,
    };
    use crate::env::stub::{EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader};

    const NUM_NODES: u64 = 4;
    const NUM_LANES: usize = 2;
    const SLOT: Slot = Slot(0);

    fn nodes() -> Vec<NodeId> {
        (0..NUM_NODES).map(NodeId::dummy).collect()
    }

    /// Three of four validators: a supermajority, every stake being one
    fn quorum() -> Vec<NodeId> {
        nodes()[..3].to_vec()
    }

    fn validator_data() -> Arc<ValidatorData> {
        use monad_mcp_common::spec::KeyPair as _;

        let valset: HashMap<_, _> = nodes().into_iter().map(|n| (n, 1u64.into())).collect();
        let mapping: HashMap<_, _> = nodes()
            .into_iter()
            .map(|n| (n, n.keypair().pubkey()))
            .collect();
        Arc::new(ValidatorData::new(valset, mapping))
    }

    fn context(node: NodeId) -> MvbaContext {
        MvbaContext {
            slot: SLOT,
            num_lanes: NUM_LANES,
            delta: TimestampDelta::from_millis(10),
            node_id: node,
            key: Arc::new(node.keypair()),
            validator_data: validator_data(),
            header_auth: Arc::new(HeaderAuth::new(|_| false)),
        }
    }

    fn dummy_view(node: NodeId) -> DummyView {
        DummyView::new(&context(node))
    }

    fn pid(slot: Slot, n: u64) -> ProposalId {
        use crate::spec::ProposalHeader as _;

        let mut hash = [0u8; 20];
        hash[..8].copy_from_slice(&n.to_le_bytes());
        ProposalHeader {
            root: MerkleRoot(MerkleHash(hash)),
            scheme: EncodingScheme::dummy(slot),
        }
        .pid()
    }

    fn strong_qc<V: IsVote>(scope: V::Scope, vote: V, signers: &[NodeId]) -> StrongQc<V> {
        let mut pool = VotePool::new(scope.clone());
        for node in signers {
            let msg = VoteMsg::new_signed(scope.clone(), vote.clone(), &node.keypair());
            pool.add_vote(dummy_index(node), msg);
        }
        pool.tally(&validator_data())
            .strong_qc()
            .expect("the signers hold a supermajority of stake")
    }

    fn entry(slot: Slot, seed: u64, lane: Lane) -> Entry {
        Entry::Positive(pid(slot, seed * 100 + usize::from(lane) as u64))
    }

    /// A metablock of `num_lanes` fast entries for `slot`, valid only for
    /// `SLOT` and `NUM_LANES`
    fn fast_metablock_in(slot: Slot, num_lanes: usize, seed: u64) -> Metablock {
        Metablock::new(Lanes::new(num_lanes, |j| {
            let qc = strong_qc(ProposalScope::new(slot, j), entry(slot, seed, j), &quorum());
            CertifiedEntry::FastQc(qc)
        }))
    }

    fn fast_metablock(seed: u64) -> Metablock {
        fast_metablock_in(SLOT, NUM_LANES, seed)
    }

    /// Valid, with the entries of [`fast_metablock`] on the same seed, but its
    /// first entry rests on a fallback quorum
    fn non_fast_metablock(seed: u64) -> Metablock {
        Metablock::new(Lanes::new(NUM_LANES, |j| {
            let entry = entry(SLOT, seed, j);
            if j == Lane::new_unchecked(0) {
                let mut pool = VotePool::new(ProposalScope::new(SLOT, j));
                for node in &quorum() {
                    let msg = VoteMsg::new_signed(
                        ProposalScope::new(SLOT, j),
                        FallbackEntry(entry.clone()),
                        &node.keypair(),
                    );
                    pool.add_vote(dummy_index(node), msg);
                }
                let qc = pool.tally(&validator_data()).weak_qcs()[0].clone();
                CertifiedEntry::FallbackQc(qc)
            } else {
                let qc = strong_qc(ProposalScope::new(SLOT, j), entry, &quorum());
                CertifiedEntry::FastQc(qc)
            }
        }))
    }

    fn dummy_vote(node: NodeId, entries: &Lanes<Entry>) -> DummyViewMessage {
        DummyViewMessage::DummyVote(VoteMsg::new_signed(
            SLOT,
            DummyVote(entries.clone()),
            &node.keypair(),
        ))
    }

    fn not_full(node: NodeId) -> DummyViewMessage {
        DummyViewMessage::NotFull(VoteMsg::new_signed(SLOT, NotFull, &node.keypair()))
    }

    fn dummy_qc(entries: &Lanes<Entry>) -> DummyQc {
        strong_qc(SLOT, DummyVote(entries.clone()), &quorum())
    }

    /// An MVBA commit vote on `entries` in view 1. Its type is private to the
    /// core, so it is reached through the certificate alias and its wire form
    fn commit_vote(node: NodeId, entries: &Lanes<Entry>) -> VoteMsg<impl IsVote + use<>> {
        fn signed<V: IsVote + Decodable>(
            _: PhantomData<StrongQc<V>>,
            scope: V::Scope,
            entries: &Lanes<Entry>,
            node: NodeId,
        ) -> VoteMsg<V> {
            let vote = alloy_rlp::decode_exact::<V>(alloy_rlp::encode(entries))
                .expect("a commit vote is its entries on the wire");
            VoteMsg::new_signed(scope, vote, &node.keypair())
        }
        signed(
            PhantomData::<FallbackCommitQc<Lanes<Entry>>>,
            MvbaScope::new(SLOT, FallbackView::FIRST),
            entries,
            node,
        )
    }

    fn deliver(view: &mut DummyView, from: NodeId, msg: DummyViewMessage) {
        view.handle_message(dummy_index(from), msg);
    }

    fn drain(view: &mut DummyView) -> Vec<DummyViewEffect> {
        std::iter::from_fn(|| view.poll()).collect()
    }

    fn broadcasts(effects: &[DummyViewEffect]) -> Vec<&DummyViewMessage> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                DummyViewEffect::Broadcast(msg) => Some(msg),
                _ => None,
            })
            .collect()
    }

    fn fast_blocks(effects: &[DummyViewEffect]) -> Vec<&Metablock> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                DummyViewEffect::FastBlock(block) => Some(block),
                _ => None,
            })
            .collect()
    }

    fn nf_certs(effects: &[DummyViewEffect]) -> Vec<&super::NfCert> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                DummyViewEffect::NfCert(cert) => Some(cert),
                _ => None,
            })
            .collect()
    }

    fn decisions(effects: &[DummyViewEffect]) -> Vec<(&DummyQc, &Metablock)> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                DummyViewEffect::Decided { qc, block } => Some((qc, block)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_fast_input_sends_its_block_and_one_dummy_vote_at_entry() {
        let me = nodes()[0];
        let block = fast_metablock(1);
        let mut view = dummy_view(me);

        view.enter(block.clone());
        let effects = drain(&mut view);

        let vote = VoteMsg::new_signed(SLOT, DummyVote(block.entries()), &me.keypair());
        assert_eq!(effects.len(), 2, "only the block and the vote: {effects:?}");
        let sent = broadcasts(&effects);
        assert!(sent.contains(&&DummyViewMessage::FastInput(block)));
        assert!(sent.contains(&&DummyViewMessage::DummyVote(vote)));
    }

    #[test]
    fn a_non_fast_input_declares_not_full_and_never_votes() {
        let me = nodes()[0];
        let block = fast_metablock(1);
        let mut view = dummy_view(me);

        view.enter(non_fast_metablock(1));
        let effects = drain(&mut view);
        let declaration = VoteMsg::new_signed(SLOT, NotFull, &me.keypair());
        assert_eq!(
            effects,
            vec![DummyViewEffect::Broadcast(DummyViewMessage::NotFull(
                declaration
            ))]
        );

        // neither a later fast block nor a quorum of votes changes the choice
        deliver(
            &mut view,
            nodes()[1],
            DummyViewMessage::FastInput(block.clone()),
        );
        for node in &nodes()[1..] {
            deliver(&mut view, *node, dummy_vote(*node, &block.entries()));
        }
        let effects = drain(&mut view);
        assert!(
            broadcasts(&effects).is_empty(),
            "a non-fast validator sends nothing more: {effects:?}"
        );
    }

    #[test]
    fn every_message_is_sent_exactly_once() {
        let me = nodes()[0];
        let block = fast_metablock(1);
        let mut view = dummy_view(me);

        view.enter(block.clone());
        let mut effects = drain(&mut view);

        // the dummy view's own broadcasts come back, then everyone else's, twice
        let own: Vec<_> = broadcasts(&effects).into_iter().cloned().collect();
        for _ in 0..2 {
            for msg in &own {
                deliver(&mut view, me, msg.clone());
            }
            for node in &nodes()[1..] {
                deliver(&mut view, *node, DummyViewMessage::FastInput(block.clone()));
                deliver(&mut view, *node, dummy_vote(*node, &block.entries()));
            }
            effects.extend(drain(&mut view));
        }

        assert_eq!(broadcasts(&effects).len(), 2, "{effects:?}");
        assert_eq!(fast_blocks(&effects), vec![&block]);
        assert_eq!(decisions(&effects).len(), 1);
    }

    #[test]
    fn nothing_is_sent_before_entry_or_after_abandon() {
        let me = nodes()[0];
        let block = fast_metablock(1);
        let mut view = dummy_view(me);

        deliver(
            &mut view,
            nodes()[1],
            DummyViewMessage::FastInput(block.clone()),
        );
        for node in nodes() {
            deliver(&mut view, node, dummy_vote(node, &block.entries()));
            deliver(&mut view, node, not_full(node));
        }
        assert!(broadcasts(&drain(&mut view)).is_empty());

        // abandoning drops what entry queued, and later calls queue nothing
        view.enter(block.clone());
        view.abandon();
        assert!(view.poll().is_none());

        view.enter(non_fast_metablock(1));
        deliver(
            &mut view,
            nodes()[2],
            DummyViewMessage::FastInput(block.clone()),
        );
        deliver(
            &mut view,
            nodes()[2],
            DummyViewMessage::DummyDecision {
                qc: dummy_qc(&block.entries()),
                block,
            },
        );
        assert!(view.poll().is_none());
    }

    #[test]
    fn a_quorum_of_declarations_forms_the_certificate_once() {
        let mut view = dummy_view(nodes()[0]);
        view.enter(non_fast_metablock(1));
        drain(&mut view);

        for node in &quorum()[..2] {
            deliver(&mut view, *node, not_full(*node));
        }
        assert!(nf_certs(&drain(&mut view)).is_empty());

        deliver(&mut view, quorum()[2], not_full(quorum()[2]));
        let effects = drain(&mut view);
        let certs = nf_certs(&effects);
        assert_eq!(certs.len(), 1);
        let validation_context =
            MakesValidationContext::<Metablock>::make_validation_context(&context(nodes()[0]));
        assert!(certs[0].validate(&validation_context));
        assert_eq!(certs[0].scope, SLOT);

        for node in nodes() {
            deliver(&mut view, node, not_full(node));
        }
        assert!(nf_certs(&drain(&mut view)).is_empty());
    }

    #[test]
    fn a_second_declaration_from_one_signer_is_not_counted() {
        let mut view = dummy_view(nodes()[0]);
        let (repeater, other) = (nodes()[1], nodes()[2]);

        deliver(&mut view, repeater, not_full(repeater));
        deliver(&mut view, repeater, not_full(repeater));
        deliver(&mut view, other, not_full(other));
        assert!(
            nf_certs(&drain(&mut view)).is_empty(),
            "two signers are not a quorum of four"
        );

        deliver(&mut view, nodes()[3], not_full(nodes()[3]));
        assert_eq!(nf_certs(&drain(&mut view)).len(), 1);
    }

    #[test]
    fn a_quorum_of_dummy_votes_and_a_fast_block_decide() {
        let block = fast_metablock(1);
        let mut view = dummy_view(nodes()[0]);

        deliver(
            &mut view,
            nodes()[1],
            DummyViewMessage::FastInput(block.clone()),
        );
        for node in &quorum()[..2] {
            deliver(&mut view, *node, dummy_vote(*node, &block.entries()));
        }
        let effects = drain(&mut view);
        assert_eq!(fast_blocks(&effects), vec![&block]);
        assert!(decisions(&effects).is_empty());

        deliver(
            &mut view,
            quorum()[2],
            dummy_vote(quorum()[2], &block.entries()),
        );
        let effects = drain(&mut view);
        let decided = decisions(&effects);
        assert_eq!(decided.len(), 1);
        let (qc, decided_block) = decided[0];
        assert_eq!(decided_block, &block);
        assert_eq!(qc.scope, SLOT);
        assert_eq!(qc.verdict, DummyVote(block.entries()));
        assert!(qc.verify(&validator_data()));

        deliver(
            &mut view,
            nodes()[3],
            dummy_vote(nodes()[3], &block.entries()),
        );
        assert!(decisions(&drain(&mut view)).is_empty());
    }

    #[test]
    fn a_quorum_of_dummy_votes_waits_for_the_fast_block() {
        let block = fast_metablock(1);
        let mut view = dummy_view(nodes()[0]);

        for node in quorum() {
            deliver(&mut view, node, dummy_vote(node, &block.entries()));
        }
        // same entries, but not a fast metablock
        deliver(
            &mut view,
            nodes()[3],
            DummyViewMessage::FastInput(non_fast_metablock(1)),
        );
        assert!(decisions(&drain(&mut view)).is_empty());

        deliver(
            &mut view,
            nodes()[3],
            DummyViewMessage::FastInput(block.clone()),
        );
        let effects = drain(&mut view);
        assert_eq!(fast_blocks(&effects), vec![&block]);
        let decided = decisions(&effects);
        assert_eq!(decided.len(), 1);
        assert_eq!(decided[0].1, &block);
    }

    #[test]
    fn votes_and_declarations_received_before_entry_are_retained() {
        let me = nodes()[0];
        let block = fast_metablock(1);

        let mut declaring = dummy_view(me);
        for node in &nodes()[1..3] {
            deliver(&mut declaring, *node, not_full(*node));
        }
        assert!(nf_certs(&drain(&mut declaring)).is_empty());
        declaring.enter(non_fast_metablock(1));
        let own = broadcasts(&drain(&mut declaring))[0].clone();
        deliver(&mut declaring, me, own);
        assert_eq!(nf_certs(&drain(&mut declaring)).len(), 1);

        let mut voting = dummy_view(me);
        deliver(
            &mut voting,
            nodes()[1],
            DummyViewMessage::FastInput(block.clone()),
        );
        for node in &nodes()[1..3] {
            deliver(&mut voting, *node, dummy_vote(*node, &block.entries()));
        }
        let effects = drain(&mut voting);
        assert_eq!(fast_blocks(&effects), vec![&block]);
        assert!(decisions(&effects).is_empty());

        voting.enter(block.clone());
        let own: Vec<_> = broadcasts(&drain(&mut voting))
            .into_iter()
            .cloned()
            .collect();
        for msg in own {
            deliver(&mut voting, me, msg);
        }
        let decided = decisions(&drain(&mut voting))
            .into_iter()
            .map(|(_, block)| block.clone())
            .collect::<Vec<_>>();
        assert_eq!(decided, vec![block]);
    }

    #[test]
    fn a_non_fast_or_invalid_metablock_is_not_fast_evidence() {
        let block = fast_metablock(1);
        let mut view = dummy_view(nodes()[0]);

        let not_evidence = [
            non_fast_metablock(1),
            fast_metablock_in(Slot(1), NUM_LANES, 1),
            fast_metablock_in(SLOT, NUM_LANES + 1, 1),
        ];
        for candidate in &not_evidence {
            deliver(
                &mut view,
                nodes()[1],
                DummyViewMessage::FastInput(candidate.clone()),
            );
            for node in quorum() {
                deliver(&mut view, node, dummy_vote(node, &candidate.entries()));
            }
        }
        let effects = drain(&mut view);
        assert!(fast_blocks(&effects).is_empty(), "{effects:?}");
        assert!(decisions(&effects).is_empty());

        // the quorum on these entries is already in, so the fast block decides
        deliver(
            &mut view,
            nodes()[1],
            DummyViewMessage::FastInput(block.clone()),
        );
        let effects = drain(&mut view);
        assert_eq!(fast_blocks(&effects), vec![&block]);
        assert_eq!(decisions(&effects).len(), 1);
    }

    #[test]
    fn a_signer_of_both_messages_is_counted_in_both_pools() {
        let block = fast_metablock(1);
        let mut view = dummy_view(nodes()[0]);
        deliver(
            &mut view,
            nodes()[2],
            DummyViewMessage::FastInput(block.clone()),
        );

        let both = nodes()[1];
        deliver(&mut view, both, not_full(both));
        deliver(&mut view, both, dummy_vote(both, &block.entries()));
        for node in [nodes()[2], nodes()[3]] {
            deliver(&mut view, node, not_full(node));
        }
        for node in [nodes()[0], nodes()[3]] {
            deliver(&mut view, node, dummy_vote(node, &block.entries()));
        }

        let effects = drain(&mut view);
        assert_eq!(nf_certs(&effects).len(), 1, "{effects:?}");
        assert_eq!(decisions(&effects).len(), 1, "{effects:?}");
    }

    #[test]
    fn dummy_not_full_and_commit_signatures_do_not_cross_domains() {
        let block = fast_metablock(1);
        let entries = block.entries();
        let mut view = dummy_view(nodes()[0]);
        deliver(&mut view, nodes()[1], DummyViewMessage::FastInput(block));
        drain(&mut view);

        // a quorum's MVBA commit signatures on the same entries, as dummy votes
        for node in quorum() {
            let commit = commit_vote(node, &entries);
            let vote = VoteMsg::new(SLOT, DummyVote(entries.clone()), commit.signature);
            deliver(&mut view, node, DummyViewMessage::DummyVote(vote));
        }
        // a quorum's enter-fallback signatures, which sign the same unit verdict
        // over the same slot, as declarations
        for node in quorum() {
            let enter = VoteMsg::new_signed(SLOT, EnterFallbackVote, &node.keypair());
            let declaration = VoteMsg::new(SLOT, NotFull, enter.signature);
            deliver(&mut view, node, DummyViewMessage::NotFull(declaration));
        }
        let effects = drain(&mut view);
        assert!(decisions(&effects).is_empty(), "{effects:?}");
        assert!(nf_certs(&effects).is_empty(), "{effects:?}");

        let commit = commit_vote(nodes()[0], &entries);
        let signed = [
            DummyVote(entries).signing_bytes(&SLOT),
            NotFull.signing_bytes(&SLOT),
            EnterFallbackVote.signing_bytes(&SLOT),
            commit.vote.signing_bytes(&commit.scope),
        ];
        for (i, a) in signed.iter().enumerate() {
            for b in &signed[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn a_dummy_decision_from_a_peer_decides() {
        let block = fast_metablock(1);
        let qc = dummy_qc(&block.entries());
        let mut view = dummy_view(nodes()[0]);
        view.enter(non_fast_metablock(1));
        drain(&mut view);

        let decision = DummyViewMessage::DummyDecision {
            qc: qc.clone(),
            block: block.clone(),
        };
        deliver(&mut view, nodes()[1], decision.clone());
        assert_eq!(
            drain(&mut view),
            vec![DummyViewEffect::Decided { qc, block }]
        );

        deliver(&mut view, nodes()[2], decision);
        assert!(drain(&mut view).is_empty());
    }

    #[test]
    fn declarations_and_votes_signed_for_another_slot_are_not_counted() {
        let block = fast_metablock(1);
        let mut view = dummy_view(nodes()[0]);
        deliver(
            &mut view,
            nodes()[1],
            DummyViewMessage::FastInput(block.clone()),
        );
        drain(&mut view);

        let other = Slot(1);
        for node in quorum() {
            let declaration = VoteMsg::new_signed(other, NotFull, &node.keypair());
            deliver(&mut view, node, DummyViewMessage::NotFull(declaration));
            let vote = VoteMsg::new_signed(other, DummyVote(block.entries()), &node.keypair());
            deliver(&mut view, node, DummyViewMessage::DummyVote(vote));
        }
        let effects = drain(&mut view);
        assert!(nf_certs(&effects).is_empty(), "{effects:?}");
        assert!(decisions(&effects).is_empty(), "{effects:?}");
    }

    #[test]
    fn a_second_dummy_decision_after_deciding_has_no_effect() {
        let block = fast_metablock(1);
        let mut view = dummy_view(nodes()[0]);
        deliver(
            &mut view,
            nodes()[1],
            DummyViewMessage::FastInput(block.clone()),
        );
        for node in quorum() {
            deliver(&mut view, node, dummy_vote(node, &block.entries()));
        }
        assert_eq!(decisions(&drain(&mut view)).len(), 1);

        // another signer set's certificate on the same entries
        let qc = strong_qc(SLOT, DummyVote(block.entries()), &nodes()[1..]);
        deliver(
            &mut view,
            nodes()[2],
            DummyViewMessage::DummyDecision { qc, block },
        );
        assert!(drain(&mut view).is_empty());
    }

    #[test]
    fn a_decided_view_handles_no_further_message() {
        let block = fast_metablock(1);
        let mut view = dummy_view(nodes()[0]);
        deliver(
            &mut view,
            nodes()[1],
            DummyViewMessage::FastInput(block.clone()),
        );
        for node in quorum() {
            deliver(&mut view, node, dummy_vote(node, &block.entries()));
        }
        assert_eq!(decisions(&drain(&mut view)).len(), 1);

        for node in quorum() {
            deliver(&mut view, node, not_full(node));
        }
        assert!(drain(&mut view).is_empty(), "no NfCert after deciding");

        deliver(
            &mut view,
            nodes()[2],
            DummyViewMessage::FastInput(fast_metablock(2)),
        );
        assert!(drain(&mut view).is_empty());
    }

    #[test]
    fn a_dummy_decision_with_a_bad_certificate_or_a_mismatched_block_is_dropped() {
        let block = fast_metablock(1);
        let entries = block.entries();
        let qc = dummy_qc(&entries);
        let mut view = dummy_view(nodes()[0]);

        let mut forged = qc.clone();
        forged.verdict = DummyVote(fast_metablock(2).entries());
        let wide = fast_metablock_in(SLOT, NUM_LANES + 1, 1);
        let bad = [
            // another slot's certificate
            (
                strong_qc(Slot(1), DummyVote(entries), &quorum()),
                block.clone(),
            ),
            // signatures that do not cover the verdict
            (forged, fast_metablock(2)),
            // too few signers for a certificate
            (
                {
                    let mut pool = VotePool::new(SLOT);
                    for node in &quorum()[..2] {
                        let vote = VoteMsg::new_signed(SLOT, qc.verdict.clone(), &node.keypair());
                        pool.add_vote(dummy_index(node), vote);
                    }
                    let validator_data = validator_data();
                    let tally = pool.tally(&validator_data);
                    let (_, sigcol) = tally.groups().next().expect("two signers, one bucket");
                    DummyQc {
                        sigcol: sigcol.clone(),
                        ..qc.clone()
                    }
                },
                block.clone(),
            ),
            // the certified entries, but not a fast metablock
            (qc.clone(), non_fast_metablock(1)),
            // a fast metablock with other entries
            (qc.clone(), fast_metablock(2)),
            // a fast metablock that is not valid for this slot's lanes
            (dummy_qc(&wide.entries()), wide),
        ];
        for (qc, block) in bad {
            deliver(
                &mut view,
                nodes()[1],
                DummyViewMessage::DummyDecision { qc, block },
            );
        }
        let effects = drain(&mut view);
        assert!(decisions(&effects).is_empty(), "{effects:?}");

        deliver(
            &mut view,
            nodes()[1],
            DummyViewMessage::DummyDecision {
                qc: qc.clone(),
                block: block.clone(),
            },
        );
        assert_eq!(
            drain(&mut view),
            vec![DummyViewEffect::Decided { qc, block }]
        );
    }

    fn rlp_list(items: &[&dyn Encodable]) -> Vec<u8> {
        let mut out = Vec::new();
        alloy_rlp::encode_list::<_, dyn Encodable>(items, &mut out);
        out
    }

    /// Bytes already in RLP, spliced in as one element
    struct Raw(Vec<u8>);

    impl Encodable for Raw {
        fn encode(&self, out: &mut dyn bytes::BufMut) {
            out.put_slice(&self.0);
        }

        fn length(&self) -> usize {
            self.0.len()
        }
    }

    fn decodes(bytes: &[u8]) -> bool {
        alloy_rlp::decode_exact::<DummyViewMessage>(bytes).is_ok()
    }

    #[test]
    fn ill_formed_encodings_are_rejected_without_a_panic() {
        let block = fast_metablock(1);
        let entries = block.entries();
        let me = nodes()[0];
        let not_full_vote = VoteMsg::new_signed(SLOT, NotFull, &me.keypair());
        let dummy_vote_msg = VoteMsg::new_signed(SLOT, DummyVote(entries.clone()), &me.keypair());
        let well_formed = [
            DummyViewMessage::FastInput(block.clone()),
            DummyViewMessage::NotFull(not_full_vote.clone()),
            DummyViewMessage::DummyVote(dummy_vote_msg.clone()),
            DummyViewMessage::DummyDecision {
                qc: dummy_qc(&entries),
                block: block.clone(),
            },
        ];

        for msg in &well_formed {
            let bytes = alloy_rlp::encode(msg);
            assert_eq!(rlp_list(&msg.rlp_fields()), bytes);
            let decoded = alloy_rlp::decode_exact::<DummyViewMessage>(&bytes).unwrap();
            assert_eq!(&decoded, msg);
            assert_eq!(decoded.slot(), SLOT);

            // an extra element inside a consistent outer header
            let mut fields = msg.rlp_fields();
            fields.push(&0u8);
            assert!(!decodes(&rlp_list(&fields)), "trailing element: {msg:?}");
        }

        // a metablock is the bare list of its entries, so with no lanes it is `[]`
        let lanes = block.certified_entries().into_iter();
        let lanes: Vec<&dyn Encodable> = lanes.map(|e| e as &dyn Encodable).collect();
        assert_eq!(rlp_list(&lanes), alloy_rlp::encode(&block));
        let no_lanes = Raw(rlp_list(&[]));

        let qc = dummy_qc(&entries);
        let ill_formed = [
            // a decision truncated after its certificate
            rlp_list(&[&4u8, &qc]),
            // a block with no lanes
            rlp_list(&[&1u8, &no_lanes]),
            rlp_list(&[&4u8, &qc, &no_lanes]),
            // a tag alone, an empty list, and payloads that are not lists
            rlp_list(&[&1u8]),
            vec![0xc0],
            vec![0x01],
            alloy_rlp::encode(b"\x01\x02".as_slice()),
            // a tag carrying another variant's payload
            rlp_list(&[&2u8, &block]),
            rlp_list(&[&1u8, &not_full_vote]),
            rlp_list(&[&3u8, &not_full_vote]),
            rlp_list(&[&2u8, &dummy_vote_msg]),
            rlp_list(&[&4u8, &block, &qc]),
        ];
        for bytes in &ill_formed {
            assert!(!decodes(bytes), "{bytes:02x?}");
        }
    }
}
