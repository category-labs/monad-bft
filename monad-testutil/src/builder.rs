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

use std::{borrow::Cow, marker::PhantomData};

use monad_consensus::messages::message::ProposalMessage;
use monad_consensus_types::{
    block::ConsensusBlockHeader,
    no_endorsement::{FreshProposalCertificate, NoEndorsementCertificate},
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::QuorumCertificate,
    timeout::{TimeoutCertificate, TimeoutInfo},
    tip::ConsensusTip,
};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain::{self as sd, SigningDomain},
};
use monad_types::{Epoch, ExecutionProtocol, NodeId, Round, SeqNum};
use monad_validator::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validator_mapping::ValidatorMapping,
};

// An abstracted piece of information that may be absent.
// Its value of type T can be conveniently set with any Into<T>.
pub struct InfoPiece<T>(Option<T>);

impl<T> InfoPiece<T> {
    pub fn new(value: impl Into<T>) -> Self {
        InfoPiece(Some(value.into()))
    }

    pub fn from_default() -> Self
    where
        T: Default,
    {
        InfoPiece(Some(T::default()))
    }

    pub fn absent() -> Self {
        InfoPiece(None)
    }

    pub fn set(&mut self, value: impl Into<T>) {
        self.0 = Some(value.into());
    }

    pub fn is_set(&self) -> bool {
        self.0.is_some()
    }

    pub fn as_ref(&self) -> Option<&T> {
        self.0.as_ref()
    }

    pub fn as_mut(&mut self) -> Option<&mut T> {
        self.0.as_mut()
    }

    pub fn into_inner(self) -> Option<T> {
        self.0
    }
}

impl<T> Default for InfoPiece<T> {
    fn default() -> Self {
        InfoPiece(None)
    }
}

/* Signing related traits and concrete types */
#[derive(Clone)]
pub struct KeyPair<'a, ST>(&'a ST::KeyPairType)
where
    ST: CertificateSignature;

impl<'a, ST> KeyPair<'a, ST>
where
    ST: CertificateSignature,
{
    pub fn from_ref(keypair: &'a ST::KeyPairType) -> Self {
        KeyPair(keypair)
    }
}

impl<ST> AsRef<ST::KeyPairType> for KeyPair<'_, ST>
where
    ST: CertificateSignature,
{
    fn as_ref(&self) -> &ST::KeyPairType {
        self.0
    }
}

impl<'a, ST> From<&'a ST::KeyPairType> for KeyPair<'a, ST>
where
    ST: CertificateSignature,
{
    fn from(keypair: &'a ST::KeyPairType) -> Self {
        KeyPair::from_ref(keypair)
    }
}

// The trait for data that can be signed for a particular signing domain.
// The minimal implementation requires providing a digest method.
pub trait Signable<ST, Domain>
where
    ST: CertificateSignature,
    Domain: SigningDomain,
{
    // digest bytes may be referenced or owned for convenience
    fn to_digest(&self) -> impl Into<Cow<[u8]>>;

    fn sign_with<'a>(&self, key: impl Into<KeyPair<'a, ST>>) -> Signature<ST, Domain> {
        let digest = self.to_digest().into();
        let key = key.into();
        let sig = ST::sign::<Domain>(digest.as_ref(), key.as_ref());
        Signature::new(sig)
    }
}

impl<ST, Domain> Signable<ST, Domain> for &[u8]
where
    ST: CertificateSignature,
    Domain: SigningDomain,
{
    fn to_digest(&self) -> impl Into<Cow<'_, [u8]>> {
        *self
    }
}

impl<'a, ST, Domain> Signable<ST, Domain> for Cow<'a, [u8]>
where
    ST: CertificateSignature,
    Domain: SigningDomain,
{
    fn to_digest(&self) -> impl Into<Cow<'_, [u8]>> {
        Cow::Borrowed(self.as_ref())
    }
}

// A wrapper around a signature type that serves multiple purposes:
//
// - Being a concrete type than generic ST, it's easy to use in traits
//   without triggering overlapping trait implementations.
// - Tying the signature to a specific signing domain for compile-time
//   domain validation.
// - Provide convenient methods for signing.
pub struct Signature<ST, Domain>(ST, PhantomData<Domain>);

impl<ST, Domain> Clone for Signature<ST, Domain>
where
    ST: Clone,
{
    fn clone(&self) -> Self {
        Signature(self.0.clone(), PhantomData)
    }
}

impl<ST, Domain> Signature<ST, Domain> {
    pub fn new(sig: ST) -> Self {
        Signature(sig, PhantomData)
    }

    pub fn from_data<'a>(data: &impl Signable<ST, Domain>, key: impl Into<KeyPair<'a, ST>>) -> Self
    where
        ST: CertificateSignature,
        Domain: SigningDomain,
    {
        data.sign_with(key)
    }

    pub fn into_inner(self) -> ST {
        self.0
    }
}

struct SigCol<SCT, Domain>(SCT, PhantomData<Domain>)
where
    SCT: SignatureCollection,
    Domain: SigningDomain;

impl<SCT, Domain> SigCol<SCT, Domain>
where
    SCT: SignatureCollection,
    Domain: SigningDomain,
{
    fn new(sig_col: SCT) -> Self {
        SigCol(sig_col, PhantomData)
    }

    fn from_sigs(
        data: &impl Signable<SCT::SignatureType, Domain>,
        sigs: impl IntoIterator<Item = (NodeId<SCT::NodeIdPubKey>, SCT::SignatureType)>,
        val_map: &ValidatorMapping<
            SCT::NodeIdPubKey,
            <SCT::SignatureType as CertificateSignature>::KeyPairType,
        >,
    ) -> Option<Self> {
        let digest = data.to_digest().into();
        let sig_col = SCT::new::<Domain>(sigs, val_map, digest.as_ref()).ok()?;
        Some(Self::new(sig_col))
    }
}

/* Application Signable Types */
impl<ST, SCT, EPT> Signable<ST, sd::Tip> for ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn to_digest(&self) -> impl Into<Cow<[u8]>> {
        alloy_rlp::encode(self)
    }
}

impl<ST> Signable<ST, sd::RoundSignature> for Round
where
    ST: CertificateSignature,
{
    fn to_digest(&self) -> impl Into<Cow<[u8]>> {
        alloy_rlp::encode(self)
    }
}

impl<ST> Signable<ST, sd::Timeout> for TimeoutInfo
where
    ST: CertificateSignature,
{
    fn to_digest(&self) -> impl Into<Cow<[u8]>> {
        alloy_rlp::encode(self)
    }
}

impl<ST> Signable<ST, sd::Vote> for monad_consensus_types::voting::Vote
where
    ST: CertificateSignatureRecoverable,
{
    fn to_digest(&self) -> impl Into<Cow<[u8]>> {
        alloy_rlp::encode(self)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BaseFee {
    base_fee: u64,
    base_fee_trend: u64,
    base_fee_moment: u64,
}

impl BaseFee {
    pub fn genesis() -> Self {
        use monad_tfm::base_fee::{
            GENESIS_BASE_FEE, GENESIS_BASE_FEE_MOMENT, GENESIS_BASE_FEE_TREND,
        };

        Self {
            base_fee: (GENESIS_BASE_FEE),
            base_fee_trend: (GENESIS_BASE_FEE_TREND),
            base_fee_moment: (GENESIS_BASE_FEE_MOMENT),
        }
    }

    pub fn next(&self, gas_limit: u64, gas_usage: u64) -> Self {
        let Self {
            base_fee,
            base_fee_trend,
            base_fee_moment,
        } = *self;

        let (next_base_fee, next_base_fee_trend, next_base_fee_moment) =
            monad_tfm::base_fee::compute_base_fee(
                gas_limit,
                gas_usage,
                base_fee,
                base_fee_trend,
                base_fee_moment,
            );

        Self {
            base_fee: next_base_fee,
            base_fee_trend: next_base_fee_trend,
            base_fee_moment: next_base_fee_moment,
        }
    }
}

impl From<(u64, u64, u64)> for BaseFee {
    fn from(value: (u64, u64, u64)) -> BaseFee {
        BaseFee {
            base_fee: value.0,
            base_fee_trend: value.1,
            base_fee_moment: value.2,
        }
    }
}

impl<ST, SCT, EPT> From<&ConsensusBlockHeader<ST, SCT, EPT>> for BaseFee
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(header: &ConsensusBlockHeader<ST, SCT, EPT>) -> BaseFee {
        BaseFee {
            base_fee: header.base_fee.unwrap(),
            base_fee_trend: header.base_fee_trend.unwrap(),
            base_fee_moment: header.base_fee_moment.unwrap(),
        }
    }
}

/* The main Builder types for Block and Proposal */
pub struct BlockTemplate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub author: InfoPiece<NodeId<CertificateSignaturePubKey<ST>>>,
    pub block_round: InfoPiece<Round>,
    pub round_signature: InfoPiece<RoundSignature<SCT::SignatureType>>,
    pub epoch: InfoPiece<Epoch>,
    pub qc: InfoPiece<QuorumCertificate<SCT>>,
    pub seq_num: InfoPiece<SeqNum>,
    pub timestamp_ns: InfoPiece<u128>,
    pub delayed_execution_results: InfoPiece<Vec<EPT::FinalizedHeader>>,
    pub execution_inputs: InfoPiece<EPT::ProposedHeader>,
    pub base_fee: InfoPiece<BaseFee>,
    pub body: InfoPiece<ConsensusBlockBody<EPT>>,
    pub block_body_id: InfoPiece<ConsensusBlockBodyId>,
}

impl<ST, SCT, EPT> Default for BlockTemplate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn default() -> Self {
        Self {
            author: InfoPiece::absent(),
            block_round: InfoPiece::absent(),
            round_signature: InfoPiece::absent(),
            epoch: InfoPiece::absent(),
            qc: InfoPiece::absent(),
            seq_num: InfoPiece::absent(),
            timestamp_ns: InfoPiece::absent(),
            delayed_execution_results: InfoPiece::absent(),
            execution_inputs: InfoPiece::absent(),
            base_fee: InfoPiece::absent(),
            body: InfoPiece::absent(),
            block_body_id: InfoPiece::absent(),
        }
    }
}

impl<ST, SCT, EPT> BlockTemplate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn from_last_qc(qc: QuorumCertificate<SCT>) -> Self {
        Self {
            qc: InfoPiece::new(qc),
            ..Self::default()
        }
    }

    pub fn is_header_complete(&self) -> bool {
        self.author.is_set()
            && self.block_round.is_set()
            && self.epoch.is_set()
            && self.qc.is_set()
            && self.seq_num.is_set()
            && self.timestamp_ns.is_set()
            && self.delayed_execution_results.is_set()
            && self.execution_inputs.is_set()
            && self.base_fee.is_set()
            && (self.body.is_set() || self.block_body_id.is_set())
    }
    pub fn is_complete(&self) -> bool {
        self.is_header_complete() && self.body.is_set()
    }

    pub fn sign_round<'a>(&mut self, key: impl Into<KeyPair<'a, SCT::SignatureType>>) {
        let round = self
            .block_round
            .as_ref()
            .expect("block round not specified");
        let key = key.into();
        let sig = RoundSignature::new(*round, key.as_ref());
        self.round_signature.set(sig);
    }

    pub fn sign<'a>(&self, key: impl Into<KeyPair<'a, ST>>) -> Option<Signature<ST, sd::Tip>>
    where
        ConsensusBlockHeader<ST, SCT, EPT>: Signable<ST, sd::Tip>,
    {
        let header = self.make_header()?;
        let sig = Signature::from_data(&header, key);
        Some(sig)
    }

    pub fn make_header(&self) -> Option<ConsensusBlockHeader<ST, SCT, EPT>> {
        if !self.is_header_complete() {
            return None;
        }

        let author = *self.author.as_ref().expect("author unspecified");
        let block_round = *self.block_round.as_ref().expect("block_round unspecified");
        let round_signature = self
            .round_signature
            .as_ref()
            .expect("round_signature unspecified")
            .clone();
        let epoch = *self.epoch.as_ref().expect("epoch unspecified");
        let qc = self.qc.as_ref().expect("qc unspecified").clone();
        let seq_num = *self.seq_num.as_ref().expect("seq_num unspecified");
        let timestamp_ns = *self
            .timestamp_ns
            .as_ref()
            .expect("timestamp_ns unspecified");
        let delayed_execution_results = self
            .delayed_execution_results
            .as_ref()
            .expect("delayed exec results unspecified")
            .clone();
        let execution_inputs = self
            .execution_inputs
            .as_ref()
            .expect("exec inputs unspecified")
            .clone();
        let base_fee = *self.base_fee.as_ref().expect("base_fee unspecified");
        let block_body_id = match self.block_body_id.as_ref() {
            Some(id) => *id,
            None => self
                .body
                .as_ref()
                .expect("block_body_id unspecified")
                .get_id(),
        };

        let block_header = ConsensusBlockHeader {
            author,
            block_round,
            round_signature,
            epoch,
            qc,
            seq_num,
            timestamp_ns,
            delayed_execution_results,
            execution_inputs,
            // can these fields be None?
            base_fee: Some(base_fee.base_fee),
            base_fee_trend: Some(base_fee.base_fee_trend),
            base_fee_moment: Some(base_fee.base_fee_moment),
            block_body_id,
        };

        Some(block_header)
    }

    pub fn make(&self) -> Option<(ConsensusBlockHeader<ST, SCT, EPT>, ConsensusBlockBody<EPT>)> {
        if !self.is_complete() {
            return None;
        }
        let header = self.make_header()?;
        let body = self.body.as_ref()?.clone();
        Some((header, body))
    }
}

pub struct BlockSignature<ST>(pub ST)
where
    ST: CertificateSignature;

pub struct ProposalTemplate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub block: InfoPiece<BlockTemplate<ST, SCT, EPT>>,
    pub block_signature: InfoPiece<Signature<ST, sd::Tip>>,
    pub proposal_round: InfoPiece<Round>,
    pub proposal_epoch: InfoPiece<Epoch>,
    pub last_round_tc: InfoPiece<Option<TimeoutCertificate<ST, SCT, EPT>>>,
    pub fresh_proposal_cert: InfoPiece<Option<FreshProposalCertificate<SCT>>>,
}

pub enum ProposalKind<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Reproposal(
        BlockTemplate<ST, SCT, EPT>,
        Signature<ST, sd::Tip>,
        TimeoutCertificate<ST, SCT, EPT>,
    ),
    FreshFromQC(QuorumCertificate<SCT>),
    FreshFromFPC(
        TimeoutCertificate<ST, SCT, EPT>,
        FreshProposalCertificate<SCT>,
    ),

    // used to construct a custom proposal with arbitrary components
    #[cfg(test)]
    Custom(
        Option<TimeoutCertificate<ST, SCT, EPT>>,
        Option<FreshProposalCertificate<SCT>>,
    ),
}

impl<ST, SCT, EPT> ProposalKind<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn from_reproposal(
        proposal: impl Into<(BlockTemplate<ST, SCT, EPT>, Signature<ST, sd::Tip>)>,
        tc: TimeoutCertificate<ST, SCT, EPT>,
    ) -> Self {
        let (block, sig) = proposal.into();
        ProposalKind::Reproposal(block, sig, tc)
    }

    pub fn from_qc(qc: QuorumCertificate<SCT>) -> Self {
        ProposalKind::FreshFromQC(qc)
    }

    pub fn from_no_tip(tc: TimeoutCertificate<ST, SCT, EPT>) -> Self {
        assert!(tc.has_no_tip(), "tc must have no tip");
        let ntc = tc
            .clone()
            .try_into_no_tip_certificate()
            .expect("tc has no tip");
        let fpc = FreshProposalCertificate::NoTip(ntc);
        ProposalKind::FreshFromFPC(tc, fpc)
    }

    pub fn from_nec(
        tc: TimeoutCertificate<ST, SCT, EPT>,
        nec: NoEndorsementCertificate<SCT>,
    ) -> Self {
        let fpc = FreshProposalCertificate::Nec(nec);
        ProposalKind::FreshFromFPC(tc, fpc)
    }

    #[cfg(test)]
    pub fn custom(
        maybe_tc: Option<TimeoutCertificate<ST, SCT, EPT>>,
        maybe_fpc: Option<FreshProposalCertificate<SCT>>,
    ) -> Self {
        ProposalKind::Custom(maybe_tc, maybe_fpc)
    }
}

impl<ST, SCT, EPT> Default for ProposalTemplate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn default() -> Self {
        Self {
            block: InfoPiece::absent(),
            block_signature: InfoPiece::absent(),
            proposal_round: InfoPiece::absent(),
            proposal_epoch: InfoPiece::absent(),
            last_round_tc: InfoPiece::absent(),
            fresh_proposal_cert: InfoPiece::absent(),
        }
    }
}

impl<ST, SCT, EPT> ProposalTemplate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(kind: impl Into<ProposalKind<ST, SCT, EPT>>) -> Self {
        let mut base = Self::default();

        match kind.into() {
            ProposalKind::Reproposal(block, sig, tc) => {
                base.fresh_proposal_cert.set(None);
                base.last_round_tc.set(Some(tc));
                base.block.set(block);
                base.block_signature.set(sig);
            }
            ProposalKind::FreshFromQC(qc) => {
                base.fresh_proposal_cert.set(None);
                base.last_round_tc.set(None);
                base.block.set(BlockTemplate::from_last_qc(qc));
            }
            ProposalKind::FreshFromFPC(tc, fpc) => {
                base.fresh_proposal_cert.set(Some(fpc));
                base.last_round_tc.set(Some(tc));
                base.block.set(BlockTemplate::default());
            }
            #[cfg(test)]
            ProposalKind::Custom(maybe_tc, maybe_fpc) => {
                base.fresh_proposal_cert.set(maybe_fpc);
                base.last_round_tc.set(maybe_tc);
                base.block.set(BlockTemplate::default());
            }
        }

        base
    }

    pub fn sign_block<'a>(&mut self, keypair: impl Into<KeyPair<'a, ST>>) -> Option<()> {
        let block = self.block.as_ref()?;
        let sig = block.sign(keypair)?;
        self.block_signature.set(sig);
        Some(())
    }

    pub fn is_complete(&self) -> bool {
        self.proposal_round.is_set()
            && self.proposal_epoch.is_set()
            && self.last_round_tc.is_set()
            && self.fresh_proposal_cert.is_set()
            && self.block_signature.is_set()
            && self.block.is_set()
            && self.block.as_ref().expect("block not set").is_complete()
    }

    pub fn make(&self) -> Option<ProposalMessage<ST, SCT, EPT>> {
        if !self.is_complete() {
            return None;
        }

        let block = self.block.as_ref().expect("block not set");
        let (block_header, block_body) = block.make()?;
        let proposal_round = *self
            .proposal_round
            .as_ref()
            .expect("proposal_round not set");
        let proposal_epoch = *self
            .proposal_epoch
            .as_ref()
            .expect("proposal_epoch not set");
        let last_round_tc = self
            .last_round_tc
            .as_ref()
            .expect("last_round_tc not set")
            .clone();
        let fresh_proposal_cert = self
            .fresh_proposal_cert
            .as_ref()
            .expect("fresh_proposal_cert not set")
            .clone();
        let block_signature = self
            .block_signature
            .as_ref()
            .expect("block_signature not set")
            .clone()
            .into_inner();

        let tip = ConsensusTip::from_raw(block_header, block_signature, fresh_proposal_cert);

        let proposal = ProposalMessage {
            proposal_round,
            proposal_epoch,
            tip,
            block_body,
            last_round_tc,
        };
        Some(proposal)
    }
}

impl<ST, SCT, EPT> From<&ProposalMessage<ST, SCT, EPT>> for BlockTemplate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(proposal: &ProposalMessage<ST, SCT, EPT>) -> BlockTemplate<ST, SCT, EPT> {
        let mut template = BlockTemplate::default();
        template.block_round.set(proposal.proposal_round);
        template.epoch.set(proposal.proposal_epoch);
        template.qc.set(proposal.tip.block_header.qc.clone());
        template.seq_num.set(proposal.tip.block_header.seq_num);
        template
            .timestamp_ns
            .set(proposal.tip.block_header.timestamp_ns);
        template
            .delayed_execution_results
            .set(proposal.tip.block_header.delayed_execution_results.clone());
        template
            .execution_inputs
            .set(proposal.tip.block_header.execution_inputs.clone());

        template.base_fee.set(&proposal.tip.block_header);
        template.body.set(proposal.block_body.clone());
        debug_assert!(template.is_complete());
        template
    }
}

impl<ST, SCT, EPT> From<&ProposalMessage<ST, SCT, EPT>> for Signature<ST, sd::Tip>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(proposal: &ProposalMessage<ST, SCT, EPT>) -> Signature<ST, sd::Tip> {
        Signature::new(*proposal.tip.signature())
    }
}
