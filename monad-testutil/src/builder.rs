use std::{borrow::Cow, marker::PhantomData};

use monad_consensus::messages::message::ProposalMessage;
use monad_consensus_types::{
    block::ConsensusBlockHeader,
    no_endorsement::{FreshProposalCertificate, NoEndorsementCertificate},
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::QuorumCertificate,
    timeout::{Timeout, TimeoutCertificate, TimeoutInfo},
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
    validator_mapping,
};

// Same as Into<T> specifically meant to be used for this module.
// TODO: maybe make this fallible to accommodate more complex extractions.
pub trait Provide<T> {
    fn extract(self) -> T;
}

impl<T> Provide<T> for T {
    fn extract(self) -> T {
        self
    }
}

// provides multiple values from a single source
macro_rules! impl_provide_for_tup {
    ( $($tt:ident),+ ) => {
        impl<'a, P, $($tt),+> Provide<($($tt),+)> for &'a P
        where
            &'a P: $(Provide<$tt> +)*,
        {
            fn extract(self) -> ($($tt),+) {
                ( $(<Self as Provide<$tt>>::extract(self)),+ )
            }
        }
    };
}

impl_provide_for_tup!(T1, T2);
impl_provide_for_tup!(T1, T2, T3);

// Derive<T, Ctx> is implemented by types that permit deriving a T
// given Ctx. It is essentially like the function type: FnOnce(&Ctx)
// -> impl Provide<T> but allows implementations by non-function-like
// types.
//
// For example, an EpochManager should implement Derive<Epoch, Round>.
pub trait Derive<T, Ctx> {
    fn derive(self, ctx: &Ctx) -> T;
}

impl<F, PT, T, Ctx> Derive<T, Ctx> for F
where
    F: FnOnce(&Ctx) -> PT,
    PT: Provide<T>,
{
    fn derive(self, ctx: &Ctx) -> T {
        self(ctx).extract()
    }
}

// An abstracted piece of information that may be absent.
// Its value of type T can be conveniently set with any Provide<T>.
pub struct InfoPiece<T>(Option<T>);

impl<T> InfoPiece<T> {
    pub fn new(value: impl Provide<T>) -> Self {
        InfoPiece(Some(value.extract()))
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

    pub fn set(&mut self, value: impl Provide<T>) {
        self.0 = Some(value.extract());
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
    fn to_digest(&self) -> impl Into<Cow<[u8]>> {
        *self
    }
}

impl<'a, ST, Domain> Signable<ST, Domain> for Cow<'a, [u8]>
where
    ST: CertificateSignature,
    Domain: SigningDomain,
{
    fn to_digest(&self) -> impl Into<Cow<[u8]>> {
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
        val_map: &ValidatorMapping<SCT>,
    ) -> Option<Self> {
        let digest = data.to_digest().into();
        let sig_col = SCT::new::<Domain>(sigs, val_map, digest.as_ref()).ok()?;
        Some(Self::new(sig_col))
    }
}

// A owned node is a tuple (node id, certificate keypair).
#[expect(unused)]
type OwnedNode<SCT> = (
    NodeId<<SCT as SignatureCollection>::NodeIdPubKey>,
    SignatureCollectionKeyPairType<SCT>,
);

// A set of owned-nodes, used to construct signature collections.
// This type is only meant to use in testing.
#[expect(unused)]
enum OwnedNodes<'a, SCT>
where
    SCT: SignatureCollection,
{
    Ref(&'a [OwnedNode<SCT>]),
    Owned(Vec<OwnedNode<SCT>>),
}

impl<SCT> AsRef<[OwnedNode<SCT>]> for OwnedNodes<'_, SCT>
where
    SCT: SignatureCollection,
{
    fn as_ref(&self) -> &[OwnedNode<SCT>] {
        match self {
            OwnedNodes::Ref(slice) => slice,
            OwnedNodes::Owned(vec) => vec.as_slice(),
        }
    }
}

impl<'a, 'b: 'a, SCT> IntoIterator for &'b OwnedNodes<'a, SCT>
where
    SCT: SignatureCollection,
{
    type Item = &'a OwnedNode<SCT>;
    type IntoIter = std::slice::Iter<'b, OwnedNode<SCT>>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_ref().iter()
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

/* The main Builder types for Block and Proposal */
pub struct BlockTemplate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // assigned
    pub author: InfoPiece<NodeId<CertificateSignaturePubKey<ST>>>,
    // assigned
    pub block_round: InfoPiece<Round>,
    // assigned or from (round, cert key)
    pub round_signature: InfoPiece<RoundSignature<SCT::SignatureType>>,
    // assigned or from (round, epoch manager)
    pub epoch: InfoPiece<Epoch>,
    // assigned or from (owned nodes, vote), vote can be substituted by a proposal
    pub qc: InfoPiece<QuorumCertificate<SCT>>,
    // assigned or from previous block, previous block can be substituted by a proposal.
    // use prev_block.seq_num + 1.
    pub seq_num: InfoPiece<SeqNum>,
    // assigned or from previous block (cfg(test)), previous block can be substituted by a proposal.
    // use prev_block.timestamp_ns + 1.
    pub timestamp_ns: InfoPiece<u128>,
    // assigned or default
    pub delayed_execution_results: InfoPiece<Vec<EPT::FinalizedHeader>>,
    // assigned or default
    pub execution_inputs: InfoPiece<EPT::ProposedHeader>,
    // assigned or default
    pub base_fee: InfoPiece<Option<u64>>,
    pub base_fee_trend: InfoPiece<Option<u64>>,
    pub base_fee_moment: InfoPiece<Option<u64>>,
    // assigned or default
    pub body: InfoPiece<ConsensusBlockBody<EPT>>,
    // assigned or auto computed
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
            base_fee_trend: InfoPiece::absent(),
            base_fee_moment: InfoPiece::absent(),
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
        let base_fee_trend = *self
            .base_fee_trend
            .as_ref()
            .expect("base_fee_trend unspecified");
        let base_fee_moment = *self
            .base_fee_moment
            .as_ref()
            .expect("base_fee_moment unspecified");
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
            base_fee,
            base_fee_trend,
            base_fee_moment,
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
    // assigned or from existing proposal
    pub block: InfoPiece<BlockTemplate<ST, SCT, EPT>>,
    // assigned or calculated
    pub block_signature: InfoPiece<Signature<ST, sd::Tip>>,
    // assigned
    pub proposal_round: InfoPiece<Round>,
    // assigned or from (round, epoch manager)
    pub proposal_epoch: InfoPiece<Epoch>,
    // absent, or assigned, or from (owned nodes, timeout), timeout can be substituted by a proposal
    pub last_round_tc: InfoPiece<Option<TimeoutCertificate<ST, SCT, EPT>>>,
    // absent, or assigned (NEC, or NoTip from TC), or from (owned nodes, no endorsement).
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
        proposal: impl Provide<(BlockTemplate<ST, SCT, EPT>, Signature<ST, sd::Tip>)>,
        tc: TimeoutCertificate<ST, SCT, EPT>,
    ) -> Self {
        let (block, sig) = proposal.extract();
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
    pub fn new(kind: impl Provide<ProposalKind<ST, SCT, EPT>>) -> Self {
        let mut base = Self::default();

        match kind.extract() {
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

impl<ST, SCT, EPT> Provide<BlockTemplate<ST, SCT, EPT>> for &ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn extract(self) -> BlockTemplate<ST, SCT, EPT> {
        let mut template = BlockTemplate::default();
        template.block_round.set(self.proposal_round);
        template.epoch.set(self.proposal_epoch);
        template.qc.set(self.tip.block_header.qc.clone());
        template.seq_num.set(self.tip.block_header.seq_num);
        template
            .timestamp_ns
            .set(self.tip.block_header.timestamp_ns);
        template
            .delayed_execution_results
            .set(self.tip.block_header.delayed_execution_results.clone());
        template
            .execution_inputs
            .set(self.tip.block_header.execution_inputs.clone());

        template.base_fee.set(self.tip.block_header.base_fee);
        template
            .base_fee_trend
            .set(self.tip.block_header.base_fee_trend);
        template
            .base_fee_moment
            .set(self.tip.block_header.base_fee_moment);
        template.body.set(self.block_body.clone());
        debug_assert!(template.is_complete());
        template
    }
}

impl<ST, SCT, EPT> Provide<Signature<ST, sd::Tip>> for &ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn extract(self) -> Signature<ST, sd::Tip> {
        Signature::new(*self.tip.signature())
    }
}

type ValidatorMapping<SCT> = validator_mapping::ValidatorMapping<
    <SCT as SignatureCollection>::NodeIdPubKey,
    SignatureCollectionKeyPairType<SCT>,
>;

// construct TC from round, epoch, [(NodeId, Timeout)] and a validator mapping
impl<ST, SCT, EPT> Provide<TimeoutCertificate<ST, SCT, EPT>>
    for (
        Round,
        Epoch,
        &[(
            NodeId<CertificateSignaturePubKey<ST>>,
            Timeout<ST, SCT, EPT>,
        )],
        &ValidatorMapping<SCT>,
    )
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn extract(self) -> TimeoutCertificate<ST, SCT, EPT> {
        let (round, epoch, timeouts, val_map) = self;
        // TODO: make Provide fallible so we can propagate errors
        TimeoutCertificate::new(epoch, round, timeouts, val_map).expect("sigcol not formed")
    }
}
