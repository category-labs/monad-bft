use std::{collections::BTreeMap, ops::Deref};

use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use monad_consensus_types::{
    no_endorsement::{FreshProposalCertificate, NoEndorsementCertificate, NoEndorsementMessage},
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighExtend, NoTipCertificate, TimeoutCertificate, TimeoutInfo},
    tip::ConsensusTip,
    validation::Error,
    voting::ValidatorMapping,
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    signing_domain,
};
use monad_types::{Epoch, ExecutionProtocol, NodeId, Round, Stake, GENESIS_ROUND};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::messages::{
    consensus_message::{ConsensusMessage, ProtocolMessage},
    message::{ProposalMessage, RoundRecoveryMessage, TimeoutMessage, VoteMessage},
};

/// A verified message carries a valid signature created by the author. It's
/// created by
/// 1. Successfully verifying the signature on a
///    [crate::validation::signing::Unverified] message
/// 2. Signing a message [crate::validation::signing::Verified::new]
///
/// For protocol level accountability and slashing, the message content is
/// accessible in Verified, but not in Unverified. For similar reasons,
/// [monad_types::Serializable] is only implemented for Verified.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Verified<S: CertificateSignatureRecoverable, M> {
    author: NodeId<CertificateSignaturePubKey<S>>,
    message: Unverified<S, M>,
}

impl<S: CertificateSignatureRecoverable, M> Verified<S, M> {
    /// Get the author NodeId
    pub fn author(&self) -> &NodeId<CertificateSignaturePubKey<S>> {
        &self.author
    }

    /// Get the author signature
    pub fn author_signature(&self) -> &S {
        self.message.author_signature()
    }
}

impl<S: CertificateSignatureRecoverable, M: Encodable> Verified<S, M> {
    pub fn new(msg: M, keypair: &S::KeyPairType) -> Self {
        let rlp_msg = alloy_rlp::encode(&msg);
        let signature = S::sign::<signing_domain::ConsensusMessage>(&rlp_msg, keypair);
        Self {
            author: NodeId::new(keypair.pubkey()),
            message: Unverified::new(msg, signature),
        }
    }

    /// Consumes the struct, destructuring it into a triplet `(author,
    /// author_signature, message)`
    pub fn destructure(self) -> (NodeId<CertificateSignaturePubKey<S>>, S, M) {
        (self.author, self.message.author_signature, self.message.obj)
    }
}

impl<S: CertificateSignatureRecoverable, M> Deref for Verified<S, M> {
    type Target = M;

    /// Dereferencing returns a reference of the message
    fn deref(&self) -> &Self::Target {
        &self.message.obj
    }
}

impl<S: CertificateSignatureRecoverable, M> AsRef<Unverified<S, M>> for Verified<S, M> {
    fn as_ref(&self) -> &Unverified<S, M> {
        &self.message
    }
}

/// An unverified message is a message with a signature, but the signature hasn't
/// been verified. It does not allow access the message content. For safety, a
/// message received on the wire is only deserializable to an unverified message
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Unverified<S, M> {
    obj: M,
    author_signature: S,
}

impl<S: CertificateSignatureRecoverable, M> Unverified<S, M> {
    pub fn new(obj: M, signature: S) -> Self {
        Self {
            obj,
            author_signature: signature,
        }
    }

    /// Get the author signature
    pub fn author_signature(&self) -> &S {
        &self.author_signature
    }
}

impl<ST, SCT, EPT> Unverified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn is_proposal(&self) -> bool {
        matches!(&self.obj.obj.message, ProtocolMessage::Proposal(_))
    }
}

impl<S: CertificateSignatureRecoverable, M> Unverified<S, Unvalidated<M>> {
    /// Test only getter. Never use in production
    /// Returns underlying object bypassing the access control rule
    #[deprecated(note = "Never use in production. Only access message after verifying")]
    pub fn get_obj_unsafe(&self) -> &M {
        &self.obj.obj
    }
}

impl<S: CertificateSignatureRecoverable, M> From<Verified<S, M>> for Unverified<S, M> {
    fn from(verified: Verified<S, M>) -> Self {
        verified.message
    }
}

impl<S: CertificateSignatureRecoverable, M> From<Verified<S, Validated<M>>>
    for Unverified<S, Unvalidated<M>>
{
    fn from(value: Verified<S, Validated<M>>) -> Self {
        let validated = value.message.obj;
        Unverified {
            obj: validated.into(),
            author_signature: value.message.author_signature,
        }
    }
}

impl<ST, SCT, EPT> Unverified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn verify<VTF, VT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        sender: &SCT::NodeIdPubKey,
    ) -> Result<Verified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        // If the node is lagging too far behind, it wouldn't know when the
        // next epoch is starting. The epoch retrieved here may be incorrect.
        // TODO: Need to check that case. Should trigger statesync.
        let epoch = epoch_manager
            .get_epoch(self.obj.obj.get_round())
            .ok_or(Error::InvalidEpoch)?;
        let validator_set = val_epoch_map
            .get_val_set(&epoch)
            .ok_or(Error::ValidatorSetDataUnavailable)?;

        let msg = alloy_rlp::encode(&self.obj.obj);
        let author = self
            .author_signature
            .recover_pubkey::<signing_domain::ConsensusMessage>(&msg)
            .map_err(|_| Error::InvalidSignature)?
            .valid_pubkey(validator_set.get_members())?;
        if sender != &author {
            return Err(Error::AuthorNotSender);
        }

        let result = Verified {
            author: NodeId::new(author),
            message: self,
        };

        Ok(result)
    }
}

// FIXME-2: move Validated/Unvalidated out of monad-consensus
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Validated<M> {
    message: Unvalidated<M>,
}

impl<M> Validated<M> {
    pub fn new(msg: M) -> Self {
        Self {
            message: Unvalidated::new(msg),
        }
    }

    pub fn into_inner(self) -> M {
        self.message.obj
    }
}

impl<M> Deref for Validated<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.message.obj
    }
}

impl<M> AsRef<Unvalidated<M>> for Validated<M> {
    fn as_ref(&self) -> &Unvalidated<M> {
        &self.message
    }
}

impl<M> Encodable for Validated<M>
where
    M: Encodable,
{
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        self.message.encode(out)
    }

    fn length(&self) -> usize {
        self.message.length()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unvalidated<M> {
    obj: M,
}

impl<M> Unvalidated<M> {
    pub fn new(obj: M) -> Self {
        Self { obj }
    }
}

impl<M: Encodable> Encodable for Unvalidated<M> {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        self.obj.encode(out)
    }

    fn length(&self) -> usize {
        self.obj.length()
    }
}

impl<M: Decodable> Decodable for Unvalidated<M> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let obj = M::decode(buf)?;
        Ok(Self { obj })
    }
}

impl<M> From<Validated<M>> for Unvalidated<M> {
    fn from(value: Validated<M>) -> Self {
        value.message
    }
}

impl<ST, SCT, EPT> Unvalidated<ConsensusMessage<ST, SCT, EPT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn validate<VTF, VT, LT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        version: u32,
    ) -> Result<Validated<ProtocolMessage<ST, SCT, EPT>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        if self.obj.version != version {
            return Err(Error::InvalidVersion);
        }

        Ok(match self.obj.message {
            ProtocolMessage::Proposal(m) => {
                let validated =
                    Unvalidated::new(m).validate(epoch_manager, val_epoch_map, election)?;
                Validated {
                    message: Unvalidated::new(ProtocolMessage::Proposal(validated.into_inner())),
                }
            }
            ProtocolMessage::Vote(m) => {
                let validated = Unvalidated::new(m).validate(epoch_manager)?;
                Validated {
                    message: Unvalidated::new(ProtocolMessage::Vote(validated.into_inner())),
                }
            }
            ProtocolMessage::Timeout(m) => {
                let validated =
                    Unvalidated::new(m).validate(epoch_manager, val_epoch_map, election)?;
                Validated {
                    message: Unvalidated::new(ProtocolMessage::Timeout(validated.into_inner())),
                }
            }
            ProtocolMessage::RoundRecovery(m) => {
                let validated =
                    Unvalidated::new(m).validate(epoch_manager, val_epoch_map, election)?;
                Validated {
                    message: Unvalidated::new(ProtocolMessage::RoundRecovery(
                        validated.into_inner(),
                    )),
                }
            }
            ProtocolMessage::NoEndorsement(m) => {
                let validated = Unvalidated::new(m).validate(epoch_manager)?;
                Validated {
                    message: Unvalidated::new(ProtocolMessage::NoEndorsement(
                        validated.into_inner(),
                    )),
                }
            }
        })
    }
}

impl<ST, SCT, EPT> Unvalidated<ProposalMessage<ST, SCT, EPT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // A verified proposal is one which is well-formed, has valid signatures for
    // the present TC or QC, and epoch number is consistent with local records
    // for block.round
    pub fn validate<VTF, VT, LT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
    ) -> Result<Validated<ProposalMessage<ST, SCT, EPT>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        if let Some(tc) = &self.obj.last_round_tc {
            verify_tc(
                &|epoch, round| {
                    epoch_to_validators(epoch_manager, val_epoch_map, election, epoch, round)
                },
                tc,
            )?;
            // last_round_tc must be from the previous round
            if self.obj.proposal_round != tc.round + Round(1) {
                return Err(Error::NotWellFormed);
            }
        }

        verify_tip(
            &|epoch, round| {
                epoch_to_validators(epoch_manager, val_epoch_map, election, epoch, round)
            },
            &self.obj.tip,
        )?;

        self.well_formed_proposal()?;
        self.verify_epoch(epoch_manager)?;

        Ok(Validated { message: self })
    }

    /// A well-formed proposal
    /// 1. carries a QC/TC from r-1, proving that the block is proposed on a
    ///    valid round
    fn well_formed_proposal(&self) -> Result<(), Error> {
        if self.obj.tip.block_header.block_body_id != self.obj.block_body.get_id() {
            return Err(Error::NotWellFormed);
        }

        if self.obj.proposal_round == self.obj.tip.block_header.qc.get_round() + Round(1) {
            // Consecutive QC

            // last_round_tc is unnecessary if the QC is consecutive
            // regardless, we allow for it to exist
            return Ok(());
        }
        // last_round_tc must exist
        let Some(tc) = &self.obj.last_round_tc else {
            return Err(Error::NotWellFormed);
        };

        // check that nec.tip corresponds to last_round_tc.high_tip
        if let Some(FreshProposalCertificate::Nec(nec)) = &self.obj.tip.fresh_certificate {
            let HighExtend::Tip(tip) = &tc.high_extend else {
                return Err(Error::NotWellFormed);
            };
            if tip.block_header.get_id() != nec.msg.tip {
                return Err(Error::NotWellFormed);
            }
        }

        Ok(())
    }

    /// Check local epoch manager record for block.round is equal to block.epoch
    fn verify_epoch(&self, epoch_manager: &EpochManager) -> Result<(), Error> {
        match epoch_manager.get_epoch(self.obj.proposal_round) {
            Some(epoch) if self.obj.proposal_epoch == epoch => Ok(()),
            _ => Err(Error::InvalidEpoch),
        }?;

        Ok(())
    }
}

impl<SCT: SignatureCollection> Unvalidated<VoteMessage<SCT>> {
    /// Verifying
    /// [crate::messages::message::VoteMessage::sig] is deferred until a
    /// signature collection is optimistically aggregated because verifying a
    /// BLS signature is expensive. Verifying every signature on VoteMessage is
    /// infeasible with the block time constraint.
    pub fn validate(
        self,
        epoch_manager: &EpochManager,
    ) -> Result<Validated<VoteMessage<SCT>>, Error> {
        self.verify_epoch(epoch_manager)?;

        Ok(Validated { message: self })
    }

    /// Check local epoch manager record for vote.round is equal to vote.epoch
    fn verify_epoch(&self, epoch_manager: &EpochManager) -> Result<(), Error> {
        match epoch_manager.get_epoch(self.obj.vote.round) {
            Some(epoch) if self.obj.vote.epoch == epoch => Ok(()),
            _ => Err(Error::InvalidEpoch),
        }
    }
}

impl<ST, SCT, EPT> Unvalidated<TimeoutMessage<ST, SCT, EPT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// A valid timeout message is well-formed, and carries valid QC/TC
    pub fn validate<VTF, VT, LT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
    ) -> Result<Validated<TimeoutMessage<ST, SCT, EPT>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
        LT: LeaderElection<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        if let Some(tc) = &self.obj.last_round_tc {
            verify_tc(
                &|epoch, round| {
                    epoch_to_validators(epoch_manager, val_epoch_map, election, epoch, round)
                },
                tc,
            )?;
            // last_round_tc must be from the previous round
            if self.obj.tminfo.round != tc.round + Round(1) {
                return Err(Error::NotWellFormed);
            }
        }
        verify_high_extend(
            &|epoch, round| {
                epoch_to_validators(epoch_manager, val_epoch_map, election, epoch, round)
            },
            &self.obj.high_extend.clone().into(),
        )?;

        self.well_formed_timeout()?;
        self.verify_epoch(epoch_manager)?;

        Ok(Validated { message: self })
    }

    /// A well-formed timeout carries a QC/TC from r-1, proving that it's timing
    /// out a valid round
    fn well_formed_timeout(&self) -> Result<(), Error> {
        if self.obj.tminfo.round == self.obj.high_extend.qc().get_round() + Round(1) {
            // Consecutive QC

            // last_round_tc is unnecessary if the QC is consecutive
            // regardless, we allow for it to exist
            return Ok(());
        }
        // last_round_tc must exist
        let Some(_tc) = &self.obj.last_round_tc else {
            return Err(Error::NotWellFormed);
        };
        Ok(())
    }

    /// Check local epoch manager record for timeout.round is equal to timeout.epoch
    fn verify_epoch(&self, epoch_manager: &EpochManager) -> Result<(), Error> {
        match epoch_manager.get_epoch(self.obj.tminfo.round) {
            Some(epoch) if self.obj.tminfo.epoch == epoch => Ok(()),
            _ => Err(Error::InvalidEpoch),
        }
    }
}

impl<ST, SCT, EPT> Unvalidated<RoundRecoveryMessage<ST, SCT, EPT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// A valid timeout message is well-formed, and carries valid QC/TC
    pub fn validate<VTF, VT, LT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
    ) -> Result<Validated<RoundRecoveryMessage<ST, SCT, EPT>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
        LT: LeaderElection<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        verify_tc(
            &|epoch, round| {
                epoch_to_validators(epoch_manager, val_epoch_map, election, epoch, round)
            },
            &self.obj.tc,
        )?;

        self.well_formed_round_recovery()?;
        self.verify_epoch(epoch_manager)?;

        Ok(Validated { message: self })
    }

    fn well_formed_round_recovery(&self) -> Result<(), Error> {
        if self.obj.round != self.obj.tc.round + Round(1) {
            return Err(Error::InvalidTcRound);
        }
        match &self.obj.tc.high_extend {
            HighExtend::Qc(_) => return Err(Error::NotWellFormed),
            HighExtend::Tip(_) => {}
        };

        Ok(())
    }

    /// Check local epoch manager record for timeout.round is equal to timeout.epoch
    fn verify_epoch(&self, epoch_manager: &EpochManager) -> Result<(), Error> {
        match epoch_manager.get_epoch(self.obj.round) {
            Some(epoch) if self.obj.epoch == epoch => Ok(()),
            _ => Err(Error::InvalidEpoch),
        }
    }
}

impl<SCT> Unvalidated<NoEndorsementMessage<SCT>>
where
    SCT: SignatureCollection,
{
    /// A valid timeout message is well-formed, and carries valid QC/TC
    pub fn validate(
        self,
        epoch_manager: &EpochManager,
    ) -> Result<Validated<NoEndorsementMessage<SCT>>, Error> {
        self.well_formed_no_endorsement()?;
        self.verify_epoch(epoch_manager)?;

        Ok(Validated { message: self })
    }

    fn well_formed_no_endorsement(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Check local epoch manager record for timeout.round is equal to timeout.epoch
    fn verify_epoch(&self, epoch_manager: &EpochManager) -> Result<(), Error> {
        match epoch_manager.get_epoch(self.obj.msg.round) {
            Some(epoch) if self.obj.msg.epoch == epoch => Ok(()),
            _ => Err(Error::InvalidEpoch),
        }
    }
}

fn epoch_to_validators<'a, SCT, VTF, VT, LT>(
    epoch_manager: &'a EpochManager,
    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    election: &LT,
    epoch: Epoch,
    round: Round,
) -> Result<
    (
        &'a VT,
        &'a ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
        NodeId<SCT::NodeIdPubKey>,
    ),
    Error,
>
where
    SCT: SignatureCollection,
    VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    LT: LeaderElection<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    if epoch != epoch_manager.get_epoch(round).ok_or(Error::InvalidEpoch)? {
        return Err(Error::InvalidEpoch);
    }
    let validator_set = val_epoch_map
        .get_val_set(&epoch)
        .ok_or(Error::ValidatorSetDataUnavailable)?;
    let validator_cert_pubkeys = val_epoch_map
        .get_cert_pubkeys(&epoch)
        .ok_or(Error::ValidatorSetDataUnavailable)?;

    let leader = election.get_leader(round, validator_set.get_members());

    Ok((validator_set, validator_cert_pubkeys, leader))
}

/// Verify the timeout certificate
///
/// The signature collections are created over `Hash(tc.round, high_qc.round)`
///
/// See [monad_consensus_types::timeout::TimeoutInfo::timeout_digest]
pub fn verify_tc<'a, ST, SCT, EPT, VT>(
    epoch_to_validators: &impl Fn(
        Epoch,
        Round,
    ) -> Result<
        (
            &'a VT,
            &'a ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
            NodeId<SCT::NodeIdPubKey>,
        ),
        Error,
    >,
    tc: &TimeoutCertificate<ST, SCT, EPT>,
) -> Result<(), Error>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    let (validators, validator_mapping, _leader) = epoch_to_validators(tc.epoch, tc.round)?;

    let mut node_ids = Vec::new();
    let mut highest_qc_round = GENESIS_ROUND;
    let mut highest_tip_round = GENESIS_ROUND;
    for t in tc.tip_rounds.iter() {
        if t.high_qc_round >= tc.round {
            return Err(Error::InvalidTcRound);
        }

        if t.high_qc_round > highest_qc_round {
            highest_qc_round = t.high_qc_round;
        }
        if t.high_tip_round > highest_tip_round {
            highest_tip_round = t.high_tip_round;
        }

        let td = TimeoutInfo {
            epoch: tc.epoch,
            round: tc.round,
            high_qc_round: t.high_qc_round,
            high_tip_round: t.high_tip_round,
        };
        let msg = alloy_rlp::encode(td);

        // TODO-3: evidence collection
        let signers = t
            .sigs
            .verify::<signing_domain::Timeout>(validator_mapping, msg.as_ref())
            .map_err(|_| Error::InvalidSignature)?;

        node_ids.extend(signers);
    }

    if !validators.has_super_majority_votes(&node_ids) {
        return Err(Error::InsufficientStake);
    }

    verify_high_extend(epoch_to_validators, &tc.high_extend)?;
    match &tc.high_extend {
        HighExtend::Qc(qc) => {
            if qc.get_round() != highest_qc_round {
                return Err(Error::NotWellFormed);
            }
            if highest_tip_round > highest_qc_round {
                // higher tip exists
                return Err(Error::NotWellFormed);
            }
        }
        HighExtend::Tip(tip) => {
            if tip.block_header.block_round != highest_tip_round {
                return Err(Error::NotWellFormed);
            }
            if highest_qc_round >= highest_tip_round {
                // qc for same or higher round exists
                return Err(Error::NotWellFormed);
            }
        }
    }

    Ok(())
}

/// Verify the quorum certificate
///
/// Verify the signature collection and super majority stake signed
pub fn verify_qc<'a, SCT, VT>(
    epoch_to_validators: &impl Fn(
        Epoch,
        Round,
    ) -> Result<
        (
            &'a VT,
            &'a ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
            NodeId<SCT::NodeIdPubKey>,
        ),
        Error,
    >,
    qc: &QuorumCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    let (validators, validator_mapping, _leader) =
        epoch_to_validators(qc.get_epoch(), qc.get_round())?;

    if qc.get_round() == GENESIS_ROUND {
        if qc == &QuorumCertificate::genesis_qc() {
            return Ok(());
        } else {
            return Err(Error::InvalidSignature);
        }
    }
    let qc_msg = alloy_rlp::encode(qc.info);
    let node_ids = qc
        .signatures
        .verify::<signing_domain::Vote>(validator_mapping, qc_msg.as_ref())
        .map_err(|_| Error::InvalidSignature)?;

    if !validators.has_super_majority_votes(&node_ids) {
        return Err(Error::InsufficientStake);
    }

    Ok(())
}

/// Verify the high_extend
pub fn verify_high_extend<'a, ST, SCT, EPT, VT>(
    epoch_to_validators: &impl Fn(
        Epoch,
        Round,
    ) -> Result<
        (
            &'a VT,
            &'a ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
            NodeId<SCT::NodeIdPubKey>,
        ),
        Error,
    >,
    high_extend: &HighExtend<ST, SCT, EPT>,
) -> Result<(), Error>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    match high_extend {
        HighExtend::Qc(qc) => {
            verify_qc(epoch_to_validators, qc)?;
        }
        HighExtend::Tip(tip) => {
            verify_tip(epoch_to_validators, tip)?;
        }
    }

    Ok(())
}

/// Verify the tip
pub fn verify_tip<'a, ST, SCT, EPT, VT>(
    epoch_to_validators: &impl Fn(
        Epoch,
        Round,
    ) -> Result<
        (
            &'a VT,
            &'a ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
            NodeId<SCT::NodeIdPubKey>,
        ),
        Error,
    >,
    tip: &ConsensusTip<ST, SCT, EPT>,
) -> Result<(), Error>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    let (_, _, leader) = epoch_to_validators(tip.block_header.epoch, tip.block_header.block_round)?;

    let tip_author = tip
        .signature_author()
        .map_err(|_| Error::InvalidSignature)?;
    if tip_author != leader.pubkey() || tip_author != tip.block_header.author.pubkey() {
        return Err(Error::InvalidAuthor);
    }

    verify_qc(&epoch_to_validators, &tip.block_header.qc)?;
    match &tip.fresh_certificate {
        Some(FreshProposalCertificate::Nec(nec)) => {
            verify_nec(&epoch_to_validators, nec)?;
        }
        Some(FreshProposalCertificate::NoTip(NoTipCertificate {
            epoch,
            round,
            tip_rounds,
            high_qc,
        })) => {
            verify_tc(
                epoch_to_validators,
                &TimeoutCertificate::<ST, SCT, EPT> {
                    epoch: *epoch,
                    round: *round,
                    tip_rounds: tip_rounds.clone(),
                    high_extend: HighExtend::Qc(high_qc.clone()),
                },
            )?;
        }
        None => {}
    };

    if tip.block_header.block_round == tip.block_header.qc.get_round() + Round(1) {
        // consecutive QC, no fresh_certificate needed
        if tip.fresh_certificate.is_some() {
            return Err(Error::NotWellFormed);
        }

        Ok(())
    } else {
        // fresh_certificate needed
        match &tip.fresh_certificate {
            None => return Err(Error::NotWellFormed),
            Some(FreshProposalCertificate::Nec(nec)) => {
                if nec.msg.round != tip.block_header.block_round {
                    return Err(Error::NotWellFormed);
                }
                if nec.msg.tip_qc_round != tip.block_header.qc.get_round() {
                    return Err(Error::NotWellFormed);
                }
            }
            Some(FreshProposalCertificate::NoTip(no_tip)) => {
                if no_tip.round + Round(1) != tip.block_header.block_round {
                    return Err(Error::NotWellFormed);
                }
                if no_tip.high_qc.get_round() != tip.block_header.qc.get_round() {
                    return Err(Error::NotWellFormed);
                }
            }
        }
        Ok(())
    }
}

/// Verify the nec
///
/// Verify the signature collection and super majority stake signed
pub fn verify_nec<'a, SCT, VT>(
    epoch_to_validators: impl Fn(
        Epoch,
        Round,
    ) -> Result<
        (
            &'a VT,
            &'a ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
            NodeId<SCT::NodeIdPubKey>,
        ),
        Error,
    >,
    nec: &NoEndorsementCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    let (validators, validator_mapping, _leader) =
        epoch_to_validators(nec.msg.epoch, nec.msg.round)?;

    let nec_msg = alloy_rlp::encode(&nec.msg);
    let node_ids = nec
        .signatures
        .verify::<signing_domain::NoEndorsement>(validator_mapping, nec_msg.as_ref())
        .map_err(|_| Error::InvalidSignature)?;

    if !validators.has_super_majority_votes(&node_ids) {
        return Err(Error::InsufficientStake);
    }

    Ok(())
}

trait ValidatorPubKey {
    type NodeIdPubKey: PubKey;
    /// PubKey is valid if it is in the validator set
    fn valid_pubkey(
        self,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> Result<Self, Error>
    where
        Self: Sized;
}

impl<PT: PubKey> ValidatorPubKey for PT {
    type NodeIdPubKey = PT;
    /// Validate that pubkey is in the validator set
    fn valid_pubkey(
        self,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> Result<Self, Error> {
        if validators.contains_key(&NodeId::new(self)) {
            Ok(self)
        } else {
            Err(Error::InvalidAuthor)
        }
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        block::{
            ConsensusBlockHeader, MockExecutionBody, MockExecutionProposedHeader,
            MockExecutionProtocol,
        },
        payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
        quorum_certificate::QuorumCertificate,
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        timeout::{HighExtend, HighTipRoundSigColTuple, TimeoutCertificate, TimeoutInfo},
        tip::ConsensusTip,
        validation::Error,
        voting::{ValidatorMapping, Vote},
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignatureRecoverable,
        },
        hasher::Hash,
        signing_domain, NopSignature,
    };
    use monad_multi_sig::MultiSig;
    use monad_testutil::{
        signing::{create_certificate_keys, create_keys, get_certificate_key, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::{
        BlockId, DontCare, Epoch, NodeId, Round, SeqNum, Stake, GENESIS_ROUND, GENESIS_SEQ_NUM,
    };
    use monad_validator::{
        epoch_manager::EpochManager,
        validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
        validators_epoch_mapping::ValidatorsEpochMapping,
        weighted_round_robin::WeightedRoundRobin,
    };
    use test_case::test_case;

    use super::{verify_qc, verify_tc, Verified};
    use crate::{
        messages::message::{ProposalMessage, TimeoutMessage},
        validation::signing::{epoch_to_validators, Unvalidated, VoteMessage},
    };

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;

    // NOTE: the error is an invalid author error
    //       the receiver uses the round number from TC, in this case `round` to recover the pubkey
    //       it's different from the message the keypair signs, which is always round 5
    //       so it will recover a pubkey different from the signer's
    //       -> in the validator set with negligible probability
    #[test_case(4 => matches Err(_) ; "TC has an older round")]
    #[test_case(6 => matches Err(_); "TC has a newer round")]
    #[test_case(5 => matches Ok(()); "TC has the correct round")]
    fn tc_comprised_of_old_tmo(round: u64) -> Result<(), Error> {
        let (keypairs, certkeys, vset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(3, ValidatorSetFactory::default());

        let tip_rounds = [
            Round(1),
            Round(2),
            Round(3),
        ]
        .iter()
        .zip(0..3)
        .map(|(x, i)| {
            let td = TimeoutInfo {
                epoch: Epoch(1),
                round: Round(5),
                high_qc_round: *x,
                high_tip_round: GENESIS_ROUND,
            };
            let msg = alloy_rlp::encode(td);

            let sigs = vec![(NodeId::new(keypairs[i].pubkey()), <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Timeout>(msg.as_ref(), &certkeys[i]))];

            let sigcol = <SignatureCollectionType as SignatureCollection>::new::<signing_domain::Timeout>(sigs, &vmap, msg.as_ref()).unwrap();

            HighTipRoundSigColTuple {
                high_qc_round: *x,
                high_tip_round: GENESIS_ROUND,
                sigs: sigcol,
            }
        })
        .collect();

        let qc = {
            let vote = Vote {
                round: Round(3),
                ..DontCare::dont_care()
            };
            let msg = alloy_rlp::encode(vote);
            let qc_sigs: Vec<_> = keypairs.iter().zip(certkeys.iter()).map(
                |(keypair, cert_keypair)|
                (
                    NodeId::new(keypair.pubkey()),
                    <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(msg.as_ref(), cert_keypair)
                )).collect();
            let qc_sigcol = <SignatureCollectionType as SignatureCollection>::new::<
                signing_domain::Vote,
            >(qc_sigs, &vmap, msg.as_ref())
            .unwrap();
            QuorumCertificate::new(
                Vote {
                    round: Round(3),
                    ..DontCare::dont_care()
                },
                qc_sigcol,
            )
        };

        let tc: TimeoutCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
            TimeoutCertificate {
                epoch: Epoch(1),
                round: Round(round),
                tip_rounds,
                high_extend: HighExtend::Qc(qc),
            };

        verify_tc(
            &|_epoch, _round| Ok((&vset, &vmap, NodeId::new(keypairs[0].pubkey()))),
            &tc,
        )
    }

    /// The signature collection is created on a Vote different from the one in
    /// Qc. Expect QC verification to fail
    #[test]
    fn qc_verification_vote_doesnt_match() {
        let vote = Vote {
            ..DontCare::dont_care()
        };

        let keypair = get_key::<SignatureType>(6);
        let cert_keypair = get_certificate_key::<SignatureCollectionType>(6);
        let stake_list = vec![(NodeId::new(keypair.pubkey()), Stake(1))];
        let voting_identity = vec![(NodeId::new(keypair.pubkey()), cert_keypair.pubkey())];

        let vset = ValidatorSetFactory::default().create(stake_list).unwrap();
        let val_mapping = ValidatorMapping::new(voting_identity);

        let msg = alloy_rlp::encode(vote);
        let s = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(msg.as_ref(), &cert_keypair);

        let vote2 = Vote {
            round: Round(1),
            ..DontCare::dont_care()
        };
        let sigs = vec![(NodeId::new(keypair.pubkey()), s)];
        let sigs =
            SignatureCollectionType::new::<signing_domain::Vote>(sigs, &val_mapping, msg.as_ref())
                .unwrap();

        let qc = QuorumCertificate::new(vote2, sigs);

        assert!(matches!(
            verify_qc(
                &|_epoch, _round| Ok((&vset, &val_mapping, NodeId::new(keypair.pubkey()))),
                &qc,
            ),
            Err(Error::InvalidSignature)
        ));
    }

    #[test]
    fn test_qc_verify_insufficient_stake() {
        let vote = Vote {
            round: Round(1),
            ..DontCare::dont_care()
        };

        let keypairs = create_keys::<SignatureType>(2);
        let vlist = vec![
            (NodeId::new(keypairs[0].pubkey()), Stake(1)),
            (NodeId::new(keypairs[1].pubkey()), Stake(2)),
        ];

        let vset = ValidatorSetFactory::default().create(vlist).unwrap();

        let cert_keys = create_certificate_keys::<SignatureCollectionType>(2);
        let voting_identity = keypairs
            .iter()
            .zip(cert_keys.iter())
            .map(|(kp, cert_kp)| (NodeId::new(kp.pubkey()), cert_kp.pubkey()))
            .collect::<Vec<_>>();

        let vmap = ValidatorMapping::new(voting_identity);

        let msg = alloy_rlp::encode(vote);
        let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(msg.as_ref(), &cert_keys[0]);

        let sigs = vec![(NodeId::new(keypairs[0].pubkey()), s)];

        let sig_col =
            SignatureCollectionType::new::<signing_domain::Vote>(sigs, &vmap, msg.as_ref())
                .unwrap();

        let qc = QuorumCertificate::new(vote, sig_col);

        assert!(matches!(
            verify_qc(
                &|_epoch, _round| Ok((&vset, &vmap, NodeId::new(keypairs[0].pubkey()))),
                &qc,
            ),
            Err(Error::InsufficientStake)
        ));
    }

    #[test]
    fn test_tc_verify_insufficient_stake() {
        let (keypairs, certkeys, vset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        let epoch = Epoch(1);
        let round = Round(5);

        let tip_rounds = [
            Round(1),
            Round(2),
        ]
        .iter()
        .zip(keypairs[..2].iter())
        .zip(certkeys[..2].iter())
        .map(|((x, keypair), certkey)| {
            let td = TimeoutInfo {
                epoch: Epoch(1),
                round: Round(5),
                high_qc_round: *x,
                high_tip_round: GENESIS_ROUND,
            };
            let msg = alloy_rlp::encode(td);

            let sigs = vec![(NodeId::new(keypair.pubkey()), <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Timeout>(msg.as_ref(), certkey))];

            let sigcol = <SignatureCollectionType as SignatureCollection>::new::<signing_domain::Timeout>(sigs, &vmap, msg.as_ref()).unwrap();

            HighTipRoundSigColTuple {
                high_qc_round: *x,
                high_tip_round: GENESIS_ROUND,
                sigs: sigcol,
            }
        })
        .collect();

        let qc = {
            let vote = Vote {
                round: Round(3),
                ..DontCare::dont_care()
            };
            let msg = alloy_rlp::encode(vote);
            let qc_sigs: Vec<_> = keypairs.iter().zip(certkeys.iter()).map(
                |(keypair, cert_keypair)|
                (
                    NodeId::new(keypair.pubkey()),
                    <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(msg.as_ref(), cert_keypair)
                )).collect();
            let qc_sigcol = <SignatureCollectionType as SignatureCollection>::new::<
                signing_domain::Vote,
            >(qc_sigs, &vmap, msg.as_ref())
            .unwrap();
            QuorumCertificate::new(
                Vote {
                    round: Round(2),
                    ..DontCare::dont_care()
                },
                qc_sigcol,
            )
        };

        let tc: TimeoutCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
            TimeoutCertificate {
                epoch,
                round,
                tip_rounds,
                high_extend: HighExtend::Qc(qc),
            };

        assert!(matches!(
            verify_tc(
                &|_epoch, _round| Ok((&vset, &vmap, NodeId::new(keypairs[0].pubkey()))),
                &tc
            ),
            Err(Error::InsufficientStake)
        ));
    }

    /// The signature collection is created on an epoch different from the
    /// TC::epoch. Expect TC signature verification to fail
    #[test]
    fn test_tc_verify_epoch_no_match() {
        let tmo_info = TimeoutInfo {
            epoch: Epoch(2), // an intentionally malformed tmo_info
            round: Round(1),
            high_qc_round: GENESIS_ROUND,
            high_tip_round: GENESIS_ROUND,
        };

        let tmo_digest = alloy_rlp::encode(&tmo_info);

        let keypair = get_key::<SignatureType>(6);
        let cert_keypair = get_certificate_key::<SignatureCollectionType>(6);
        let stake_list = vec![(NodeId::new(keypair.pubkey()), Stake(1))];
        let voting_identity = vec![(NodeId::new(keypair.pubkey()), cert_keypair.pubkey())];

        let vset = ValidatorSetFactory::default().create(stake_list).unwrap();
        let val_mapping = ValidatorMapping::new(voting_identity);

        let s = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Timeout>(tmo_digest.as_ref(), &cert_keypair);

        let sigs = vec![(NodeId::new(keypair.pubkey()), s)];
        let sigcol = SignatureCollectionType::new::<signing_domain::Timeout>(
            sigs,
            &val_mapping,
            tmo_digest.as_ref(),
        )
        .unwrap();

        let tc: TimeoutCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
            TimeoutCertificate {
                epoch: Epoch(1),
                round: Round(1),
                tip_rounds: vec![HighTipRoundSigColTuple {
                    high_qc_round: GENESIS_ROUND,
                    high_tip_round: GENESIS_ROUND,
                    sigs: sigcol,
                }],
                high_extend: HighExtend::Qc(QuorumCertificate::genesis_qc()),
            };

        assert!(matches!(
            verify_tc(
                &|_epoch, _round| Ok((&vset, &val_mapping, NodeId::new(keypair.pubkey()))),
                &tc
            ),
            Err(Error::InvalidSignature)
        ));
    }

    /// Creates a timeout message with a TC in r-1, but the TC doesn't carry any
    /// signature. Expect TimeoutMsg verification to return in sufficient stake
    /// error
    #[test]
    fn empty_tc() {
        let (keypairs, certkeys, vset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(2, ValidatorSetFactory::default());
        let election = WeightedRoundRobin::default();

        // TC doesn't have any signatures
        let tc: TimeoutCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
            TimeoutCertificate {
                epoch: Epoch(1),
                round: Round(3),
                tip_rounds: vec![],
                high_extend: HighExtend::Qc(QuorumCertificate::genesis_qc()),
            };

        let vote = Vote {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(3),
            ..DontCare::dont_care()
        };

        let msg = alloy_rlp::encode(vote);
        let mut sigs = Vec::new();

        for (key, certkey) in keypairs.iter().zip(certkeys.iter()) {
            let s = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(msg.as_ref(), certkey);
            sigs.push((NodeId::new(key.pubkey()), s));
        }

        let sig_col =
            SignatureCollectionType::new::<signing_domain::Vote>(sigs, &vmap, msg.as_ref())
                .unwrap();

        let qc = QuorumCertificate::new(vote, sig_col);

        let timeout = TimeoutInfo {
            epoch: Epoch(1),

            round: Round(4),
            high_qc_round: qc.get_round(),
            high_tip_round: GENESIS_ROUND,
        };

        let unvalidated_tmo_msg =
            Unvalidated::new(TimeoutMessage::<
                SignatureType,
                SignatureCollectionType,
                ExecutionProtocolType,
            >::new(
                &certkeys[0], timeout, HighExtend::Qc(qc), Some(tc)
            ));

        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(
            Epoch(1),
            vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
            vmap,
        );

        let err = unvalidated_tmo_msg.validate(&epoch_manager, &val_epoch_map, &election);

        assert!(matches!(err, Err(Error::InsufficientStake)));
    }

    /// The timeout message (r=5) carries a high QC (r=3) with no TC. Expect
    /// TimeoutMsg verification to yield "not wellformed" error
    #[test]
    fn old_high_qc_in_timeout_msg() {
        let (keypairs, certkeys, vset, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(2, ValidatorSetFactory::default());
        let election = WeightedRoundRobin::default();

        let tmo_epoch = Epoch(1);
        let tmo_round = Round(5);

        // create an old qc on Round(3)
        let vote = Vote {
            round: Round(3),
            ..DontCare::dont_care()
        };

        let msg = alloy_rlp::encode(vote);
        let mut sigs = Vec::new();

        for (key, certkey) in keypairs.iter().zip(certkeys.iter()) {
            let s = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(msg.as_ref(), certkey);
            sigs.push((NodeId::new(key.pubkey()), s));
        }

        let sig_col =
            SignatureCollectionType::new::<signing_domain::Vote>(sigs, &vmap, msg.as_ref())
                .unwrap();

        let qc = QuorumCertificate::new(vote, sig_col);

        let timeout = TimeoutInfo {
            epoch: tmo_epoch,
            round: tmo_round,
            high_qc_round: qc.get_round(),
            high_tip_round: GENESIS_ROUND,
        };

        let unvalidated_byzantine_tmo_msg =
            Unvalidated::new(TimeoutMessage::<
                SignatureType,
                SignatureCollectionType,
                ExecutionProtocolType,
            >::new(
                &certkeys[0], timeout, HighExtend::Qc(qc), None
            ));

        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(
            Epoch(1),
            vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
            vmap,
        );

        let err = unvalidated_byzantine_tmo_msg.validate(&epoch_manager, &val_epoch_map, &election);
        assert!(matches!(err, Err(Error::NotWellFormed)));
    }

    #[test]
    fn vote_message_test() {
        let vote = Vote {
            ..DontCare::dont_care()
        };

        let mut privkey: [u8; 32] = [127; 32];
        let keypair =
            <SignatureType as CertificateSignature>::KeyPairType::from_bytes(&mut privkey.clone())
                .unwrap();
        let certkeypair = <SignatureCollectionKeyPairType<SignatureCollectionType> as CertificateKeyPair>::from_bytes(&mut privkey).unwrap();

        let vm = VoteMessage::<SignatureCollectionType>::new(vote, &certkeypair);

        let svm = Verified::<SignatureType, _>::new(vm, &keypair);
        let (author, signature, message) = svm.destructure();
        let msg = alloy_rlp::encode(message);
        assert_eq!(
            signature
                .recover_pubkey::<signing_domain::ConsensusMessage>(msg.as_ref())
                .unwrap(),
            keypair.pubkey()
        );
        assert_eq!(author, NodeId::new(keypair.pubkey()));
        assert_eq!(vote, vm.vote);
    }

    #[test]
    fn proposal_invalid_epoch() {
        let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        let validator_stakes = Vec::from_iter(valset.get_members().clone());
        let election = WeightedRoundRobin::default();

        let author = &keys[0];
        let author_cert_key = &cert_keys[0];

        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(Epoch(1), validator_stakes, valmap);

        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: MockExecutionBody {
                data: Default::default(),
            },
        });
        let block = ConsensusBlockHeader::new(
            NodeId::new(author.pubkey()),
            Epoch(2), // wrong epoch: should be 1
            Round(1),
            Vec::new(), // delayed_execution_results
            MockExecutionProposedHeader {},
            payload.get_id(),
            QuorumCertificate::genesis_qc(),
            GENESIS_SEQ_NUM + SeqNum(1),
            1,
            RoundSignature::new(Round(1), author_cert_key),
        );
        let proposal: ProposalMessage<
            SignatureType,
            SignatureCollectionType,
            MockExecutionProtocol,
        > = ProposalMessage {
            proposal_epoch: block.epoch,
            proposal_round: block.block_round,
            block_body: payload,
            last_round_tc: None,
            tip: ConsensusTip::new(&keys[0], block, None),
        };

        let unvalidated_proposal = Unvalidated::new(proposal);

        let maybe_validated =
            unvalidated_proposal.validate(&epoch_manager, &val_epoch_map, &election);

        assert_eq!(maybe_validated, Err(Error::InvalidEpoch));
    }

    #[test]
    fn vote_invalid_epoch() {
        let (_keys, cert_keys, _valset, _valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        let author_cert_key = &cert_keys[0];

        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);

        let vote = Vote {
            id: BlockId(Hash([0x0a_u8; 32])),
            epoch: Epoch(2), // wrong epoch: should be 1
            round: Round(10),
            block_round: Round(10),
        };

        let vote_message = VoteMessage::<SignatureCollectionType>::new(vote, author_cert_key);

        let unvalidated_vote = Unvalidated::new(vote_message);

        let maybe_validated = unvalidated_vote.validate(&epoch_manager);
        assert_eq!(maybe_validated, Err(Error::InvalidEpoch));
    }

    #[test]
    fn timeout_invalid_epoch() {
        let (_keys, cert_keys, valset, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());
        let validator_stakes = Vec::from_iter(valset.get_members().clone());
        let election = WeightedRoundRobin::default();

        let author_cert_key = &cert_keys[0];

        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(Epoch(1), validator_stakes, valmap);

        let timeout = TimeoutInfo {
            epoch: Epoch(2), // wrong epoch: should be 1
            round: Round(1),
            high_qc_round: GENESIS_ROUND,
            high_tip_round: GENESIS_ROUND,
        };

        let timeout_message: TimeoutMessage<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
        > = TimeoutMessage::new(
            author_cert_key,
            timeout,
            HighExtend::Qc(QuorumCertificate::genesis_qc()),
            None,
        );

        let unvalidated_timeout = Unvalidated::new(timeout_message);

        let maybe_validated =
            unvalidated_timeout.validate(&epoch_manager, &val_epoch_map, &election);
        assert_eq!(maybe_validated, Err(Error::InvalidEpoch));
    }

    #[test]
    fn qc_invalid_epoch() {
        let (_keys, cert_keys, valset, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());
        let validator_stakes = Vec::from_iter(valset.get_members().clone());
        let election = WeightedRoundRobin::default();

        let vote = Vote {
            id: BlockId(Hash([0x09_u8; 32])),
            epoch: Epoch(2), // wrong epoch
            round: Round(10),
            block_round: Round(10),
        };

        let msg = alloy_rlp::encode(vote);
        let mut sigs = Vec::new();
        for ck in cert_keys.iter() {
            let sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(msg.as_ref(), ck);

            for (node_id, pubkey) in valmap.map.iter() {
                if *pubkey == ck.pubkey() {
                    sigs.push((*node_id, sig));
                }
            }
        }

        let sigcol =
            SignatureCollectionType::new::<signing_domain::Vote>(sigs, &valmap, msg.as_ref())
                .unwrap();

        // moved here because of valmap ownership
        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map: ValidatorsEpochMapping<_, SignatureCollectionType> =
            ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(Epoch(1), validator_stakes, valmap);

        let qc = QuorumCertificate::new(vote, sigcol);
        let verify_result = verify_qc(
            &|epoch, round| {
                epoch_to_validators(&epoch_manager, &val_epoch_map, &election, epoch, round)
            },
            &qc,
        );
        assert_eq!(verify_result, Err(Error::InvalidEpoch));
    }

    #[test]
    fn tc_invalid_epoch() {
        let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());
        let validator_stakes = Vec::from_iter(valset.get_members().clone());
        let election = WeightedRoundRobin::default();

        // create valid QC
        let vote = Vote {
            id: BlockId(Hash([0x09_u8; 32])),
            epoch: Epoch(1), // correct epoch
            round: Round(10),
            block_round: Round(10),
        };

        let msg = alloy_rlp::encode(vote);
        let mut sigs = Vec::new();
        for ck in cert_keys.iter() {
            let sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(msg.as_ref(), ck);

            for (node_id, pubkey) in valmap.map.iter() {
                if *pubkey == ck.pubkey() {
                    sigs.push((*node_id, sig));
                }
            }
        }

        let sigcol =
            SignatureCollectionType::new::<signing_domain::Vote>(sigs, &valmap, msg.as_ref())
                .unwrap();
        let qc = QuorumCertificate::new(vote, sigcol);

        // create invalid TC
        let tminfo = TimeoutInfo {
            epoch: Epoch(2), // wrong epoch
            round: Round(11),
            high_qc_round: qc.get_round(),
            high_tip_round: GENESIS_ROUND,
        };

        let tmo_digest = alloy_rlp::encode(&tminfo);
        let mut tc_sigs = Vec::new();
        for (key, certkey) in keys.iter().zip(cert_keys.iter()) {
            let node_id = NodeId::new(key.pubkey());
            let sig =
                <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign::<signing_domain::Timeout>(tmo_digest.as_ref(), certkey);
            tc_sigs.push((node_id, sig));
        }
        let tmo_sig_col = SignatureCollectionType::new::<signing_domain::Timeout>(
            tc_sigs,
            &valmap,
            tmo_digest.as_ref(),
        )
        .unwrap();

        let high_qc_sig_tuple = HighTipRoundSigColTuple {
            high_tip_round: GENESIS_ROUND,
            high_qc_round: qc.get_round(),
            sigs: tmo_sig_col,
        };

        let tc: TimeoutCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
            TimeoutCertificate {
                epoch: Epoch(2), // wrong epoch here
                round: Round(11),
                tip_rounds: vec![high_qc_sig_tuple],
                high_extend: HighExtend::Qc(qc.clone()),
            };

        // moved here because of valmap ownership
        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map: ValidatorsEpochMapping<_, SignatureCollectionType> =
            ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(Epoch(1), validator_stakes, valmap);

        let qc_verify_result = verify_qc(
            &|epoch, round| {
                epoch_to_validators(&epoch_manager, &val_epoch_map, &election, epoch, round)
            },
            &qc,
        );
        assert_eq!(qc_verify_result, Ok(()));
        let tc_verify_result = verify_tc(
            &|epoch, round| {
                epoch_to_validators(&epoch_manager, &val_epoch_map, &election, epoch, round)
            },
            &tc,
        );
        assert_eq!(tc_verify_result, Err(Error::InvalidEpoch));
    }
}
