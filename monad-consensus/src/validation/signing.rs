use std::{collections::BTreeMap, ops::Deref};

use monad_consensus_types::{
    block::BlockType,
    convert::signing::certificate_signature_to_proto,
    ledger::CommitResult,
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::TimeoutCertificate,
    validation::Error,
    voting::ValidatorMapping,
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hash, Hashable, Hasher, HasherType},
};
use monad_proto::proto::message::{
    proto_block_sync_message, proto_unverified_consensus_message, ProtoBlockSyncMessage,
    ProtoPeerStateRootMessage, ProtoRequestBlockSyncMessage, ProtoUnverifiedConsensusMessage,
};
use monad_types::{NodeId, Round, SeqNum, Stake};
use monad_validator::{
    epoch_manager::EpochManager,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{
    convert::message::UnverifiedConsensusMessage,
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{
            BlockSyncResponseMessage, PeerStateRootMessage, ProposalMessage,
            RequestBlockSyncMessage, TimeoutMessage, VoteMessage,
        },
    },
    validation::{message::well_formed, safety::consecutive},
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

impl<S: CertificateSignatureRecoverable, M: Hashable> Verified<S, M> {
    pub fn new(msg: M, keypair: &S::KeyPairType) -> Self {
        let hash = HasherType::hash_object(&msg);
        let signature = S::sign(hash.as_ref(), keypair);
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
#[derive(Clone, Debug, PartialEq, Eq)]
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

impl<ST: CertificateSignatureRecoverable, SCT: SignatureCollection>
    Unverified<ST, Unvalidated<ConsensusMessage<SCT>>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn verify<VTF, VT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        sender: &SCT::NodeIdPubKey,
    ) -> Result<Verified<ST, Unvalidated<ConsensusMessage<SCT>>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        let msg = HasherType::hash_object(&self.obj);

        // If the node is lagging too far behind, it wouldn't know when the
        // next epoch is starting. The epoch retrieved here may be incorrect.
        // TODO: Need to check that case. Should trigger statesync.
        let epoch = epoch_manager
            .get_epoch(self.obj.obj.get_round())
            .ok_or(Error::InvalidEpoch)?;
        let validator_set = val_epoch_map
            .get_val_set(&epoch)
            .ok_or(Error::ValidatorSetDataUnavailable)?;

        let author = verify_author(
            validator_set.get_members(),
            sender,
            &msg,
            &self.author_signature,
        )?;

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

impl<SCT: SignatureCollection> Hashable for Validated<ConsensusMessage<SCT>> {
    fn hash(&self, state: &mut impl Hasher) {
        self.as_ref().hash(state)
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

impl<M> From<Validated<M>> for Unvalidated<M> {
    fn from(value: Validated<M>) -> Self {
        value.message
    }
}

impl<M: Hashable> Hashable for Unvalidated<M> {
    fn hash(&self, state: &mut impl Hasher) {
        self.obj.hash(state)
    }
}

impl<SCT: SignatureCollection> Unvalidated<ConsensusMessage<SCT>> {
    pub fn validate<VTF, VT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        version: &str,
    ) -> Result<Validated<ProtocolMessage<SCT>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        if self.obj.version != version {
            return Err(Error::InvalidVersion);
        }

        Ok(match self.obj.message {
            ProtocolMessage::Proposal(m) => {
                let validated = Unvalidated::new(m).validate(epoch_manager, val_epoch_map)?;
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
                let validated = Unvalidated::new(m).validate(epoch_manager, val_epoch_map)?;
                Validated {
                    message: Unvalidated::new(ProtocolMessage::Timeout(validated.into_inner())),
                }
            }
        })
    }
}

impl<SCT: SignatureCollection> Unvalidated<ProposalMessage<SCT>> {
    // A verified proposal is one which is well-formed, has valid signatures for
    // the present TC or QC, and epoch number is consistent with local records
    // for block.round
    pub fn validate<VTF, VT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
    ) -> Result<Validated<ProposalMessage<SCT>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        self.well_formed_proposal()?;
        self.verify_epoch(epoch_manager)?;
        verify_certificates(
            epoch_manager,
            val_epoch_map,
            &self.obj.last_round_tc,
            &self.obj.block.qc,
        )?;

        Ok(Validated { message: self })
    }

    /// A well-formed proposal
    /// 1. extends the sequence number in the QC by 1 and
    /// 2. carries a QC/TC from r-1, proving that the block is proposed on a
    ///    valid round
    fn well_formed_proposal(&self) -> Result<(), Error> {
        self.valid_seq_num()?;
        well_formed(
            self.obj.block.round,
            self.obj.block.qc.get_round(),
            &self.obj.last_round_tc,
        )
    }

    fn valid_seq_num(&self) -> Result<(), Error> {
        if self.obj.block.get_seq_num() != self.obj.block.qc.get_seq_num() + SeqNum(1) {
            return Err(Error::InvalidSeqNum);
        }
        Ok(())
    }

    /// Check local epoch manager record for block.round is equal to block.epoch
    fn verify_epoch(&self, epoch_manager: &EpochManager) -> Result<(), Error> {
        match epoch_manager.get_epoch(self.obj.block.round) {
            Some(epoch) if self.obj.block.epoch == epoch => Ok(()),
            _ => Err(Error::InvalidEpoch),
        }
    }
}

impl<SCT: SignatureCollection> Unvalidated<VoteMessage<SCT>> {
    /// VoteMessage is valid if (consecutive iff commit). Verifying
    /// [crate::messages::message::VoteMessage::sig] is deferred until a
    /// signature collection is optimistically aggregated because verifying a
    /// BLS signature is expensive. Verifying every signature on VoteMessage is
    /// infeasible with the block time constraint.
    pub fn validate(
        self,
        epoch_manager: &EpochManager,
    ) -> Result<Validated<VoteMessage<SCT>>, Error> {
        self.valid_commit_result()?;
        self.verify_epoch(epoch_manager)?;

        Ok(Validated { message: self })
    }

    fn valid_commit_result(&self) -> Result<(), Error> {
        let consecutive = consecutive(
            self.obj.vote.vote_info.round,
            self.obj.vote.vote_info.parent_round,
        );
        let commit = self.obj.vote.ledger_commit_info == CommitResult::Commit;

        if consecutive == commit {
            Ok(())
        } else {
            Err(Error::InvalidVoteMessage)
        }
    }

    /// Check local epoch manager record for vote.round is equal to vote.epoch
    fn verify_epoch(&self, epoch_manager: &EpochManager) -> Result<(), Error> {
        match epoch_manager.get_epoch(self.obj.vote.vote_info.round) {
            Some(epoch) if self.obj.vote.vote_info.epoch == epoch => Ok(()),
            _ => Err(Error::InvalidEpoch),
        }
    }
}

impl<SCT: SignatureCollection> Unvalidated<TimeoutMessage<SCT>> {
    /// A valid timeout message is well-formed, and carries valid QC/TC
    pub fn validate<VTF, VT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
    ) -> Result<Validated<TimeoutMessage<SCT>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        self.well_formed_timeout()?;
        self.verify_epoch(epoch_manager)?;

        verify_certificates(
            epoch_manager,
            val_epoch_map,
            &self.obj.timeout.last_round_tc,
            &self.obj.timeout.tminfo.high_qc,
        )?;

        Ok(Validated { message: self })
    }

    /// A well-formed timeout carries a QC/TC from r-1, proving that it's timing
    /// out a valid round
    fn well_formed_timeout(&self) -> Result<(), Error> {
        well_formed(
            self.obj.timeout.tminfo.round,
            self.obj.timeout.tminfo.high_qc.get_round(),
            &self.obj.timeout.last_round_tc,
        )
    }

    /// Check local epoch manager record for timeout.round is equal to timeout.epoch
    fn verify_epoch(&self, epoch_manager: &EpochManager) -> Result<(), Error> {
        match epoch_manager.get_epoch(self.obj.timeout.tminfo.round) {
            Some(epoch) if self.obj.timeout.tminfo.epoch == epoch => Ok(()),
            _ => Err(Error::InvalidEpoch),
        }
    }
}

impl Unvalidated<RequestBlockSyncMessage> {
    pub fn validate(self) -> Result<Validated<RequestBlockSyncMessage>, Error> {
        Ok(Validated { message: self })
    }
}

impl<SCT: SignatureCollection> Unvalidated<BlockSyncResponseMessage<SCT>> {
    /// If the block sync response message carries a full block, the certificates on it need to be valid
    pub fn validate<VTF, VT>(
        self,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
    ) -> Result<Validated<BlockSyncResponseMessage<SCT>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        if let BlockSyncResponseMessage::BlockFound(b) = &self.obj {
            verify_certificates(epoch_manager, val_epoch_map, &(None), b.get_qc())?;
        }

        Ok(Validated { message: self })
    }
}

impl<SCT: SignatureCollection> Unvalidated<PeerStateRootMessage<SCT>> {
    pub fn validate<VTF, VT>(
        self,
        sender: &NodeId<SCT::NodeIdPubKey>,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
    ) -> Result<Validated<PeerStateRootMessage<SCT>>, Error>
    where
        VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        if self.obj.peer != *sender {
            return Err(Error::AuthorNotSender);
        }

        // If the node is lagging too far behind, it wouldn't know when the
        // next epoch is starting. The epoch retrieved here may be incorrect.
        // TODO: Need to check that case. Should trigger statesync.
        let epoch = self
            .obj
            .info
            .seq_num
            .to_epoch(epoch_manager.val_set_update_interval);
        let valset = val_epoch_map
            .get_val_set(&epoch)
            .ok_or(Error::ValidatorSetDataUnavailable)?;

        if !valset.is_member(&self.obj.peer) {
            return Err(Error::InvalidAuthor);
        }

        Ok(Validated { message: self })
    }
}

impl<SCT: SignatureCollection> From<&Unvalidated<PeerStateRootMessage<SCT>>>
    for ProtoPeerStateRootMessage
{
    fn from(value: &Unvalidated<PeerStateRootMessage<SCT>>) -> Self {
        ProtoPeerStateRootMessage {
            peer: Some((&value.obj.peer).into()),
            info: Some((&value.obj.info).into()),
            sig: Some(certificate_signature_to_proto(&value.obj.sig)),
        }
    }
}

fn verify_certificates<SCT, VTF, VT>(
    epoch_manager: &EpochManager,
    val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
    tc: &Option<TimeoutCertificate<SCT>>,
    qc: &QuorumCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    VTF: ValidatorSetTypeFactory<ValidatorSetType = VT>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    if let Some(tc) = tc {
        let tc_epoch = tc.epoch;
        let local_tc_epoch = epoch_manager
            .get_epoch(tc.round)
            .ok_or(Error::InvalidEpoch)?;
        if tc_epoch != local_tc_epoch {
            return Err(Error::InvalidEpoch);
        }
        let validator_set = val_epoch_map
            .get_val_set(&tc_epoch)
            .ok_or(Error::ValidatorSetDataUnavailable)?;
        let validator_cert_pubkeys = val_epoch_map
            .get_cert_pubkeys(&tc_epoch)
            .ok_or(Error::ValidatorSetDataUnavailable)?;
        verify_tc(validator_set, validator_cert_pubkeys, tc)?;
    }

    let qc_epoch = qc.get_epoch();
    let local_qc_epoch = epoch_manager
        .get_epoch(qc.get_round())
        .ok_or(Error::InvalidEpoch)?;
    if qc_epoch != local_qc_epoch {
        return Err(Error::InvalidEpoch);
    }

    let validator_set = val_epoch_map
        .get_val_set(&qc_epoch)
        .ok_or(Error::ValidatorSetDataUnavailable)?;
    let validator_cert_pubkeys = val_epoch_map
        .get_cert_pubkeys(&qc_epoch)
        .ok_or(Error::ValidatorSetDataUnavailable)?;
    verify_qc(validator_set, validator_cert_pubkeys, qc)?;

    Ok(())
}

/// Verify the timeout certificate
///
/// The signature collections are created over `Hash(tc.round, high_qc.round)`
///
/// See [monad_consensus_types::timeout::TimeoutInfo::timeout_digest]
fn verify_tc<SCT, VT>(
    validators: &VT,
    validator_mapping: &ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
    tc: &TimeoutCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    let mut node_ids = Vec::new();
    for t in tc.high_qc_rounds.iter() {
        if t.high_qc_round.qc_round >= tc.round {
            return Err(Error::InvalidTcRound);
        }

        let mut h = HasherType::new();
        h.update(tc.epoch);
        h.update(tc.round);
        h.update(t.high_qc_round.qc_round);
        let msg = h.hash();

        // TODO-3: evidence collection
        let signers = t
            .sigs
            .verify(validator_mapping, msg.as_ref())
            .map_err(|_| Error::InvalidSignature)?;

        node_ids.extend(signers);
    }

    if !validators.has_super_majority_votes(&node_ids) {
        return Err(Error::InsufficientStake);
    }

    Ok(())
}

/// Verify the quorum certificate
///
/// Verify the signature collection and super majority stake signed
pub fn verify_qc<SCT, VT>(
    validators: &VT,
    validator_mapping: &ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
    qc: &QuorumCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    if qc.get_round() == Round(0) {
        if qc == &QuorumCertificate::genesis_qc() {
            return Ok(());
        } else {
            return Err(Error::InvalidSignature);
        }
    }
    let qc_msg = HasherType::hash_object(&qc.info.vote);
    let node_ids = qc
        .signatures
        .verify(validator_mapping, qc_msg.as_ref())
        .map_err(|_| Error::InvalidSignature)?;

    if !validators.has_super_majority_votes(&node_ids) {
        return Err(Error::InsufficientStake);
    }

    Ok(())
}

/// Verify that the message is signed by the sender and that the sender is a
/// staked validator
fn verify_author<ST: CertificateSignatureRecoverable>(
    validators: &BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Stake>,
    sender: &CertificateSignaturePubKey<ST>,
    msg: &Hash,
    sig: &ST,
) -> Result<CertificateSignaturePubKey<ST>, Error> {
    let pubkey = get_pubkey(msg.as_ref(), sig)?.valid_pubkey(validators)?;
    sig.verify(msg.as_ref(), &pubkey)
        .map_err(|_| Error::InvalidSignature)?;
    if sender != &pubkey {
        Err(Error::AuthorNotSender)
    } else {
        Ok(pubkey)
    }
}

/// Extract the PubKey from the secp recoverable signature
fn get_pubkey<ST: CertificateSignatureRecoverable>(
    msg: &[u8],
    sig: &ST,
) -> Result<CertificateSignaturePubKey<ST>, Error> {
    sig.recover_pubkey(msg).map_err(|_| Error::InvalidSignature)
}

/// Protobuf conversion
///
/// The conversion is targeted to convert events with unverified messages
/// received over the wire to its protobuf counterpart for persistence.
///
/// Network serialization should use the interface functions in
/// [crate::convert::interface] to avoid serializing unverified messages
impl<ST: CertificateSignature, SCT: SignatureCollection> From<&UnverifiedConsensusMessage<ST, SCT>>
    for ProtoUnverifiedConsensusMessage
{
    fn from(value: &UnverifiedConsensusMessage<ST, SCT>) -> Self {
        let oneof_message = match &value.obj.obj.message {
            ProtocolMessage::Proposal(msg) => {
                proto_unverified_consensus_message::OneofMessage::Proposal(msg.into())
            }
            ProtocolMessage::Vote(msg) => {
                proto_unverified_consensus_message::OneofMessage::Vote(msg.into())
            }
            ProtocolMessage::Timeout(msg) => {
                proto_unverified_consensus_message::OneofMessage::Timeout(msg.into())
            }
        };
        Self {
            author_signature: Some(certificate_signature_to_proto(&value.author_signature)),
            oneof_message: Some(oneof_message),
            version: value.obj.obj.version.clone(),
        }
    }
}

impl From<&Unvalidated<RequestBlockSyncMessage>> for ProtoRequestBlockSyncMessage {
    fn from(value: &Unvalidated<RequestBlockSyncMessage>) -> Self {
        ProtoRequestBlockSyncMessage {
            block_id: Some((&value.obj.block_id).into()),
        }
    }
}

impl<SCT: SignatureCollection> From<&Unvalidated<BlockSyncResponseMessage<SCT>>>
    for ProtoBlockSyncMessage
{
    fn from(value: &Unvalidated<BlockSyncResponseMessage<SCT>>) -> Self {
        Self {
            oneof_message: Some(match &value.obj {
                BlockSyncResponseMessage::BlockFound(blk) => {
                    proto_block_sync_message::OneofMessage::BlockFound(blk.into())
                }
                BlockSyncResponseMessage::NotAvailable(bid) => {
                    proto_block_sync_message::OneofMessage::NotAvailable(bid.into())
                }
            }),
        }
    }
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
        block::Block,
        ledger::CommitResult,
        payload::{ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal},
        quorum_certificate::{QcInfo, QuorumCertificate},
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
        validation::Error,
        voting::{ValidatorMapping, Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignatureRecoverable,
        },
        hasher::{Hash, Hashable, Hasher, HasherType},
        NopSignature,
    };
    use monad_eth_types::EthAddress;
    use monad_multi_sig::MultiSig;
    use monad_testutil::{
        signing::{create_certificate_keys, create_keys, get_certificate_key, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum, Stake};
    use monad_validator::{
        epoch_manager::EpochManager,
        validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
        validators_epoch_mapping::ValidatorsEpochMapping,
    };
    use test_case::test_case;

    use super::{verify_certificates, verify_qc, verify_tc, Verified};
    use crate::{
        messages::message::{ProposalMessage, TimeoutMessage},
        validation::signing::{Unvalidated, VoteMessage},
    };

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;

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

        let high_qc_rounds = [
            HighQcRound { qc_round: Round(1) },
            HighQcRound { qc_round: Round(2) },
            HighQcRound { qc_round: Round(3) },
        ]
        .iter()
        .zip(0..3)
        .map(|(x, i)| {
            let mut h = HasherType::new();
            h.update(Epoch(1));
            h.update(Round(5));
            x.hash(&mut h);
            let msg = h.hash();

            let sigs = vec![(NodeId::new(keypairs[i].pubkey()), <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), &certkeys[i]))];

            let sigcol = <SignatureCollectionType as SignatureCollection>::new(sigs, &vmap, msg.as_ref()).unwrap();

            HighQcRoundSigColTuple {
                high_qc_round: *x,
                sigs: sigcol,
            }
        })
        .collect();

        let tc = TimeoutCertificate {
            epoch: Epoch(1),
            round: Round(round),
            high_qc_rounds,
        };

        verify_tc(&vset, &vmap, &tc)
    }

    /// The signature collection is created on a Vote different from the one in
    /// QcInfo. Expect QC verification to fail
    #[test]
    fn qc_verification_vote_doesnt_match() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let keypair = get_key::<SignatureType>(6);
        let cert_keypair = get_certificate_key::<SignatureCollectionType>(6);
        let stake_list = vec![(NodeId::new(keypair.pubkey()), Stake(1))];
        let voting_identity = vec![(NodeId::new(keypair.pubkey()), cert_keypair.pubkey())];

        let vset = ValidatorSetFactory::default().create(stake_list).unwrap();
        let val_mapping = ValidatorMapping::new(voting_identity);

        let vote = Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::NoCommit,
        };
        let msg = HasherType::hash_object(&vote);
        let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), &cert_keypair);

        let vi2 = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };
        let sigs = vec![(NodeId::new(keypair.pubkey()), s)];
        let sigs = MultiSig::new(sigs, &val_mapping, msg.as_ref()).unwrap();

        let qc = QuorumCertificate::new(
            QcInfo {
                vote: Vote {
                    vote_info: vi2,
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            sigs,
        );

        assert!(matches!(
            verify_qc(&vset, &val_mapping, &qc),
            Err(Error::InvalidSignature)
        ));
    }

    #[test]
    fn test_qc_verify_insufficient_stake() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };
        let vote = Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::NoCommit,
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

        let msg = HasherType::hash_object(&vote);
        let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), &cert_keys[0]);

        let sigs = vec![(NodeId::new(keypairs[0].pubkey()), s)];

        let sig_col = MultiSig::new(sigs, &vmap, msg.as_ref()).unwrap();

        let qc = QuorumCertificate::new(QcInfo { vote }, sig_col);

        assert!(matches!(
            verify_qc(&vset, &vmap, &qc),
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

        let high_qc_rounds = [
            HighQcRound { qc_round: Round(1) },
            HighQcRound { qc_round: Round(2) },
        ]
        .iter()
        .zip(keypairs[..2].iter())
        .zip(certkeys[..2].iter())
        .map(|((x, keypair), certkey)| {
            let mut h = HasherType::new();
            h.update(epoch);
            h.update(round);
            x.hash(&mut h);
            let msg = h.hash();

            let sigs = vec![(NodeId::new(keypair.pubkey()), < <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), certkey))];

            let sigcol = <SignatureCollectionType as SignatureCollection>::new(sigs, &vmap, msg.as_ref()).unwrap();

            HighQcRoundSigColTuple {
                high_qc_round: *x,
                sigs: sigcol,
            }
        })
        .collect();

        let tc = TimeoutCertificate {
            epoch,
            round,
            high_qc_rounds,
        };

        assert!(matches!(
            verify_tc(&vset, &vmap, &tc),
            Err(Error::InsufficientStake)
        ));
    }

    /// The signature collection is created on an epoch different from the
    /// TC::epoch. Expect TC signature verification to fail
    #[test]
    fn test_tc_verify_epoch_no_match() {
        let tmo_info = TimeoutInfo::<SignatureCollectionType> {
            epoch: Epoch(2), // an intentionally malformed tmo_info
            round: Round(1),
            high_qc: QuorumCertificate::genesis_qc(),
        };

        let tmo_digest = tmo_info.timeout_digest();

        let keypair = get_key::<SignatureType>(6);
        let cert_keypair = get_certificate_key::<SignatureCollectionType>(6);
        let stake_list = vec![(NodeId::new(keypair.pubkey()), Stake(1))];
        let voting_identity = vec![(NodeId::new(keypair.pubkey()), cert_keypair.pubkey())];

        let vset = ValidatorSetFactory::default().create(stake_list).unwrap();
        let val_mapping = ValidatorMapping::new(voting_identity);

        let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(tmo_digest.as_ref(), &cert_keypair);

        let sigs = vec![(NodeId::new(keypair.pubkey()), s)];
        let sigcol = MultiSig::new(sigs, &val_mapping, tmo_digest.as_ref()).unwrap();

        let tc = TimeoutCertificate {
            epoch: Epoch(1),
            round: Round(1),
            high_qc_rounds: vec![HighQcRoundSigColTuple {
                high_qc_round: HighQcRound {
                    qc_round: QuorumCertificate::<SignatureCollectionType>::genesis_qc()
                        .get_round(),
                },
                sigs: sigcol,
            }],
        };

        assert!(matches!(
            verify_tc(&vset, &val_mapping, &tc),
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

        // TC doesn't have any signatures
        let tc = TimeoutCertificate {
            epoch: Epoch(1),
            round: Round(3),
            high_qc_rounds: vec![],
        };

        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: Round(3),
            parent_id: BlockId(Hash([0x01_u8; 32])),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };
        let vote = Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::Commit,
        };

        let msg = HasherType::hash_object(&vote);
        let mut sigs = Vec::new();

        for (key, certkey) in keypairs.iter().zip(certkeys.iter()) {
            let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), certkey);
            sigs.push((NodeId::new(key.pubkey()), s));
        }

        let sig_col = MultiSig::new(sigs, &vmap, msg.as_ref()).unwrap();

        let qc = QuorumCertificate::new(QcInfo { vote }, sig_col);

        let tmo_info = TimeoutInfo::<SignatureCollectionType> {
            epoch: Epoch(1),

            round: Round(4),
            high_qc: qc,
        };

        let tmo = Timeout::<SignatureCollectionType> {
            tminfo: tmo_info,
            last_round_tc: Some(tc),
        };

        let unvalidated_tmo_msg = Unvalidated::new(TimeoutMessage::<SignatureCollectionType>::new(
            tmo,
            &certkeys[0],
        ));

        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(
            Epoch(1),
            vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
            vmap,
        );

        let err = unvalidated_tmo_msg.validate(&epoch_manager, &val_epoch_map);

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

        let tmo_epoch = Epoch(1);
        let tmo_round = Round(5);

        // create an old qc on Round(3)
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: Round(3),
            parent_id: BlockId(Hash([0x01_u8; 32])),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };
        let vote = Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::Commit,
        };

        let msg = HasherType::hash_object(&vote);
        let mut sigs = Vec::new();

        for (key, certkey) in keypairs.iter().zip(certkeys.iter()) {
            let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), certkey);
            sigs.push((NodeId::new(key.pubkey()), s));
        }

        let sig_col = MultiSig::new(sigs, &vmap, msg.as_ref()).unwrap();

        let qc = QuorumCertificate::new(QcInfo { vote }, sig_col);

        let tmo_info = TimeoutInfo::<SignatureCollectionType> {
            epoch: tmo_epoch,
            round: tmo_round,
            high_qc: qc,
        };

        let tmo = Timeout::<SignatureCollectionType> {
            tminfo: tmo_info,
            last_round_tc: None,
        };

        let unvalidated_byzantine_tmo_msg = Unvalidated::new(TimeoutMessage::<
            SignatureCollectionType,
        >::new(tmo, &certkeys[0]));

        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(
            Epoch(1),
            vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
            vmap,
        );

        let err = unvalidated_byzantine_tmo_msg.validate(&epoch_manager, &val_epoch_map);
        assert!(matches!(err, Err(Error::NotWellFormed)));
    }

    #[test]
    fn vote_message_test() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };
        let v = Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::NoCommit,
        };

        let mut privkey: [u8; 32] = [127; 32];
        let keypair =
            <SignatureType as CertificateSignature>::KeyPairType::from_bytes(&mut privkey.clone())
                .unwrap();
        let certkeypair = <SignatureCollectionKeyPairType<SignatureCollectionType> as CertificateKeyPair>::from_bytes(&mut privkey).unwrap();

        let vm = VoteMessage::<SignatureCollectionType>::new(v, &certkeypair);

        let expected_vote_hash = HasherType::hash_object(&v);

        let svm = Verified::<SignatureType, _>::new(vm, &keypair);
        let (author, signature, _) = svm.destructure();
        let msg = HasherType::hash_object(&vm);
        assert_eq!(
            signature.recover_pubkey(msg.as_ref()).unwrap(),
            keypair.pubkey()
        );
        assert_eq!(author, NodeId::new(keypair.pubkey()));

        let vote_hash = HasherType::hash_object(&vm.vote);
        assert_eq!(expected_vote_hash, vote_hash);
    }

    #[test]
    fn proposal_invalid_epoch() {
        let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        let validator_stakes = Vec::from_iter(valset.get_members().clone());

        let author = &keys[0];
        let author_cert_key = &cert_keys[0];

        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(Epoch(1), validator_stakes, valmap);

        let block = Block::<SignatureCollectionType>::new(
            NodeId::new(author.pubkey()),
            Epoch(2), // wrong epoch: should be 1
            Round(1),
            &Payload {
                txns: FullTransactionList::empty(),
                header: ExecutionArtifacts::zero(),
                seq_num: SeqNum(0),
                beneficiary: EthAddress::from_bytes([0x00_u8; 20]),
                randao_reveal: RandaoReveal::new::<SignatureType>(Round(1), author_cert_key),
            },
            &QuorumCertificate::genesis_qc(),
        );
        let proposal = ProposalMessage {
            block,
            last_round_tc: None,
        };

        let unvalidated_proposal = Unvalidated::new(proposal);

        let maybe_validated = unvalidated_proposal.validate(&epoch_manager, &val_epoch_map);

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
            vote_info: VoteInfo {
                id: BlockId(Hash([0x0a_u8; 32])),
                epoch: Epoch(2), // wrong epoch: should be 1
                round: Round(10),
                parent_id: BlockId(Hash([0x09_u8; 32])),
                parent_round: Round(9),
                seq_num: SeqNum(10),
            },
            ledger_commit_info: CommitResult::Commit,
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

        let author_cert_key = &cert_keys[0];

        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(Epoch(1), validator_stakes, valmap);

        let timeout = Timeout {
            tminfo: TimeoutInfo {
                epoch: Epoch(2), // wrong epoch: should be 1
                round: Round(1),
                high_qc: QuorumCertificate::<SignatureCollectionType>::genesis_qc(),
            },
            last_round_tc: None,
        };

        let timeout_message = TimeoutMessage::new(timeout, author_cert_key);

        let unvalidated_timeout = Unvalidated::new(timeout_message);

        let maybe_validated = unvalidated_timeout.validate(&epoch_manager, &val_epoch_map);
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

        let vi = VoteInfo {
            id: BlockId(Hash([0x09_u8; 32])),
            epoch: Epoch(2), // wrong epoch
            round: Round(10),
            parent_id: BlockId(Hash([0x0a_u8; 32])),
            parent_round: Round(9),
            seq_num: SeqNum(7),
        };

        let qcinfo = QcInfo {
            vote: Vote {
                vote_info: vi,
                ledger_commit_info: CommitResult::Commit,
            },
        };

        let msg = HasherType::hash_object(&qcinfo.vote);
        let mut sigs = Vec::new();
        for ck in cert_keys.iter() {
            let sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), ck);

            for (node_id, pubkey) in valmap.map.iter() {
                if *pubkey == ck.pubkey() {
                    sigs.push((*node_id, sig));
                }
            }
        }

        let sigcol = SignatureCollectionType::new(sigs, &valmap, msg.as_ref()).unwrap();

        // moved here because of valmap ownership
        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(Epoch(1), validator_stakes, valmap);

        let qc = QuorumCertificate::new(qcinfo, sigcol);
        let verify_result = verify_certificates(&epoch_manager, &val_epoch_map, &None, &qc);
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

        // create valid QC
        let vi = VoteInfo {
            id: BlockId(Hash([0x09_u8; 32])),
            epoch: Epoch(1), // correct epoch
            round: Round(10),
            parent_id: BlockId(Hash([0x0a_u8; 32])),
            parent_round: Round(9),
            seq_num: SeqNum(7),
        };

        let qcinfo = QcInfo {
            vote: Vote {
                vote_info: vi,
                ledger_commit_info: CommitResult::Commit,
            },
        };

        let msg = HasherType::hash_object(&qcinfo.vote);
        let mut sigs = Vec::new();
        for ck in cert_keys.iter() {
            let sig = <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), ck);

            for (node_id, pubkey) in valmap.map.iter() {
                if *pubkey == ck.pubkey() {
                    sigs.push((*node_id, sig));
                }
            }
        }

        let sigcol = SignatureCollectionType::new(sigs, &valmap, msg.as_ref()).unwrap();
        let qc = QuorumCertificate::new(qcinfo, sigcol);

        // create invalid TC
        let tminfo = TimeoutInfo {
            epoch: Epoch(2), // wrong epoch
            round: Round(11),
            high_qc: qc.clone(),
        };

        let tmo_digest = tminfo.timeout_digest();
        let mut tc_sigs = Vec::new();
        for (key, certkey) in keys.iter().zip(cert_keys.iter()) {
            let node_id = NodeId::new(key.pubkey());
            let sig =
                <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(tmo_digest.as_ref(), certkey);
            tc_sigs.push((node_id, sig));
        }
        let tmo_sig_col =
            SignatureCollectionType::new(tc_sigs, &valmap, tmo_digest.as_ref()).unwrap();

        let high_qc_sig_tuple = HighQcRoundSigColTuple {
            high_qc_round: HighQcRound {
                qc_round: qc.get_round(),
            },
            sigs: tmo_sig_col,
        };

        let tc = TimeoutCertificate {
            epoch: Epoch(2), // wrong epoch here
            round: Round(11),
            high_qc_rounds: vec![high_qc_sig_tuple],
        };

        // moved here because of valmap ownership
        let epoch_manager = EpochManager::new(SeqNum(2000), Round(50), &[(Epoch(1), Round(0))]);
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(Epoch(1), validator_stakes, valmap);

        let qc_verify_result = verify_certificates(&epoch_manager, &val_epoch_map, &None, &qc);
        assert_eq!(qc_verify_result, Ok(()));
        let tc_verify_result = verify_certificates(&epoch_manager, &val_epoch_map, &Some(tc), &qc);
        assert_eq!(tc_verify_result, Err(Error::InvalidEpoch));
    }
}
