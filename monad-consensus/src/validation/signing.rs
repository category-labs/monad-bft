use std::{collections::HashMap, ops::Deref};

use monad_consensus_types::{
    block::BlockType,
    convert::signing::message_signature_to_proto,
    message_signature::MessageSignature,
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::TimeoutCertificate,
    validation::Error,
    voting::ValidatorMapping,
};
use monad_crypto::{
    hasher::{Hash, Hashable, Hasher, HasherType},
    secp256k1::{KeyPair, PubKey},
};
use monad_proto::proto::message::{
    proto_block_sync_message, proto_unverified_consensus_message, ProtoBlockSyncMessage,
    ProtoRequestBlockSyncMessage, ProtoUnverifiedConsensusMessage,
};
use monad_types::{NodeId, Round, SeqNum, Stake};
use monad_validator::validator_set::ValidatorSetType;

use crate::{
    convert::message::UnverifiedConsensusMessage,
    messages::{
        consensus_message::ConsensusMessage,
        message::{
            BlockSyncResponseMessage, ProposalMessage, RequestBlockSyncMessage, TimeoutMessage,
            VoteMessage,
        },
    },
    validation::message::well_formed,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verified<S, M> {
    author: NodeId,
    message: Unverified<S, M>,
}

impl<S: MessageSignature, M> Verified<S, M> {
    pub fn author(&self) -> &NodeId {
        &self.author
    }
    pub fn author_signature(&self) -> &S {
        self.message.author_signature()
    }
}

impl<S: MessageSignature, M: Hashable> Verified<S, M> {
    pub fn new(msg: M, keypair: &KeyPair) -> Self {
        let hash = HasherType::hash_object(&msg);
        let signature = S::sign(hash.as_ref(), keypair);
        Self {
            author: NodeId(keypair.pubkey()),
            message: Unverified::new(msg, signature),
        }
    }

    pub fn destructure(self) -> (NodeId, S, M) {
        (self.author, self.message.author_signature, self.message.obj)
    }
}

impl<S, M> Deref for Verified<S, M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.message.obj
    }
}

impl<S, M> AsRef<Unverified<S, M>> for Verified<S, M> {
    fn as_ref(&self) -> &Unverified<S, M> {
        &self.message
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unverified<S, M> {
    obj: M,
    author_signature: S,
}

impl<S: MessageSignature, M> Unverified<S, M> {
    pub fn new(obj: M, signature: S) -> Self {
        Self {
            obj,
            author_signature: signature,
        }
    }

    pub fn author_signature(&self) -> &S {
        &self.author_signature
    }
}

impl<S, M> From<Verified<S, M>> for Unverified<S, M> {
    fn from(verified: Verified<S, M>) -> Self {
        verified.message
    }
}

impl<S, M> From<Verified<S, Validated<M>>> for Unverified<S, Unvalidated<M>> {
    fn from(value: Verified<S, Validated<M>>) -> Self {
        let validated = value.message.obj;
        Unverified {
            obj: validated.into(),
            author_signature: value.message.author_signature,
        }
    }
}

impl<S: MessageSignature, M: Hashable> Unverified<S, M> {
    pub fn verify<VT: ValidatorSetType>(
        self,
        validators: &VT,
        sender: &PubKey,
    ) -> Result<Verified<S, M>, Error> {
        let msg = HasherType::hash_object(&self.obj);

        let author = verify_author(
            validators.get_members(),
            sender,
            &msg,
            &self.author_signature,
        )?;

        let result = Verified {
            author: NodeId(author),
            message: self,
        };

        Ok(result)
    }
}

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
    pub fn validate<VT: ValidatorSetType>(
        self,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> Result<Validated<ConsensusMessage<SCT>>, Error> {
        Ok(match self.obj {
            ConsensusMessage::Proposal(m) => {
                let validated = Unvalidated::new(m).validate(validators, validator_mapping)?;
                Validated {
                    message: Unvalidated::new(ConsensusMessage::Proposal(validated.into_inner())),
                }
            }
            ConsensusMessage::Vote(m) => {
                let validated = Unvalidated::new(m).validate()?;
                Validated {
                    message: Unvalidated::new(ConsensusMessage::Vote(validated.into_inner())),
                }
            }
            ConsensusMessage::Timeout(m) => {
                let validated = Unvalidated::new(m).validate(validators, validator_mapping)?;
                Validated {
                    message: Unvalidated::new(ConsensusMessage::Timeout(validated.into_inner())),
                }
            }
        })
    }
}

impl<SCT: SignatureCollection> Unvalidated<ProposalMessage<SCT>> {
    // A verified proposal is one which is well-formed and has valid
    // signatures for the present TC or QC
    pub fn validate<VT: ValidatorSetType>(
        self,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> Result<Validated<ProposalMessage<SCT>>, Error> {
        self.well_formed_proposal()?;
        verify_certificates(
            validators,
            validator_mapping,
            &self.obj.last_round_tc,
            &self.obj.block.qc,
        )?;

        Ok(Validated { message: self })
    }

    fn well_formed_proposal(&self) -> Result<(), Error> {
        self.valid_seq_num()?;
        well_formed(
            self.obj.block.round,
            self.obj.block.qc.info.vote.round,
            &self.obj.last_round_tc,
        )
    }

    fn valid_seq_num(&self) -> Result<(), Error> {
        if self.obj.block.get_seq_num() != self.obj.block.qc.info.vote.seq_num + SeqNum(1) {
            return Err(Error::InvalidSeqNum);
        }
        Ok(())
    }
}

impl<SCT: SignatureCollection> Unvalidated<VoteMessage<SCT>> {
    pub fn validate(self) -> Result<Validated<VoteMessage<SCT>>, Error> {
        Ok(Validated { message: self })
    }
}

impl<SCT: SignatureCollection> Unvalidated<TimeoutMessage<SCT>> {
    pub fn validate<VT: ValidatorSetType>(
        self,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> Result<Validated<TimeoutMessage<SCT>>, Error> {
        self.well_formed_timeout()?;

        verify_certificates(
            validators,
            validator_mapping,
            &self.obj.timeout.last_round_tc,
            &self.obj.timeout.tminfo.high_qc,
        )?;

        Ok(Validated { message: self })
    }

    fn well_formed_timeout(&self) -> Result<(), Error> {
        well_formed(
            self.obj.timeout.tminfo.round,
            self.obj.timeout.tminfo.high_qc.info.vote.round,
            &self.obj.timeout.last_round_tc,
        )
    }
}

impl Unvalidated<RequestBlockSyncMessage> {
    pub fn validate(self) -> Result<Validated<RequestBlockSyncMessage>, Error> {
        Ok(Validated { message: self })
    }
}

impl<SCT: SignatureCollection> Unvalidated<BlockSyncResponseMessage<SCT>> {
    pub fn validate<VT: ValidatorSetType>(
        self,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> Result<Validated<BlockSyncResponseMessage<SCT>>, Error> {
        if let BlockSyncResponseMessage::BlockFound(b) = &self.obj {
            verify_certificates(validators, validator_mapping, &(None), &b.block.qc)?;
        }

        Ok(Validated { message: self })
    }
}

fn verify_certificates<SCT, VT>(
    validators: &VT,
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    tc: &Option<TimeoutCertificate<SCT>>,
    qc: &QuorumCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    if let Some(tc) = tc {
        verify_tc(validators, validator_mapping, tc)?;
    }

    verify_qc(validators, validator_mapping, qc)?;

    Ok(())
}

/// Verify the timeout certificate
///
/// The signature collections are created over `Hash(tc.round, high_qc.round)`
///
/// See [monad_consensus_types::timeout::TimeoutInfo::timeout_digest]
fn verify_tc<SCT, VT>(
    validators: &VT,
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    tc: &TimeoutCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    let mut node_ids = Vec::new();
    for t in tc.high_qc_rounds.iter() {
        if t.high_qc_round.qc_round >= tc.round {
            return Err(Error::InvalidTcRound);
        }

        let mut h = HasherType::new();
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

    if !validators.has_super_majority_votes(node_ids.iter()) {
        return Err(Error::InsufficientStake);
    }

    Ok(())
}

fn verify_qc<SCT, VT>(
    validators: &VT,
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    qc: &QuorumCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    if qc.info.vote.round == Round(0) {
        if qc == &QuorumCertificate::genesis_prime_qc() {
            return Ok(());
        } else {
            return Err(Error::InvalidSignature);
        }
    }
    if HasherType::hash_object(&qc.info.vote) != qc.info.ledger_commit.vote_info_hash {
        // TODO-3: collect author for evidence?
        return Err(Error::InvalidSignature);
    }

    let qc_msg = HasherType::hash_object(&qc.info.ledger_commit);
    let node_ids = qc
        .signatures
        .verify(validator_mapping, qc_msg.as_ref())
        .map_err(|_| Error::InvalidSignature)?;

    if !validators.has_super_majority_votes(node_ids.iter()) {
        return Err(Error::InsufficientStake);
    }

    Ok(())
}

fn verify_author(
    validators: &HashMap<NodeId, Stake>,
    sender: &PubKey,
    msg: &Hash,
    sig: &impl MessageSignature,
) -> Result<PubKey, Error> {
    let pubkey = get_pubkey(msg.as_ref(), sig)?.valid_pubkey(validators)?;
    sig.verify(msg.as_ref(), &pubkey)
        .map_err(|_| Error::InvalidSignature)?;
    if sender != &pubkey {
        Err(Error::AuthorNotSender)
    } else {
        Ok(pubkey)
    }
}

// Extract the PubKey from the Signature if possible
fn get_pubkey(msg: &[u8], sig: &impl MessageSignature) -> Result<PubKey, Error> {
    sig.recover_pubkey(msg).map_err(|_| Error::InvalidSignature)
}

impl<MS: MessageSignature, SCT: SignatureCollection> From<&UnverifiedConsensusMessage<MS, SCT>>
    for ProtoUnverifiedConsensusMessage
{
    fn from(value: &UnverifiedConsensusMessage<MS, SCT>) -> Self {
        let oneof_message = match &value.obj.obj {
            ConsensusMessage::Proposal(msg) => {
                proto_unverified_consensus_message::OneofMessage::Proposal(msg.into())
            }
            ConsensusMessage::Vote(msg) => {
                proto_unverified_consensus_message::OneofMessage::Vote(msg.into())
            }
            ConsensusMessage::Timeout(msg) => {
                proto_unverified_consensus_message::OneofMessage::Timeout(msg.into())
            }
        };
        Self {
            author_signature: Some(message_signature_to_proto(value.author_signature())),
            oneof_message: Some(oneof_message),
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
    // PubKey is valid if it is in the validator set
    fn valid_pubkey(self, validators: &HashMap<NodeId, Stake>) -> Result<Self, Error>
    where
        Self: Sized;
}

impl ValidatorPubKey for PubKey {
    fn valid_pubkey(self, validators: &HashMap<NodeId, Stake>) -> Result<Self, Error> {
        if validators.contains_key(&NodeId(self)) {
            Ok(self)
        } else {
            Err(Error::InvalidAuthor)
        }
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        quorum_certificate::{QcInfo, QuorumCertificate},
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
        validation::Error,
        voting::{ValidatorMapping, Vote, VoteInfo},
    };
    use monad_crypto::{
        hasher::{Hash, Hashable, Hasher, HasherType},
        secp256k1::{KeyPair, SecpSignature},
    };
    use monad_testutil::{
        signing::{create_certificate_keys, create_keys, get_certificate_key, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, NodeId, Round, SeqNum, Stake};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};
    use test_case::test_case;

    use super::{verify_qc, verify_tc, Verified};
    use crate::{
        messages::message::TimeoutMessage,
        validation::signing::{Unvalidated, VoteMessage},
    };

    type SignatureType = SecpSignature;
    type SignatureCollectionType = MultiSig<SecpSignature>;

    // NOTE: the error is an invalid author error
    //       the receiver uses the round number from TC, in this case `round` to recover the pubkey
    //       it's different from the message the keypair signs, which is always round 5
    //       so it will recover a pubkey different from the signer's
    //       -> almost certainly not in the validator set
    #[test_case(4 => matches Err(_) ; "TC has an older round")]
    #[test_case(6 => matches Err(_); "TC has a newer round")]
    #[test_case(5 => matches Ok(()); "TC has the correct round")]
    fn tc_comprised_of_old_tmo(round: u64) -> Result<(), Error> {
        let (keypairs, certkeys, vset, vmap) =
            create_keys_w_validators::<SignatureCollectionType>(3);

        let high_qc_rounds = [
            HighQcRound { qc_round: Round(1) },
            HighQcRound { qc_round: Round(2) },
            HighQcRound { qc_round: Round(3) },
        ]
        .iter()
        .zip(0..3)
        .map(|(x, i)| {
            let mut h = HasherType::new();
            h.update(Round(5));
            x.hash(&mut h);
            let msg = h.hash();

            let sigs = vec![(NodeId(keypairs[i].pubkey()), <<SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), &certkeys[i]))];

            let sigcol = <SignatureCollectionType as SignatureCollection>::new(sigs, &vmap, msg.as_ref()).unwrap();

            HighQcRoundSigColTuple {
                high_qc_round: *x,
                sigs: sigcol,
            }
        })
        .collect();

        let tc = TimeoutCertificate {
            round: Round(round),
            high_qc_rounds,
        };

        verify_tc(&vset, &vmap, &tc)
    }

    #[test]
    fn qc_verifcation_vote_doesnt_match() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let lci = LedgerCommitInfo::new(Some(Hash([0xad_u8; 32])), &vi);

        let keypair = get_key(6);
        let cert_keypair = get_certificate_key::<SignatureCollectionType>(6);
        let stake_list = vec![(NodeId(keypair.pubkey()), Stake(1))];
        let voting_identity = vec![(NodeId(keypair.pubkey()), cert_keypair.pubkey())];

        let vset = ValidatorSet::new(stake_list).unwrap();
        let val_mapping = ValidatorMapping::new(voting_identity);

        let msg = HasherType::hash_object(&lci);
        let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), &cert_keypair);

        let sigs = vec![(NodeId(keypair.pubkey()), s)];

        let sigs = MultiSig::new(sigs, &val_mapping, msg.as_ref()).unwrap();

        let vi2 = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let qc = QuorumCertificate::new(
            QcInfo {
                vote: vi2,
                ledger_commit: lci,
            },
            sigs,
        );

        assert!(verify_qc(&vset, &val_mapping, &qc).is_err());
    }

    #[test]
    fn test_qc_verify_insufficient_stake() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let lci = LedgerCommitInfo::new(Some(Hash([0xad_u8; 32])), &vi);

        let keypairs = create_keys(2);
        let vlist = vec![
            (NodeId(keypairs[0].pubkey()), Stake(1)),
            (NodeId(keypairs[1].pubkey()), Stake(2)),
        ];

        let vset = ValidatorSet::new(vlist).unwrap();

        let cert_keys = create_certificate_keys::<SignatureCollectionType>(2);
        let voting_identity = keypairs
            .iter()
            .zip(cert_keys.iter())
            .map(|(kp, cert_kp)| (NodeId(kp.pubkey()), cert_kp.pubkey()))
            .collect::<Vec<_>>();

        let vmap = ValidatorMapping::new(voting_identity);

        let msg = HasherType::hash_object(&lci);
        let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), &cert_keys[0]);

        let sigs = vec![(NodeId(keypairs[0].pubkey()), s)];

        let sig_col = MultiSig::new(sigs, &vmap, msg.as_ref()).unwrap();

        let qc = QuorumCertificate::new(
            QcInfo {
                vote: vi,
                ledger_commit: lci,
            },
            sig_col,
        );

        assert!(matches!(
            verify_qc(&vset, &vmap, &qc),
            Err(Error::InsufficientStake)
        ));
    }

    #[test]
    fn test_tc_verify_insufficient_stake() {
        let (keypairs, certkeys, vset, vmap) =
            create_keys_w_validators::<SignatureCollectionType>(4);

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
            h.update(round);
            x.hash(&mut h);
            let msg = h.hash();

            let sigs = vec![(NodeId(keypair.pubkey()), < <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), certkey))];

            let sigcol = <SignatureCollectionType as SignatureCollection>::new(sigs, &vmap, msg.as_ref()).unwrap();

            HighQcRoundSigColTuple {
                high_qc_round: *x,
                sigs: sigcol,
            }
        })
        .collect();

        let tc = TimeoutCertificate {
            round,
            high_qc_rounds,
        };

        assert!(matches!(
            verify_tc(&vset, &vmap, &tc),
            Err(Error::InsufficientStake)
        ));
    }

    #[test]
    fn empty_tc() {
        let (keypairs, certkeys, vset, vmap) =
            create_keys_w_validators::<SignatureCollectionType>(2);

        let tc = TimeoutCertificate {
            round: Round(3),
            high_qc_rounds: vec![],
        };

        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(3),
            parent_id: BlockId(Hash([0x01_u8; 32])),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };

        let lci = LedgerCommitInfo::new(Some(Hash([0xad_u8; 32])), &vi);

        let msg = HasherType::hash_object(&lci);
        let mut sigs = Vec::new();

        for (key, certkey) in keypairs.iter().zip(certkeys.iter()) {
            let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), certkey);
            sigs.push((NodeId(key.pubkey()), s));
        }

        let sig_col = MultiSig::new(sigs, &vmap, msg.as_ref()).unwrap();

        let qc = QuorumCertificate::new(
            QcInfo {
                vote: vi,
                ledger_commit: lci,
            },
            sig_col,
        );

        let tmo_info = TimeoutInfo::<SignatureCollectionType> {
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

        let err = unvalidated_tmo_msg.validate(&vset, &vmap);

        assert!(matches!(err, Err(Error::InsufficientStake)));
    }

    #[test]
    fn old_high_qc_in_timeout_msg() {
        let (keypairs, certkeys, vset, vmap) =
            create_keys_w_validators::<SignatureCollectionType>(2);

        let tmo_round = Round(5);

        // create an old qc on Round(3)
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(3),
            parent_id: BlockId(Hash([0x01_u8; 32])),
            parent_round: Round(2),
            seq_num: SeqNum(0),
        };

        let lci = LedgerCommitInfo::new(Some(Hash([0xad_u8; 32])), &vi);

        let msg = HasherType::hash_object(&lci);
        let mut sigs = Vec::new();

        for (key, certkey) in keypairs.iter().zip(certkeys.iter()) {
            let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), certkey);
            sigs.push((NodeId(key.pubkey()), s));
        }

        let sig_col = MultiSig::new(sigs, &vmap, msg.as_ref()).unwrap();

        let qc = QuorumCertificate::new(
            QcInfo {
                vote: vi,
                ledger_commit: lci,
            },
            sig_col,
        );

        let tmo_info = TimeoutInfo::<SignatureCollectionType> {
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

        let err = unvalidated_byzantine_tmo_msg.validate(&vset, &vmap);
        assert!(matches!(err, Err(Error::NotWellFormed)));
    }

    #[test]
    fn vote_message_test() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };
        let lci = LedgerCommitInfo::new(Some(Hash([0xad_u8; 32])), &vi);

        let v = Vote {
            vote_info: vi,
            ledger_commit_info: lci,
        };

        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey.clone()).unwrap();
        let certkeypair = <SignatureCollectionKeyPairType<SignatureCollectionType> as CertificateKeyPair>::from_bytes(&mut privkey).unwrap();

        let vm = VoteMessage::<SignatureCollectionType>::new(v, &certkeypair);

        let expected_vote_info_hash = vm.vote.ledger_commit_info.vote_info_hash;

        let svm = Verified::<SignatureType, _>::new(vm, &keypair);
        let (author, signature, orig_vm) = svm.destructure();
        let msg = HasherType::hash_object(&vm);
        assert_eq!(
            signature.recover_pubkey(msg.as_ref()).unwrap(),
            keypair.pubkey()
        );
        assert_eq!(author, NodeId(keypair.pubkey()));
        assert_eq!(
            expected_vote_info_hash,
            orig_vm.vote.ledger_commit_info.vote_info_hash
        );
    }
}
