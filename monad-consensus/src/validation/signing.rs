use std::{collections::HashMap, ops::Deref};

use monad_consensus_types::{
    block::BlockType,
    convert::signing::message_signature_to_proto,
    message_signature::MessageSignature,
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::TimeoutCertificate,
    validation::{Error, Hashable, Hasher},
    voting::ValidatorMapping,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_proto::proto::message::{
    proto_unverified_consensus_message, ProtoUnverifiedConsensusMessage,
};
use monad_types::{Hash, NodeId, Stake};
use monad_validator::validator_set::ValidatorSetType;

use crate::{
    convert::message::UnverifiedConsensusMessage,
    messages::{
        consensus_message::ConsensusMessage,
        message::{
            BlockSyncMessage, ProposalMessage, RequestBlockSyncMessage, TimeoutMessage, VoteMessage,
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
    pub fn new<H: Hasher>(msg: M, keypair: &KeyPair) -> Self {
        let hash = H::hash_object(&msg);
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

impl<S, SCT> Unverified<S, ConsensusMessage<SCT>>
where
    S: MessageSignature,
    SCT: SignatureCollection,
{
    // A verified proposal is one which is well-formed and has valid
    // signatures for the present TC or QC
    pub fn verify<H: Hasher, VT: ValidatorSetType>(
        self,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        sender: &PubKey,
    ) -> Result<Verified<S, ConsensusMessage<SCT>>, Error> {
        // FIXME this feels wrong... it feels like the enum variant should factor into the hash so
        //       signatures can't be reused across variant members
        Ok(match self.obj {
            ConsensusMessage::Proposal(m) => {
                let verified = Unverified::new(m, self.author_signature).verify::<H, _>(
                    validators,
                    validator_mapping,
                    sender,
                )?;
                Verified {
                    author: verified.author,
                    message: Unverified::new(
                        ConsensusMessage::Proposal(verified.message.obj),
                        verified.message.author_signature,
                    ),
                }
            }
            ConsensusMessage::Vote(m) => {
                let verified = Unverified::new(m, self.author_signature)
                    .verify::<H>(validators.get_members(), sender)?;
                Verified {
                    author: verified.author,
                    message: Unverified::new(
                        ConsensusMessage::Vote(verified.message.obj),
                        verified.message.author_signature,
                    ),
                }
            }
            ConsensusMessage::Timeout(m) => {
                let verified = Unverified::new(m, self.author_signature).verify::<H, _>(
                    validators,
                    validator_mapping,
                    sender,
                )?;
                Verified {
                    author: verified.author,
                    message: Unverified::new(
                        ConsensusMessage::Timeout(verified.message.obj),
                        verified.message.author_signature,
                    ),
                }
            }
            ConsensusMessage::RequestBlockSync(m) => {
                let verified = Unverified::new(m, self.author_signature)
                    .verify::<H>(validators.get_members(), sender)?;
                Verified {
                    author: verified.author,
                    message: Unverified::new(
                        ConsensusMessage::RequestBlockSync(verified.message.obj),
                        verified.message.author_signature,
                    ),
                }
            }
            ConsensusMessage::BlockSync(m) => {
                let verified = Unverified::new(m, self.author_signature).verify::<H, _>(
                    validators,
                    validator_mapping,
                    sender,
                )?;
                Verified {
                    author: verified.author,
                    message: Unverified::new(
                        ConsensusMessage::BlockSync(verified.message.obj),
                        verified.message.author_signature,
                    ),
                }
            }
        })
    }
}

impl<S, SCT> Unverified<S, ProposalMessage<SCT>>
where
    S: MessageSignature,
    SCT: SignatureCollection,
{
    // A verified proposal is one which is well-formed and has valid
    // signatures for the present TC or QC
    pub fn verify<H: Hasher, VT: ValidatorSetType>(
        self,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        sender: &PubKey,
    ) -> Result<Verified<S, ProposalMessage<SCT>>, Error> {
        self.well_formed_proposal()?;
        let msg = H::hash_object(&self.obj);
        let author = verify_author(
            validators.get_members(),
            sender,
            &msg,
            &self.author_signature,
        )?;
        verify_certificates::<H, _, _>(
            validators,
            validator_mapping,
            &self.obj.last_round_tc,
            &self.obj.block.qc,
        )?;

        let result = Verified {
            author: NodeId(author),
            message: self,
        };

        Ok(result)
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
        if self.obj.block.get_seq_num() != self.obj.block.qc.info.vote.seq_num + 1 {
            return Err(Error::InvalidSeqNum);
        }
        Ok(())
    }
}

impl<S: MessageSignature, SCT: SignatureCollection> Unverified<S, VoteMessage<SCT>> {
    // A verified vote message has a valid signature
    // Return type must keep the signature with the message as it is used later by the protocol
    pub fn verify<H: Hasher>(
        self,
        validators: &HashMap<NodeId, Stake>,
        sender: &PubKey,
    ) -> Result<Verified<S, VoteMessage<SCT>>, Error> {
        let msg = H::hash_object(&self.obj);

        let author = verify_author(validators, sender, &msg, &self.author_signature)?;

        let result = Verified {
            author: NodeId(author),
            message: self,
        };

        Ok(result)
    }
}

impl<S, SCT> Unverified<S, TimeoutMessage<SCT>>
where
    S: MessageSignature,
    SCT: SignatureCollection,
{
    pub fn verify<H: Hasher, VT: ValidatorSetType>(
        self,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        sender: &PubKey,
    ) -> Result<Verified<S, TimeoutMessage<SCT>>, Error> {
        self.well_formed_timeout()?;
        let msg = H::hash_object(&self.obj);
        let author = verify_author(
            validators.get_members(),
            sender,
            &msg,
            &self.author_signature,
        )?;
        verify_certificates::<H, _, _>(
            validators,
            validator_mapping,
            &self.obj.timeout.last_round_tc,
            &self.obj.timeout.tminfo.high_qc,
        )?;

        let result = Verified {
            author: NodeId(author),
            message: self,
        };
        Ok(result)
    }

    fn well_formed_timeout(&self) -> Result<(), Error> {
        well_formed(
            self.obj.timeout.tminfo.round,
            self.obj.timeout.tminfo.high_qc.info.vote.round,
            &self.obj.timeout.last_round_tc,
        )
    }
}

impl<S: MessageSignature> Unverified<S, RequestBlockSyncMessage> {
    pub fn verify<H: Hasher>(
        self,
        validators: &HashMap<NodeId, Stake>,
        sender: &PubKey,
    ) -> Result<Verified<S, RequestBlockSyncMessage>, Error> {
        let msg = H::hash_object(&self.obj);

        let author = verify_author(validators, sender, &msg, &self.author_signature)?;

        let result = Verified {
            author: NodeId(author),
            message: self,
        };

        Ok(result)
    }
}

impl<S, SCT> Unverified<S, BlockSyncMessage<SCT>>
where
    S: MessageSignature,
    SCT: SignatureCollection,
{
    pub fn verify<H: Hasher, VT: ValidatorSetType>(
        self,
        validators: &VT,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        sender: &PubKey,
    ) -> Result<Verified<S, BlockSyncMessage<SCT>>, Error> {
        let msg = H::hash_object(&self.obj);

        let author = verify_author(
            validators.get_members(),
            sender,
            &msg,
            &self.author_signature,
        )?;

        verify_certificates::<H, _, _>(validators, validator_mapping, &(None), &self.obj.block.qc)?;

        let result = Verified {
            author: NodeId(author),
            message: self,
        };

        Ok(result)
    }
}

fn verify_certificates<H, SCT, VT>(
    validators: &VT,
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    tc: &Option<TimeoutCertificate<SCT>>,
    qc: &QuorumCertificate<SCT>,
) -> Result<(), Error>
where
    H: Hasher,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    if let Some(tc) = tc {
        verify_tc::<SCT, H, _>(validators, validator_mapping, tc)?;
    }

    verify_qc::<SCT, H, _>(validators, validator_mapping, qc)?;

    Ok(())
}

fn verify_tc<SCT, H, VT>(
    validators: &VT,
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    tc: &TimeoutCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    H: Hasher,
    VT: ValidatorSetType,
{
    let mut node_ids = Vec::new();
    for t in tc.high_qc_rounds.iter() {
        if t.high_qc_round.qc_round >= tc.round {
            return Err(Error::InvalidTcRound);
        }

        // TODO fix this hashing..
        let mut h = H::new();
        h.update(tc.round);
        t.high_qc_round.hash(&mut h);
        let msg = h.hash();

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

fn verify_qc<SCT, H, VT>(
    validators: &VT,
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    qc: &QuorumCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    H: Hasher,
    VT: ValidatorSetType,
{
    if H::hash_object(&qc.info.vote) != qc.info.ledger_commit.vote_info_hash {
        // TODO: collect author for evidence?
        return Err(Error::InvalidSignature);
    }

    let qc_msg = H::hash_object(&qc.info.ledger_commit);
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
        let oneof_message = match &value.obj {
            ConsensusMessage::Proposal(msg) => {
                proto_unverified_consensus_message::OneofMessage::Proposal(msg.into())
            }
            ConsensusMessage::Vote(msg) => {
                proto_unverified_consensus_message::OneofMessage::Vote(msg.into())
            }
            ConsensusMessage::Timeout(msg) => {
                proto_unverified_consensus_message::OneofMessage::Timeout(msg.into())
            }
            ConsensusMessage::RequestBlockSync(msg) => {
                proto_unverified_consensus_message::OneofMessage::RequestBlockSync(msg.into())
            }
            ConsensusMessage::BlockSync(msg) => {
                proto_unverified_consensus_message::OneofMessage::BlockSync(msg.into())
            }
        };
        Self {
            author_signature: Some(message_signature_to_proto(value.author_signature())),
            oneof_message: Some(oneof_message),
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
        certificate_signature::CertificateSignature,
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        quorum_certificate::{QcInfo, QuorumCertificate},
        signature_collection::SignatureCollection,
        timeout::{HighQcRound, HighQcRoundSigColTuple, TimeoutCertificate},
        validation::{Error, Hashable, Hasher, Sha256Hash},
        voting::{ValidatorMapping, VoteInfo},
    };
    use monad_crypto::secp256k1::SecpSignature;
    use monad_testutil::{
        signing::{create_certificate_keys, create_keys, get_certificate_key, get_key},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Hash, NodeId, Round, Stake};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};
    use test_case::test_case;

    use super::{verify_qc, verify_tc};

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
            let mut h = Sha256Hash::new();
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

        verify_tc::<_, Sha256Hash, _>(&vset, &vmap, &tc)
    }

    #[test]
    fn qc_verifcation_vote_doesnt_match() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let lci = LedgerCommitInfo::new::<Sha256Hash>(Some(Hash([0xad_u8; 32])), &vi);

        let keypair = get_key(6);
        let cert_keypair = get_certificate_key::<SignatureCollectionType>(6);
        let stake_list = vec![(NodeId(keypair.pubkey()), Stake(1))];
        let voting_identity = vec![(NodeId(keypair.pubkey()), cert_keypair.pubkey())];

        let vset = ValidatorSet::new(stake_list).unwrap();
        let val_mapping = ValidatorMapping::new(voting_identity);

        let msg = Sha256Hash::hash_object(&lci);
        let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), &cert_keypair);

        let sigs = vec![(NodeId(keypair.pubkey()), s)];

        let sigs = MultiSig::new(sigs, &val_mapping, msg.as_ref()).unwrap();

        let vi2 = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let qc = QuorumCertificate::new::<Sha256Hash>(
            QcInfo {
                vote: vi2,
                ledger_commit: lci,
            },
            sigs,
        );

        assert!(
            verify_qc::<SignatureCollectionType, Sha256Hash, _>(&vset, &val_mapping, &qc).is_err()
        );
    }

    #[test]
    fn test_qc_verify_insufficient_stake() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let lci = LedgerCommitInfo::new::<Sha256Hash>(Some(Hash([0xad_u8; 32])), &vi);

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

        let msg = Sha256Hash::hash_object(&lci);
        let s =< <SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::sign(msg.as_ref(), &cert_keys[0]);

        let sigs = vec![(NodeId(keypairs[0].pubkey()), s)];

        let sig_col = MultiSig::new(sigs, &vmap, msg.as_ref()).unwrap();

        let qc = QuorumCertificate::new::<Sha256Hash>(
            QcInfo {
                vote: vi,
                ledger_commit: lci,
            },
            sig_col,
        );

        assert!(matches!(
            verify_qc::<SignatureCollectionType, Sha256Hash, _>(&vset, &vmap, &qc),
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
            let mut h = Sha256Hash::new();
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
            verify_tc::<_, Sha256Hash, _>(&vset, &vmap, &tc),
            Err(Error::InsufficientStake)
        ));
    }
}
