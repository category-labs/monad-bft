use std::{collections::HashMap, ops::Deref};

use monad_consensus_types::{
    quorum_certificate::QuorumCertificate,
    signature::SignatureCollection,
    timeout::TimeoutCertificate,
    validation::{Error, Hashable, Hasher},
};
#[cfg(feature = "proto")]
use monad_crypto::convert::signature_to_proto;
use monad_crypto::{
    secp256k1::{KeyPair, PubKey},
    Signature,
};
#[cfg(feature = "proto")]
use monad_proto::proto::message::{
    proto_unverified_consensus_message, ProtoUnverifiedConsensusMessage,
};
use monad_types::{Hash, NodeId, Stake};
use monad_validator::{
    validator_property::ValidatorSetPropertyType, validator_set::ValidatorSetType,
};

#[cfg(feature = "proto")]
use crate::convert::message::UnverifiedConsensusMessage;
use crate::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{ProposalMessage, TimeoutMessage, VoteMessage},
    },
    validation::message::well_formed,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verified<S, M> {
    author: NodeId,
    message: Unverified<S, M>,
}

impl<S: Signature, M> Verified<S, M> {
    pub fn author(&self) -> &NodeId {
        &self.author
    }
    pub fn author_signature(&self) -> &S {
        self.message.author_signature()
    }
}

impl<S: Signature, M: Hashable> Verified<S, M> {
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

impl<S: Signature, M> Unverified<S, M> {
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

impl<S, T> Unverified<S, ConsensusMessage<S, T>>
where
    S: Signature,
    T: SignatureCollection,
{
    // A verified proposal is one which is well-formed and has valid
    // signatures for the present TC or QC
    pub fn verify<H: Hasher, VT: ValidatorSetType, VPT: ValidatorSetPropertyType>(
        self,
        validators: &VT,
        validators_property: &VPT,
        sender: &PubKey,
    ) -> Result<Verified<S, ConsensusMessage<S, T>>, Error> {
        // FIXME this feels wrong... it feels like the enum variant should factor into the hash so
        //       signatures can't be reused across variant members
        Ok(match self.obj {
            ConsensusMessage::Proposal(m) => {
                let verified = Unverified::new(m, self.author_signature).verify::<H, _, _>(
                    validators,
                    validators_property,
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
                let verified = Unverified::new(m, self.author_signature).verify::<H, _, _>(
                    validators,
                    validators_property,
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
        })
    }
}

impl<S, SCT> Unverified<S, ProposalMessage<S, SCT>>
where
    S: Signature,
    SCT: SignatureCollection,
{
    // A verified proposal is one which is well-formed and has valid
    // signatures for the present TC or QC
    pub fn verify<H: Hasher, VT: ValidatorSetType, VPT: ValidatorSetPropertyType>(
        self,
        validators: &VT,
        validators_property: &VPT,
        sender: &PubKey,
    ) -> Result<Verified<S, ProposalMessage<S, SCT>>, Error> {
        self.well_formed_proposal()?;
        let msg = H::hash_object(&self.obj);
        let author = verify_author(
            validators.get_members(),
            sender,
            &msg,
            &self.author_signature,
        )?;
        verify_certificates::<S, H, _, _, _>(
            validators,
            validators_property,
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
        well_formed(
            self.obj.block.round,
            self.obj.block.qc.info.vote.round,
            &self.obj.last_round_tc,
        )
    }
}

impl<S: Signature> Unverified<S, VoteMessage> {
    // A verified vote message has a valid signature
    // Return type must keep the signature with the message as it is used later by the protocol
    pub fn verify<H: Hasher>(
        self,
        validators: &ValidatorMember,
        sender: &PubKey,
    ) -> Result<Verified<S, VoteMessage>, Error> {
        let msg = H::hash_object(&self.obj.ledger_commit_info);

        let author = verify_author(validators, sender, &msg, &self.author_signature)?;

        let result = Verified {
            author: NodeId(author),
            message: self,
        };

        Ok(result)
    }
}

impl<S, T> Unverified<S, TimeoutMessage<S, T>>
where
    S: Signature,
    T: SignatureCollection,
{
    pub fn verify<H: Hasher, VT: ValidatorSetType, VPT: ValidatorSetPropertyType>(
        self,
        validators: &VT,
        validators_property: &VPT,
        sender: &PubKey,
    ) -> Result<Verified<S, TimeoutMessage<S, T>>, Error> {
        self.well_formed_timeout()?;
        let msg = H::hash_object(&self.obj);
        let author = verify_author(
            validators.get_members(),
            sender,
            &msg,
            &self.author_signature,
        )?;
        verify_certificates::<S, H, _, _, _>(
            validators,
            validators_property,
            &self.obj.last_round_tc,
            &self.obj.tminfo.high_qc,
        )?;

        let result = Verified {
            author: NodeId(author),
            message: self,
        };
        Ok(result)
    }

    fn well_formed_timeout(&self) -> Result<(), Error> {
        well_formed(
            self.obj.tminfo.round,
            self.obj.tminfo.high_qc.info.vote.round,
            &self.obj.last_round_tc,
        )
    }
}

fn verify_certificates<S, H, SCT, VT, VPT>(
    validators: &VT,
    validators_property: &VPT,
    tc: &Option<TimeoutCertificate<S>>,
    qc: &QuorumCertificate<SCT>,
) -> Result<(), Error>
where
    S: Signature,
    H: Hasher,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
    VPT: ValidatorSetPropertyType,
{
    if let Some(tc) = tc {
        verify_tc::<S, H, _>(validators, tc)?;
    }

    verify_qc::<SCT, H, _, _>(validators, validators_property, qc)?;

    Ok(())
}

fn verify_tc<S, H, VT>(validators: &VT, tc: &TimeoutCertificate<S>) -> Result<(), Error>
where
    S: Signature,
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

        let pubkey = get_pubkey(msg.as_ref(), &t.author_signature)?;
        pubkey.valid_pubkey(validators.get_members())?;

        t.author_signature
            .verify(msg.as_ref(), &pubkey)
            .map_err(|_| Error::InvalidSignature)?;

        node_ids.push(NodeId(pubkey));
    }

    if !validators.has_super_majority_votes(node_ids.iter()) {
        return Err(Error::InsufficientStake);
    }

    Ok(())
}

fn verify_qc<SCT, H, VT, VPT>(
    validators: &VT,
    validators_property: &VPT,
    qc: &QuorumCertificate<SCT>,
) -> Result<(), Error>
where
    SCT: SignatureCollection,
    H: Hasher,
    VT: ValidatorSetType,
    VPT: ValidatorSetPropertyType,
{
    if H::hash_object(&qc.info.vote) != qc.info.ledger_commit.vote_info_hash {
        // TODO: collect author for evidence?
        return Err(Error::InvalidSignature);
    }

    let qc_msg = H::hash_object(&qc.info.ledger_commit);
    let node_ids = qc
        .signatures
        .verify(validators_property.get_voting_list(), qc_msg.as_ref())
        .map_err(|_| Error::InvalidSignature)?;

    if !validators.has_super_majority_votes(node_ids.iter()) {
        return Err(Error::InsufficientStake);
    }

    Ok(())
}

fn verify_author(
    validators: &ValidatorMember,
    sender: &PubKey,
    msg: &Hash,
    sig: &impl Signature,
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
fn get_pubkey(msg: &[u8], sig: &impl Signature) -> Result<PubKey, Error> {
    sig.recover_pubkey(msg).map_err(|_| Error::InvalidSignature)
}

#[cfg(feature = "proto")]
impl<S: Signature> From<&UnverifiedConsensusMessage<S>> for ProtoUnverifiedConsensusMessage {
    fn from(value: &UnverifiedConsensusMessage<S>) -> Self {
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
        };
        Self {
            author_signature: Some(signature_to_proto(value.author_signature())),
            oneof_message: Some(oneof_message),
        }
    }
}

pub type ValidatorMember = HashMap<NodeId, Stake>;

trait ValidatorPubKey {
    // PubKey is valid if it is in the validator set
    fn valid_pubkey(self, validators: &ValidatorMember) -> Result<Self, Error>
    where
        Self: Sized;
}

impl ValidatorPubKey for PubKey {
    fn valid_pubkey(self, validators: &ValidatorMember) -> Result<Self, Error> {
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
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        quorum_certificate::{QcInfo, QuorumCertificate},
        signature::{SignatureBuilder, SignatureCollection},
        timeout::{HighQcRound, HighQcRoundSigTuple, TimeoutCertificate},
        validation::{Error, Hashable, Hasher, Sha256Hash},
        voting::VoteInfo,
    };
    use monad_crypto::secp256k1::SecpSignature;
    use monad_testutil::signing::{create_keys, create_keys_bls, get_key, get_key_bls};
    use monad_types::{BlockId, Hash, NodeId, Round, Stake};
    use monad_validator::{
        validator_property::{ValidatorSetProperty, ValidatorSetPropertyType},
        validator_set::{ValidatorSet, ValidatorSetType},
    };
    use test_case::test_case;

    use super::{verify_qc, verify_tc};

    // NOTE: the error is an invalid author error
    //       the receiver uses the round number from TC, in this case `round` to recover the pubkey
    //       it's different from the message the keypair signs, which is always round 5
    //       so it will recover a pubkey different from the signer's
    //       -> almost certainly not in the validator set
    #[test_case(4 => matches Err(_) ; "TC has an older round")]
    #[test_case(6 => matches Err(_); "TC has a newer round")]
    #[test_case(5 => matches Ok(()); "TC has the correct round")]
    fn tc_comprised_of_old_tmo(round: u64) -> Result<(), Error> {
        let keypairs = create_keys(3);
        let vlist = keypairs
            .iter()
            .map(|kp| (NodeId(kp.pubkey()), Stake(1)))
            .collect::<Vec<_>>();

        let vset = ValidatorSet::new(vlist).unwrap();

        let high_qc_rounds = vec![
            HighQcRound { qc_round: Round(1) },
            HighQcRound { qc_round: Round(2) },
            HighQcRound { qc_round: Round(3) },
        ]
        .iter()
        .zip(keypairs.iter())
        .map(|(x, keypair)| {
            let mut h = Sha256Hash::new();
            h.update(Round(5));
            x.hash(&mut h);
            let msg = h.hash();
            HighQcRoundSigTuple {
                high_qc_round: *x,
                author_signature: keypair.sign(msg.as_ref()),
            }
        })
        .collect();

        let tc = TimeoutCertificate {
            round: Round(round),
            high_qc_rounds,
        };

        verify_tc::<SecpSignature, Sha256Hash, _>(&vset, &tc)
    }

    #[test]
    fn qc_verifcation_vote_doesnt_match() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let lci = LedgerCommitInfo::new::<Sha256Hash>(Some(Hash([0xad_u8; 32])), &vi);

        let keypair = get_key(6);
        let blskeypair = get_key_bls(6);
        let stake_list = vec![(NodeId(keypair.pubkey()), Stake(1))];
        let prop_list = vec![(NodeId(keypair.pubkey()), blskeypair.pubkey())];

        let vset = ValidatorSet::new(stake_list).unwrap();
        let vsetprop = ValidatorSetProperty::new(prop_list).unwrap();

        let msg = Sha256Hash::hash_object(&lci);
        let s = keypair.sign(msg.as_ref());

        let mut sig_builder = SignatureBuilder::new();
        sig_builder.add_signature(vsetprop.get_index(&NodeId(keypair.pubkey())).unwrap(), s);

        let sigs = MultiSig::new(sig_builder, vsetprop.get_voting_list(), msg.as_ref()).unwrap();

        let vi2 = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(1),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let qc = QuorumCertificate::new(
            QcInfo {
                vote: vi2,
                ledger_commit: lci,
            },
            sigs,
        );

        assert!(
            verify_qc::<MultiSig<SecpSignature>, Sha256Hash, _, _>(&vset, &vsetprop, &qc).is_err()
        );
    }

    #[test]
    fn test_qc_verify_insufficient_stake() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let lci = LedgerCommitInfo::new::<Sha256Hash>(Some(Hash([0xad_u8; 32])), &vi);

        let keypairs = create_keys(2);
        let vlist = vec![
            (NodeId(keypairs[0].pubkey()), Stake(1)),
            (NodeId(keypairs[1].pubkey()), Stake(2)),
        ];

        let vset = ValidatorSet::new(vlist).unwrap();

        let blskeypairs = create_keys_bls(2);
        let prop_list = keypairs
            .iter()
            .zip(blskeypairs.iter())
            .map(|(kp, blskp)| (NodeId(kp.pubkey()), blskp.pubkey()))
            .collect::<Vec<_>>();
        let vprop = ValidatorSetProperty::new(prop_list).unwrap();

        let msg = Sha256Hash::hash_object(&lci);
        let s = keypairs[0].sign(msg.as_ref());

        let mut sig_builder = SignatureBuilder::new();
        sig_builder.add_signature(vprop.get_index(&NodeId(keypairs[0].pubkey())).unwrap(), s);

        let sigs = MultiSig::new(sig_builder, vprop.get_voting_list(), msg.as_ref()).unwrap();

        let qc = QuorumCertificate::new(
            QcInfo {
                vote: vi,
                ledger_commit: lci,
            },
            sigs,
        );

        assert!(matches!(
            verify_qc::<MultiSig<SecpSignature>, Sha256Hash, _, _>(&vset, &vprop, &qc),
            Err(Error::InsufficientStake)
        ));
    }

    #[test]
    fn test_tc_verify_insufficient_stake() {
        let keypairs = create_keys(4);
        let vlist = keypairs
            .iter()
            .map(|kp| (NodeId(kp.pubkey()), Stake(1)))
            .collect::<Vec<_>>();

        let vset = ValidatorSet::new(vlist).unwrap();

        let round = Round(5);

        let high_qc_rounds = vec![
            HighQcRound { qc_round: Round(1) },
            HighQcRound { qc_round: Round(2) },
        ]
        .iter()
        .zip(keypairs[..2].iter())
        .map(|(x, keypair)| {
            let mut h = Sha256Hash::new();
            h.update(round);
            x.hash(&mut h);
            let msg = h.hash();
            HighQcRoundSigTuple {
                high_qc_round: *x,
                author_signature: keypair.sign(msg.as_ref()),
            }
        })
        .collect();

        let tc = TimeoutCertificate {
            round,
            high_qc_rounds,
        };

        assert!(matches!(
            verify_tc::<SecpSignature, Sha256Hash, _>(&vset, &tc),
            Err(Error::InsufficientStake)
        ));
    }
}
