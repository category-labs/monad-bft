use std::collections::HashMap;
use std::ops::Deref;

use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_crypto::Signature;
use monad_types::Hash;
use monad_types::NodeId;
use monad_validator::validator::Validator;

use crate::types::consensus_message::ConsensusMessage;
use crate::types::message::ProposalMessage;
use crate::types::message::TimeoutMessage;
use crate::types::message::VoteMessage;
use crate::types::quorum_certificate::QuorumCertificate;
use crate::types::signature::SignatureCollection;
use crate::types::timeout::TimeoutCertificate;
use crate::validation::error::Error;
use crate::validation::hashing::{Hashable, Hasher};

use crate::validation::message::well_formed;

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
        let signature = S::sign(&hash, keypair);
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
    pub fn verify<H: Hasher>(
        self,
        validators: &ValidatorMember,
        sender: &PubKey,
    ) -> Result<Verified<S, ConsensusMessage<S, T>>, Error> {
        // FIXME this feels wrong... it feels like the enum variant should factor into the hash so
        //       signatures can't be reused across variant members
        Ok(match self.obj {
            ConsensusMessage::Proposal(m) => {
                let verified =
                    Unverified::new(m, self.author_signature).verify::<H>(validators, sender)?;
                Verified {
                    author: verified.author,
                    message: Unverified::new(
                        ConsensusMessage::Proposal(verified.message.obj),
                        verified.message.author_signature,
                    ),
                }
            }
            ConsensusMessage::Vote(m) => {
                let verified =
                    Unverified::new(m, self.author_signature).verify::<H>(validators, sender)?;
                Verified {
                    author: verified.author,
                    message: Unverified::new(
                        ConsensusMessage::Vote(verified.message.obj),
                        verified.message.author_signature,
                    ),
                }
            }
            ConsensusMessage::Timeout(m) => {
                let verified =
                    Unverified::new(m, self.author_signature).verify::<H>(validators, sender)?;
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

impl<S, T> Unverified<S, ProposalMessage<S, T>>
where
    S: Signature,
    T: SignatureCollection,
{
    // A verified proposal is one which is well-formed and has valid
    // signatures for the present TC or QC
    pub fn verify<H: Hasher>(
        self,
        validators: &ValidatorMember,
        sender: &PubKey,
    ) -> Result<Verified<S, ProposalMessage<S, T>>, Error> {
        self.well_formed_proposal()?;
        let msg = H::hash_object(&self.obj);
        let author = verify_author(validators, sender, &msg, &self.author_signature)?;
        verify_certificates::<S, H, _>(validators, &self.obj.last_round_tc, &self.obj.block.qc)?;

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
    pub fn verify<H: Hasher>(
        self,
        validators: &ValidatorMember,
        sender: &PubKey,
    ) -> Result<Verified<S, TimeoutMessage<S, T>>, Error> {
        self.well_formed_timeout()?;
        let msg = H::hash_object(&self.obj);
        let author = verify_author(validators, sender, &msg, &self.author_signature)?;
        verify_certificates::<S, H, _>(
            validators,
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

fn verify_certificates<S, H, V>(
    validators: &ValidatorMember,
    tc: &Option<TimeoutCertificate<S>>,
    qc: &QuorumCertificate<V>,
) -> Result<(), Error>
where
    S: Signature,
    H: Hasher,
    V: SignatureCollection,
{
    if let Some(tc) = tc {
        verify_tc::<S, H>(validators, tc)?;
    }

    verify_qc::<V, H>(validators, qc)?;

    Ok(())
}

fn verify_tc<S, H>(validators: &ValidatorMember, tc: &TimeoutCertificate<S>) -> Result<(), Error>
where
    S: Signature,
    H: Hasher,
{
    for t in tc.high_qc_rounds.iter() {
        if t.0.qc_round >= tc.round {
            return Err(Error::InvalidTcRound);
        }

        // TODO fix this hashing..
        let mut h = H::new();
        h.update(tc.round);
        h.update(t.0.qc_round);
        let msg = h.hash();

        let pubkey = get_pubkey(&msg, &t.1)?;
        pubkey.valid_pubkey(validators)?;

        t.1.verify(&msg, &pubkey)
            .map_err(|_| Error::InvalidSignature)?;
    }

    Ok(())
}

fn verify_qc<V, H>(validators: &ValidatorMember, qc: &QuorumCertificate<V>) -> Result<(), Error>
where
    V: SignatureCollection,
    H: Hasher,
{
    if H::hash_object(&qc.info.vote) != qc.info.ledger_commit.vote_info_hash {
        // TODO: collect author for evidence?
        return Err(Error::InvalidSignature);
    }

    let qc_msg = H::hash_object(&qc.info.ledger_commit);
    let pubkeys = qc
        .signatures
        .get_pubkeys(&qc_msg)
        .map_err(|_| Error::InvalidSignature)?;

    for p in pubkeys.iter() {
        p.valid_pubkey(validators)?;
    }

    qc.signatures
        .verify_signatures(&qc_msg)
        .map_err(|_| Error::InvalidSignature)?;

    Ok(())
}

fn verify_author(
    validators: &ValidatorMember,
    sender: &PubKey,
    msg: &Hash,
    sig: &impl Signature,
) -> Result<PubKey, Error> {
    let pubkey = get_pubkey(msg, sig)?.valid_pubkey(validators)?;
    sig.verify(msg, &pubkey)
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

pub type ValidatorMember = HashMap<NodeId, Validator>;

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
    use crate::signatures::aggregate_signature::AggregateSignatures;
    use crate::types::ledger::LedgerCommitInfo;
    use crate::types::quorum_certificate::{QcInfo, QuorumCertificate};
    use crate::types::signature::SignatureCollection;
    use crate::types::timeout::{HighQcRound, TimeoutCertificate};
    use crate::types::voting::VoteInfo;
    use crate::validation::error::Error;
    use crate::validation::{hashing::*, signing::ValidatorMember};
    use monad_crypto::secp256k1::SecpSignature;
    use monad_testutil::signing::get_key;
    use monad_types::{BlockId, NodeId, Round};
    use monad_validator::validator::Validator;
    use test_case::test_case;

    use super::{verify_qc, verify_tc};

    #[test_case(4 => matches Err(_) ; "TC has an older round")]
    #[test_case(6 => matches Err(_); "TC has a newer round")]
    #[test_case(5 => matches Ok(()); "TC has the correct round")]
    fn tc_comprised_of_old_tmo(round: u64) -> Result<(), Error> {
        let mut vset = ValidatorMember::new();
        let keypair = get_key(6);

        vset.insert(
            NodeId(keypair.pubkey()),
            Validator {
                pubkey: keypair.pubkey(),
                stake: 1,
            },
        );

        let high_qc_rounds = vec![
            HighQcRound { qc_round: Round(1) },
            HighQcRound { qc_round: Round(2) },
            HighQcRound { qc_round: Round(3) },
        ]
        .iter()
        .map(|x| {
            let mut h = Sha256Hash::new();
            h.update(Round(5));
            h.update(x.qc_round);
            let msg = h.hash();
            (*x, keypair.sign(&msg))
        })
        .collect();

        let tc = TimeoutCertificate {
            round: Round(round),
            high_qc_rounds,
        };

        verify_tc::<SecpSignature, Sha256Hash>(&vset, &tc)
    }

    #[test]
    fn qc_verifcation_vote_doesnt_match() {
        let vi = VoteInfo {
            id: BlockId([0x00_u8; 32]),
            round: Round(0),
            parent_id: BlockId([0x00_u8; 32]),
            parent_round: Round(0),
        };

        let lci = LedgerCommitInfo::new::<Sha256Hash>(Some([0xad_u8; 32]), &vi);

        let mut vset = ValidatorMember::new();
        let keypair = get_key(6);

        vset.insert(
            NodeId(keypair.pubkey()),
            Validator {
                pubkey: keypair.pubkey(),
                stake: 1,
            },
        );

        let msg = Sha256Hash::hash_object(&lci);
        let s = keypair.sign(&msg);

        let mut sigs = AggregateSignatures::new();
        sigs.add_signature(s);

        let vi2 = VoteInfo {
            id: BlockId([0x00_u8; 32]),
            round: Round(1),
            parent_id: BlockId([0x00_u8; 32]),
            parent_round: Round(0),
        };

        let qc = QuorumCertificate::new(
            QcInfo {
                vote: vi2,
                ledger_commit: lci,
            },
            sigs,
        );

        assert!(verify_qc::<AggregateSignatures<SecpSignature>, Sha256Hash>(&vset, &qc).is_err());
    }
}
